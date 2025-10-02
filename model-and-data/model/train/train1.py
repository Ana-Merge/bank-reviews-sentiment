import pandas as pd
import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from torch.optim import AdamW
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score, confusion_matrix
from tqdm import tqdm
import warnings
import os
warnings.filterwarnings("ignore")

# 1. Определение параметров
MODEL_NAME = "DeepPavlov/rubert-base-cased"
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Используемое устройство: {DEVICE}")
if torch.cuda.is_available():
    print(f"Доступные GPU: {torch.cuda.device_count()} | Текущий GPU: {torch.cuda.get_device_name(0)}")
else:
    print("GPU не доступен, используется CPU. Проверьте установку CUDA и драйверов.")
NUM_EPOCHS = 5
BATCH_SIZE = 16
LEARNING_RATE = 2e-5

# 2. Набор данных
class ReviewDataset(Dataset):
    def __init__(self, texts, topics, sentiments, tokenizer, max_length=128):
        self.texts = texts
        self.topics = topics
        self.sentiments = sentiments
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = str(self.texts[idx])
        topic_labels = self.topics[idx]
        sentiment_labels = self.sentiments[idx]

        encoding = tokenizer(
            text,
            max_length=self.max_length,
            padding="max_length",
            truncation=True,
            return_tensors="pt"
        )

        return {
            "input_ids": encoding["input_ids"].flatten(),
            "attention_mask": encoding["attention_mask"].flatten(),
            "topics": torch.FloatTensor(topic_labels),
            "sentiments": torch.FloatTensor(sentiment_labels)
        }

# 3. Загрузка и подготовка данных
def prepare_data(file_path):
    df = pd.read_csv(file_path)
    df['pseudo_topic'] = df['pseudo_topic'].fillna('Прочее').apply(lambda x: x.split(',') if isinstance(x, str) else ['Прочее'])
    df['pseudo_sentiment'] = df['pseudo_sentiment'].fillna('unknown').apply(lambda x: x.split(',') if isinstance(x, str) else ['unknown'])

    # Мультилейбл-кодирование тем
    mlb_topics = MultiLabelBinarizer()
    topic_labels = mlb_topics.fit_transform(df['pseudo_topic'])
    topic_classes = mlb_topics.classes_

    # Мультилейбл-кодирование тональности
    sentiment_map = {'отрицательно': 0, 'нейтрально': 1, 'положительно': 2, 'unknown': 3}
    sentiment_labels = []
    for sentiments in df['pseudo_sentiment']:
        sent_vec = np.zeros(len(topic_classes))
        for i, sent in enumerate(sentiments):
            if i < len(topic_classes):
                sent_vec[i] = sentiment_map.get(sent, 3)
        sentiment_labels.append(sent_vec)
    sentiment_labels = np.array(sentiment_labels)

    # Разделение на тренировочную и тестовую выборки
    texts = df['preprocessed_text'].values
    X_train, X_test, y_train_topics, y_test_topics, y_train_sents, y_test_sents = train_test_split(
        texts, topic_labels, sentiment_labels, test_size=0.2, random_state=42
    )

    return X_train, X_test, y_train_topics, y_test_topics, y_train_sents, y_test_sents, topic_classes

# 4. Инициализация модели
class MultiTaskModel(torch.nn.Module):
    def __init__(self, num_topics, num_sentiments):
        super(MultiTaskModel, self).__init__()
        self.bert = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=1, output_hidden_states=True)
        self.dropout = torch.nn.Dropout(0.1)
        self.attention = torch.nn.MultiheadAttention(self.bert.config.hidden_size, num_heads=8)
        self.topic_head = torch.nn.Linear(self.bert.config.hidden_size, num_topics)
        self.sentiment_head = torch.nn.Linear(self.bert.config.hidden_size, num_sentiments)

    def forward(self, input_ids, attention_mask):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        pooled_output = outputs.hidden_states[-1][:, 0, :]  # [CLS] токен из последнего слоя
        pooled_output = pooled_output.unsqueeze(0)  # Добавляем размерность для внимания
        attn_output, _ = self.attention(pooled_output, pooled_output, pooled_output)
        attn_output = attn_output.squeeze(0)
        attn_output = self.dropout(attn_output)
        topic_logits = self.topic_head(attn_output)
        sentiment_logits = self.sentiment_head(attn_output)
        return topic_logits, sentiment_logits

# 5. Обучение
def train_model(model, train_loader, val_loader, device, num_epochs):
    optimizer = AdamW(model.parameters(), lr=LEARNING_RATE)
    best_loss = float('inf')

    for epoch in range(num_epochs):
        model.train()
        total_loss = 0
        train_progress = tqdm(train_loader, desc=f"Эпоха {epoch+1}/{num_epochs}", leave=False, dynamic_ncols=True)
        for batch in train_progress:
            optimizer.zero_grad()
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            topics = batch["topics"].to(device)
            sentiments = batch["sentiments"].to(device)

            # Проверка устройства для отладки
            print(f"input_ids device: {input_ids.device}, topics device: {topics.device}")

            topic_logits, sentiment_logits = model(input_ids, attention_mask)

            # Loss для тем (BCE)
            topic_loss = torch.nn.functional.binary_cross_entropy_with_logits(
                topic_logits, topics
            )
            # Loss для тональности (CE)
            sentiment_loss = torch.nn.functional.cross_entropy(
                sentiment_logits, sentiments.argmax(dim=1)
            )
            loss = topic_loss + sentiment_loss
            total_loss += loss.item()

            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()

            train_progress.set_postfix({'loss': f'{loss.item():.4f}'})

        avg_train_loss = total_loss / len(train_loader)
        print(f"Эпоха {epoch+1}/{num_epochs}, Потеря на обучении: {avg_train_loss:.4f}")

        # Валидация
        model.eval()
        val_loss = 0
        val_progress = tqdm(val_loader, desc="Валидация", leave=False, dynamic_ncols=True)
        with torch.no_grad():
            for batch in val_progress:
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)
                topics = batch["topics"].to(device)
                sentiments = batch["sentiments"].to(device)

                topic_logits, sentiment_logits = model(input_ids, attention_mask)
                topic_loss = torch.nn.functional.binary_cross_entropy_with_logits(
                    topic_logits, topics
                )
                sentiment_loss = torch.nn.functional.cross_entropy(
                    sentiment_logits, sentiments.argmax(dim=1)
                )
                loss = topic_loss + sentiment_loss
                val_loss += loss.item()

                val_progress.set_postfix({'loss': f'{loss.item():.4f}'})

        avg_val_loss = val_loss / len(val_loader)
        print(f"Потеря на валидации: {avg_val_loss:.4f}")
        if avg_val_loss < best_loss:
            best_loss = avg_val_loss
            os.makedirs("models", exist_ok=True)
            torch.save(model.state_dict(), "models/best_model.pt")

# 6. Функция оценки на тестовой выборке
def evaluate_model(model, test_loader, device, topic_classes):
    model.eval()
    all_topic_preds = []
    all_topic_labels = []
    all_sent_preds = []
    all_sent_labels = []

    with torch.no_grad():
        for batch in test_loader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            topics = batch["topics"].to(device)
            sentiments = batch["sentiments"].to(device)

            topic_logits, sentiment_logits = model(input_ids, attention_mask)

            topic_preds = (torch.sigmoid(topic_logits) > 0.5).cpu().numpy()
            sent_preds = torch.argmax(sentiment_logits, dim=1).cpu().numpy()

            all_topic_preds.extend(topic_preds)
            all_topic_labels.extend(topics.cpu().numpy())
            all_sent_preds.extend(sent_preds)
            all_sent_labels.extend(sentiments.argmax(dim=1).cpu().numpy())

    # Метрики для тем (F1-micro, precision, recall)
    f1_topics = f1_score(all_topic_labels, all_topic_preds, average='micro')
    precision_topics = precision_score(all_topic_labels, all_topic_preds, average='micro')
    recall_topics = recall_score(all_topic_labels, all_topic_preds, average='micro')

    # Метрики для тональности (Accuracy, precision, recall, confusion matrix)
    accuracy_sents = accuracy_score(all_sent_labels, all_sent_preds)
    precision_sents = precision_score(all_sent_labels, all_sent_preds, average='weighted')
    recall_sents = recall_score(all_sent_labels, all_sent_preds, average='weighted')
    cm_sents = confusion_matrix(all_sent_labels, all_sent_preds)

    print("Метрики на тестовой выборке:")
    print(f"Для тем: F1-micro = {f1_topics:.4f}, Precision = {precision_topics:.4f}, Recall = {recall_topics:.4f}")
    print(f"Для тональности: Accuracy = {accuracy_sents:.4f}, Precision = {precision_sents:.4f}, Recall = {recall_sents:.4f}")
    print(f"Confusion matrix для тональности:\n {cm_sents}")

# 7. Основной скрипт
if __name__ == "__main__":
    # Загрузка данных
    X_train, X_test, y_train_topics, y_test_topics, y_train_sents, y_test_sents, topic_classes = prepare_data(
        "data/prepared/processed_gazprom.csv"
    )

    # Токенизатор
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    # Создание датасетов
    train_dataset = ReviewDataset(X_train, y_train_topics, y_train_sents, tokenizer)
    test_dataset = ReviewDataset(X_test, y_test_topics, y_test_sents, tokenizer)
    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE)

    # Инициализация модели
    num_topics = len(topic_classes)
    num_sentiments = 4  # отрицательно, нейтрально, положительно, unknown
    model = MultiTaskModel(num_topics, num_sentiments).to(DEVICE)

    # Обучение
    train_model(model, train_loader, test_loader, DEVICE, NUM_EPOCHS)
    print("Обучение завершено. Лучшая модель сохранена как models/best_model.pt")

    # Оценка на тестовой выборке
    evaluate_model(model, test_loader, DEVICE, topic_classes)