# train_cat.py - Оптимизирован для обучения за ~25 минут с использованием ≤8 ГБ VRAM

import json
import os
import torch
from torch.utils.data import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.model_selection import train_test_split
import numpy as np
import time
import logging

# Настройка логирования
os.makedirs('./results/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./results/logs/training_log.txt'),
        logging.StreamHandler()
    ]
)

# Шаг 1: Загрузка данных из JSONL
def load_data(file_path):
    reviews = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                item = json.loads(line.strip())
                data = item.get('data', {})
                predictions = item.get('predictions', {})
                text = data.get('text', '')
                topics = predictions.get('topics', [])
                sentiments = predictions.get('sentiments', [])
                if len(topics) == len(sentiments) and text:
                    reviews.append({
                        'text': text,
                        'topics': topics,
                        'sentiments': sentiments
                    })
    return reviews

start_time = time.time()
data_file = './processed_multi_label_reviews_10k.jsonl'  # Файл в той же директории, масштабируемый
reviews = load_data(data_file)
logging.info(f"Загружено {len(reviews)} отзывов. Время: {time.time() - start_time:.2f} секунд.")

# Шаг 2: Извлечение уникальных тем и сентиментов
start_time = time.time()
all_topics = set()
all_sentiments = set()
for review in reviews:
    all_topics.update(review['topics'])
    all_sentiments.update(review['sentiments'])

topics_list = sorted(list(all_topics))
sentiments_list = sorted(list(all_sentiments))

# Добавление 'нейтральная', если отсутствует
if 'нейтральная' not in sentiments_list:
    sentiments_list.append('нейтральная')

logging.info(f"Уникальные темы: {topics_list}")
logging.info(f"Уникальные сентименты: {sentiments_list}")
logging.info(f"Шаг 2 завершен. Время: {time.time() - start_time:.2f} секунд.")

# Шаг 3: Подготовка данных для классификации тем (мульти-лейбл)
start_time = time.time()
mlb = MultiLabelBinarizer(classes=topics_list)
topic_labels = mlb.fit_transform([r['topics'] for r in reviews])

# Создание словаря для сентиментов
sentiment_to_id = {s: i for i, s in enumerate(sentiments_list)}
num_sentiments = len(sentiments_list)

# Матрица сентиментов: (num_samples, num_topics, num_sentiments)
sentiment_matrix = np.zeros((len(reviews), len(topics_list), num_sentiments))
for i, review in enumerate(reviews):
    for j, topic in enumerate(review['topics']):
        sent_id = sentiment_to_id[review['sentiments'][j]]
        topic_idx = topics_list.index(topic)
        sentiment_matrix[i, topic_idx, sent_id] = 1

# Тексты уже предобработаны в JSONL
texts = [r['text'] for r in reviews]

# Разделение данных на тренировочные и тестовые
train_texts, test_texts, train_topic_labels, test_topic_labels, train_sent_matrix, test_sent_matrix = train_test_split(
    texts, topic_labels, sentiment_matrix, test_size=0.2, random_state=42
)
logging.info(f"Разделение данных: Тренировочные: {len(train_texts)}, Тестовые: {len(test_texts)}")
logging.info(f"Шаг 3 завершен. Время: {time.time() - start_time:.2f} секунд.")

# Шаг 4: Модель для классификации тем (мульти-лейбл)
start_time = time.time()
model_name = 'cointegrated/rubert-tiny2'  # Легкая модель для быстрого обучения
tokenizer = AutoTokenizer.from_pretrained(model_name)

class TopicDataset(Dataset):
    def __init__(self, texts, labels):
        self.encodings = tokenizer(texts, truncation=True, padding=True, max_length=128)  # Уменьшено для скорости
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx], dtype=torch.float)
        return item

    def __len__(self):
        return len(self.labels)

train_topic_dataset = TopicDataset(train_texts, train_topic_labels)
test_topic_dataset = TopicDataset(test_texts, test_topic_labels)

# Модель для мульти-лейбл классификации
topic_model = AutoModelForSequenceClassification.from_pretrained(
    model_name, 
    num_labels=len(topics_list), 
    problem_type="multi_label_classification",
    use_safetensors=True
)

# Расчет шагов для warmup
approx_steps_per_epoch = len(train_topic_dataset) / 12  # batch_size=12
total_steps = 1 * approx_steps_per_epoch
warmup_steps = int(0.1 * total_steps)

training_args = TrainingArguments(
    output_dir='./results/topics',
    num_train_epochs=1,
    per_device_train_batch_size=12,
    per_device_eval_batch_size=12,
    gradient_accumulation_steps=1,
    warmup_steps=warmup_steps,
    weight_decay=0.01,
    logging_dir='./logs/topics',
    logging_steps=50,
    eval_strategy="epoch",
    save_strategy="epoch",
    save_steps=500,
    load_best_model_at_end=True,
    dataloader_num_workers=2,
    fp16=True,
    gradient_checkpointing=True,
    report_to=None,
)

def compute_metrics(p):
    preds = (torch.sigmoid(torch.tensor(p.predictions)) > 0.5).numpy().astype(int)
    labels = p.label_ids.astype(int)
    from sklearn.metrics import f1_score, precision_score, recall_score
    return {
        'f1_micro': f1_score(labels, preds, average='micro', zero_division=0),
        'precision_micro': precision_score(labels, preds, average='micro', zero_division=0),
        'recall_micro': recall_score(labels, preds, average='micro', zero_division=0)
    }

trainer_topic = Trainer(
    model=topic_model,
    args=training_args,
    train_dataset=train_topic_dataset,
    eval_dataset=test_topic_dataset,
    compute_metrics=compute_metrics
)

trainer_topic.train()
trainer_topic.save_model('./results/topic_model')
logging.info(f"Обучение модели тем завершено. Время: {time.time() - start_time:.2f} секунд.")

# Шаг 5: Модель для классификации сентиментов (мульти-класс по темам)
start_time = time.time()
# Подготовка данных: один пример на пару отзыв-тема
sent_train_texts = []
sent_train_labels = []
for i in range(len(train_texts)):
    for j, topic in enumerate(topics_list):
        if train_topic_labels[i][j] == 1:
            prompt = f"Текст: {train_texts[i]} [SEP] Тема: {topic}"
            sent_train_texts.append(prompt)
            sent_label = np.argmax(train_sent_matrix[i, j])
            sent_train_labels.append(sent_label)

sent_test_texts = []
sent_test_labels = []
for i in range(len(test_texts)):
    for j, topic in enumerate(topics_list):
        if test_topic_labels[i][j] == 1:
            prompt = f"Текст: {test_texts[i]} [SEP] Тема: {topic}"
            sent_test_texts.append(prompt)
            sent_label = np.argmax(test_sent_matrix[i, j])
            sent_test_labels.append(sent_label)

logging.info(f"Образцы для сентиментов: Тренировочные: {len(sent_train_texts)}, Тестовые: {len(sent_test_texts)}")

class SentimentDataset(Dataset):
    def __init__(self, texts, labels):
        self.encodings = tokenizer(texts, truncation=True, padding=True, max_length=128)
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx], dtype=torch.long)
        return item

    def __len__(self):
        return len(self.labels)

train_sent_dataset = SentimentDataset(sent_train_texts, sent_train_labels)
test_sent_dataset = SentimentDataset(sent_test_texts, sent_test_labels)

# Модель для мульти-класс классификации
sent_model = AutoModelForSequenceClassification.from_pretrained(
    model_name, 
    num_labels=num_sentiments,
    use_safetensors=True
)

# Расчет шагов для warmup
approx_steps_per_epoch_sent = len(train_sent_dataset) / 12
total_steps_sent = 1 * approx_steps_per_epoch_sent
warmup_steps_sent = int(0.1 * total_steps_sent)

training_args_sent = TrainingArguments(
    output_dir='./results/sentiments',
    num_train_epochs=1,
    per_device_train_batch_size=12,
    per_device_eval_batch_size=12,
    gradient_accumulation_steps=1,
    warmup_steps=warmup_steps_sent,
    weight_decay=0.01,
    logging_dir='./logs/sentiments',
    logging_steps=50,
    eval_strategy="epoch",
    save_strategy="epoch",
    save_steps=500,
    load_best_model_at_end=True,
    dataloader_num_workers=2,
    fp16=True,
    gradient_checkpointing=True,
    report_to=None,
)

def compute_sent_metrics(p):
    preds = np.argmax(p.predictions, axis=1)
    labels = p.label_ids
    from sklearn.metrics import accuracy_score, f1_score
    return {
        'accuracy': accuracy_score(labels, preds),
        'f1_macro': f1_score(labels, preds, average='macro', zero_division=0)
    }

trainer_sent = Trainer(
    model=sent_model,
    args=training_args_sent,
    train_dataset=train_sent_dataset,
    eval_dataset=test_sent_dataset,
    compute_metrics=compute_sent_metrics
)

trainer_sent.train()
trainer_sent.save_model('./results/sentiment_model')
logging.info(f"Обучение модели сентиментов завершено. Время: {time.time() - start_time:.2f} секунд.")

# Финальная оценка
topic_eval = trainer_topic.evaluate()
logging.info(f"Метрики оценки модели тем: {topic_eval}")

sent_eval = trainer_sent.evaluate()
logging.info(f"Метрики оценки модели сентиментов: {sent_eval}")
logging.info(f"Обучение завершено. Общее время: {time.time() - start_time:.2f} секунд.")