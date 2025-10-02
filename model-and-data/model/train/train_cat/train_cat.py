import json
import os
import random
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score, confusion_matrix
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
from collections import defaultdict
import time
import logging

# Настройка логирования
os.makedirs('/app/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/training_log.txt'),
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
data_file = '/data/processed_multi_label_reviews_50k.jsonl'
reviews = load_data(data_file)
print(reviews[0])
logging.info(f"Loaded {len(reviews)} reviews. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 2: Извлечение уникальных тем и сентиментов
start_time = time.time()
all_topics = set()
all_sentiments = set()
for review in reviews:
    all_topics.update(review['topics'])
    all_sentiments.update(review['sentiments'])

topics_list = sorted(list(all_topics))
sentiments_list = sorted(list(all_sentiments))  # 'негативная', 'положительная' (нейтральная может отсутствовать в данных)

logging.info(f"Unique topics: {topics_list}")
logging.info(f"Unique sentiments: {sentiments_list}")
logging.info(f"Step 2 completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 3: Подготовка данных для multi-label classification тем
start_time = time.time()
mlb = MultiLabelBinarizer(classes=topics_list)
topic_labels = mlb.fit_transform([r['topics'] for r in reviews])

# Для сентиментов: Создадим словарь {topic: sentiment_id} для каждого отзыва
sentiment_to_id = {s: i for i, s in enumerate(sentiments_list)}
# Добавим 'нейтральная' если нужно, но в данных её нет, так что опираемся на данные
if 'нейтральная' not in sentiment_to_id:
    sentiments_list.append('нейтральная')
    sentiment_to_id['нейтральная'] = len(sentiment_to_id)

num_sentiments = len(sentiments_list)

# Для каждого отзыва создадим матрицу сентиментов: для каждой темы - one-hot sentiment
# Размер: (num_samples, num_topics, num_sentiments)
sentiment_matrix = np.zeros((len(reviews), len(topics_list), num_sentiments))
for i, review in enumerate(reviews):
    for j, topic in enumerate(review['topics']):
        sent_id = sentiment_to_id[review['sentiments'][j]]
        topic_idx = topics_list.index(topic)
        sentiment_matrix[i, topic_idx, sent_id] = 1

texts = [r['text'] for r in reviews]

# Разделение на train/test
train_texts, test_texts, train_topic_labels, test_topic_labels, train_sent_matrix, test_sent_matrix = train_test_split(
    texts, topic_labels, sentiment_matrix, test_size=0.2, random_state=42
)
logging.info(f"Data split: Train samples: {len(train_texts)}, Test samples: {len(test_texts)}")
logging.info(f"Step 3 completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 4: Модель для тем (multi-label classification)
start_time = time.time()
model_name = 'DeepPavlov/rubert-base-cased'  # Более мощная base-модель для RTX 3060 (110M params, ~1-2GB VRAM на batch=8 с fp16)
tokenizer = AutoTokenizer.from_pretrained(model_name)

class TopicDataset(Dataset):
    def __init__(self, texts, labels):
        self.encodings = tokenizer(texts, truncation=True, padding=True, max_length=512)  # Увеличено до стандартного для BERT
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx], dtype=torch.float)  # Multi-label
        return item

    def __len__(self):
        return len(self.labels)

train_topic_dataset = TopicDataset(train_texts, train_topic_labels)
test_topic_dataset = TopicDataset(test_texts, test_topic_labels)

# Модель для multi-label: num_labels = len(topics_list)
topic_model = AutoModelForSequenceClassification.from_pretrained(
    model_name, 
    num_labels=len(topics_list), 
    problem_type="multi_label_classification",
    use_safetensors=True  # Принудительно safetensors для обхода CVE
)

# Calculate approximate total steps for warmup (10% of total training steps)
approx_steps_per_epoch = len(train_topic_dataset) / 8  # batch_size=8
total_steps = 3 * approx_steps_per_epoch  # 3 epochs (для 50k: ~2-4 часа на RTX 3060; для 1M: добавить gradient_accumulation_steps)
warmup_steps = int(0.1 * total_steps)

training_args = TrainingArguments(
    output_dir='./results_topics',
    num_train_epochs=3,  # Для 50k хватит; для 1M — 2-3, с early_stopping
    per_device_train_batch_size=8,  # Для RTX 3060 с fp16: ~6-8GB VRAM; если OOM — уменьшить до 4
    per_device_eval_batch_size=8,
    gradient_accumulation_steps=1,  # Для 1M: увеличить до 4, чтобы эффективный batch=32
    warmup_steps=warmup_steps,
    weight_decay=0.01,
    logging_dir='./logs_topics',
    logging_steps=50,  # Реже для больших датасетов
    eval_strategy="epoch",
    save_strategy="epoch",
    load_best_model_at_end=True,
    dataloader_num_workers=4,  # Увеличено для ускорения на CPU (RTX 3060 имеет сильный CPU)
    fp16=True,  # Обязательно для экономии VRAM
    gradient_checkpointing=True,  # Для больших моделей: экономит память за счёт времени
    report_to=None,  # Disable wandb/tensorboard for simplicity in container
)

def compute_metrics(p):
    preds = (torch.sigmoid(torch.tensor(p.predictions)) > 0.5).numpy().astype(int)
    labels = p.label_ids.astype(int)
    f1_micro = f1_score(labels, preds, average='micro', zero_division=0)
    f1_macro = f1_score(labels, preds, average='macro', zero_division=0)
    f1_samples = f1_score(labels, preds, average='samples', zero_division=0)
    precision_micro = precision_score(labels, preds, average='micro', zero_division=0)
    recall_micro = recall_score(labels, preds, average='micro', zero_division=0)
    return {
        'f1_micro': f1_micro,
        'f1_macro': f1_macro,
        'f1_samples': f1_samples,
        'precision_micro': precision_micro,
        'recall_micro': recall_micro
    }

trainer_topic = Trainer(
    model=topic_model,
    args=training_args,
    train_dataset=train_topic_dataset,
    eval_dataset=test_topic_dataset,
    compute_metrics=compute_metrics
)

trainer_topic.train()
trainer_topic.save_model('./topic_model')
logging.info(f"Topic model training completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 5: Модель для сентиментов (multi-class per topic)
start_time = time.time()
# Подготовим данные: Для каждого отзыва и каждой темы в нём - отдельный sample
sent_train_texts = []
sent_train_labels = []
for i in range(len(train_texts)):
    for j, topic in enumerate(topics_list):
        if train_topic_labels[i][j] == 1:  # Только для релевантных тем
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

logging.info(f"Sentiment train samples: {len(sent_train_texts)}, test samples: {len(sent_test_texts)}")

class SentimentDataset(Dataset):
    def __init__(self, texts, labels):
        self.encodings = tokenizer(texts, truncation=True, padding=True, max_length=512)  # Увеличено
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx], dtype=torch.long)
        return item

    def __len__(self):
        return len(self.labels)

train_sent_dataset = SentimentDataset(sent_train_texts, sent_train_labels)
test_sent_dataset = SentimentDataset(sent_test_texts, sent_test_labels)

# Модель для multi-class: num_labels = num_sentiments
sent_model = AutoModelForSequenceClassification.from_pretrained(
    model_name, 
    num_labels=num_sentiments,
    use_safetensors=True  # Принудительно safetensors для обхода CVE
)

# Similar warmup calculation for sentiment
approx_steps_per_epoch_sent = len(train_sent_dataset) / 8
total_steps_sent = 3 * approx_steps_per_epoch_sent
warmup_steps_sent = int(0.1 * total_steps_sent)

training_args_sent = TrainingArguments(
    output_dir='./results_sentiments',
    num_train_epochs=3,
    per_device_train_batch_size=8,  # Аналогично
    per_device_eval_batch_size=8,
    gradient_accumulation_steps=1,  # Для масштаба
    warmup_steps=warmup_steps_sent,
    weight_decay=0.01,
    logging_dir='./logs_sentiments',
    logging_steps=50,
    eval_strategy="epoch",
    save_strategy="epoch",
    load_best_model_at_end=True,
    dataloader_num_workers=4,
    fp16=True,
    gradient_checkpointing=True,  # Добавлено
    report_to=None,
)

def compute_sent_metrics(p):
    preds = np.argmax(p.predictions, axis=1)
    labels = p.label_ids
    acc = accuracy_score(labels, preds)
    f1_macro = f1_score(labels, preds, average='macro', zero_division=0)
    f1_micro = f1_score(labels, preds, average='micro', zero_division=0)
    f1_weighted = f1_score(labels, preds, average='weighted', zero_division=0)
    precision_macro = precision_score(labels, preds, average='macro', zero_division=0)
    recall_macro = recall_score(labels, preds, average='macro', zero_division=0)
    cm = confusion_matrix(labels, preds)
    return {
        'accuracy': acc,
        'f1_macro': f1_macro,
        'f1_micro': f1_micro,
        'f1_weighted': f1_weighted,
        'precision_macro': precision_macro,
        'recall_macro': recall_macro,
        'confusion_matrix': cm.tolist()  # Для логирования как списка
    }

trainer_sent = Trainer(
    model=sent_model,
    args=training_args_sent,
    train_dataset=train_sent_dataset,
    eval_dataset=test_sent_dataset,
    compute_metrics=compute_sent_metrics
)

trainer_sent.train()
trainer_sent.save_model('./sentiment_model')
logging.info(f"Sentiment model training completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 6: Пример inference
def predict_topics(text):
    device = topic_model.device
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = topic_model(**inputs)
    probs = torch.sigmoid(outputs.logits)
    predicted_topics = [topics_list[i] for i in range(len(topics_list)) if probs[0][i] > 0.5]
    return predicted_topics

def predict_sentiment(text, topic):
    device = sent_model.device
    prompt = f"Текст: {text} [SEP] Тема: {topic}"
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, padding=True, max_length=512)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = sent_model(**inputs)
    pred_id = torch.argmax(outputs.logits, dim=1).item()
    return sentiments_list[pred_id]

# Тест на случайном отзыве
start_time = time.time()
sample_review = reviews[0]
logging.info(f"Sample text: {sample_review['text'][:100]}")
predicted_topics = predict_topics(sample_review['text'])
logging.info(f"Predicted topics: {predicted_topics}")
for topic in predicted_topics:
    sent = predict_sentiment(sample_review['text'], topic)
    logging.info(f"Topic: {topic}, Sentiment: {sent}")
logging.info(f"Inference example completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Финальная оценка на test
start_time = time.time()
topic_eval = trainer_topic.evaluate()
logging.info(f"Topic eval metrics: {topic_eval}")

sent_eval = trainer_sent.evaluate()
logging.info(f"Sentiment eval metrics: {sent_eval}")
logging.info(f"Final evaluation completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 7: Предсказание для конкретного отзыва (ввод от пользователя)
def predict_full_review(text):
    predicted_topics = predict_topics(text)
    predicted_sentiments = []
    for topic in predicted_topics:
        sent = predict_sentiment(text, topic)
        predicted_sentiments.append(sent)
    
    # Формируем вывод в формате как во входном файле
    prediction = {
        "data": {
            "text": text
        },
        "predictions": {
            "topics": predicted_topics,
            "sentiments": predicted_sentiments
        }
    }
    return prediction

# Ввод текста отзыва от пользователя (можно запустить в интерактивном режиме или передать как аргумент)
print("\n=== Предсказание для конкретного отзыва ===")
user_text = "вчера получил предложение офромил новый прекрасный кредит остался доволен всем но раньше давно оформил ужасный отвратительный кредитный карта обманул все условия никогда больше не пришел"
# user_text = "вчера получил предложение офромил новый дебетоывй карта остался доволен всем. но раньше давно оформил ужасный кредит обманул все условия * никогда больше не пришел *"
if user_text:
    start_time = time.time()
    result = predict_full_review(user_text)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    logging.info(f"User review prediction completed. Time taken: {time.time() - start_time:.2f} seconds.")
else:
    logging.info("No user text provided, skipping prediction.")