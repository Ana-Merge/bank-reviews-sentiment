# train_cat.py
import json
import os
import random
import torch
from torch.utils.data import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from peft import LoraConfig, get_peft_model, TaskType
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
import numpy as np
import time
import logging
import re
import nltk

from preprocess_single_review import preprocess_text

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pymorphy3

morph = pymorphy3.MorphAnalyzer()
custom_stop_words = {'банка', 'банке', 'банк', 'газпром', 'газпромбанк', 'руб', 'рублей', 'деньги', 'санкт',
                     'счет', 'счета', 'альфа', 'втб', 'сбер', 'тинькоф', 'мтс', 'спб', 'гпб', 'газпром', 'санкт', '\\*'}
to_replace = ['(', ')', '«', '»', '`', '\'', '№']
russian_stopwords = set(stopwords.words('russian'))

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
data_file = '/data/processed_multi_label_reviews.jsonl'
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
sentiments_list = sorted(list(all_sentiments))

logging.info(f"Unique topics: {topics_list}")
logging.info(f"Unique sentiments: {sentiments_list}")
logging.info(f"Step 2 completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 3: Подготовка данных
start_time = time.time()
mlb = MultiLabelBinarizer(classes=topics_list)
topic_labels = mlb.fit_transform([r['topics'] for r in reviews])

sentiment_to_id = {s: i for i, s in enumerate(sentiments_list)}
if 'нейтральная' not in sentiment_to_id:
    sentiments_list.append('нейтральная')
    sentiment_to_id['нейтральная'] = len(sentiment_to_id)

num_sentiments = len(sentiments_list)

sentiment_matrix = np.zeros((len(reviews), len(topics_list), num_sentiments))
for i, review in enumerate(reviews):
    for j, topic in enumerate(review['topics']):
        sent_id = sentiment_to_id[review['sentiments'][j]]
        topic_idx = topics_list.index(topic)
        sentiment_matrix[i, topic_idx, sent_id] = 1

texts = [r['text'] for r in reviews]

train_texts, test_texts, train_topic_labels, test_topic_labels, train_sent_matrix, test_sent_matrix = train_test_split(
    texts, topic_labels, sentiment_matrix, test_size=0.2, random_state=42
)
logging.info(f"Data split: Train {len(train_texts)}, Test {len(test_texts)}")
logging.info(f"Step 3 completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 4: Модель тем (LoRA)
start_time = time.time()
model_name = 'cointegrated/rubert-tiny'
tokenizer = AutoTokenizer.from_pretrained(model_name)

class TopicDataset(Dataset):
    def __init__(self, texts, labels):
        self.encodings = tokenizer(texts, truncation=True, padding=True, max_length=256)
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx], dtype=torch.float)
        return item

    def __len__(self):
        return len(self.labels)

train_topic_dataset = TopicDataset(train_texts, train_topic_labels)
test_topic_dataset = TopicDataset(test_texts, test_topic_labels)

base_model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=len(topics_list), problem_type="multi_label_classification")

lora_config = LoraConfig(task_type=TaskType.SEQ_CLS, r=8, lora_alpha=16, lora_dropout=0.1)
topic_model = get_peft_model(base_model, lora_config)

training_args = TrainingArguments(
    output_dir='./topic_model',
    num_train_epochs=1,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    gradient_accumulation_steps=2,
    warmup_steps=100,
    weight_decay=0.01,
    logging_steps=100,
    eval_strategy="no",
    save_strategy="no",
    fp16=True,
    gradient_checkpointing=True,
    dataloader_num_workers=2,
    report_to=None,
)

def compute_metrics(p):
    preds = (torch.sigmoid(torch.tensor(p.predictions)) > 0.5).numpy().astype(int)
    labels = p.label_ids.astype(int)
    return {'f1_micro': f1_score(labels, preds, average='micro', zero_division=0)}

trainer_topic = Trainer(
    model=topic_model,
    args=training_args,
    train_dataset=train_topic_dataset,
    compute_metrics=compute_metrics
)

trainer_topic.train()
trainer_topic.save_model('./topic_model')
logging.info(f"Topic model training completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 5: Модель сентиментов (LoRA)
start_time = time.time()
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

logging.info(f"Sentiment samples: Train {len(sent_train_texts)}, Test {len(sent_test_texts)}")

class SentimentDataset(Dataset):
    def __init__(self, texts, labels):
        self.encodings = tokenizer(texts, truncation=True, padding=True, max_length=256)
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx], dtype=torch.long)
        return item

    def __len__(self):
        return len(self.labels)

train_sent_dataset = SentimentDataset(sent_train_texts, sent_train_labels)
test_sent_dataset = SentimentDataset(sent_test_texts, sent_test_labels)

base_sent_model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=num_sentiments)
sent_model = get_peft_model(base_sent_model, lora_config)

training_args_sent = TrainingArguments(
    output_dir='./sentiment_model',
    num_train_epochs=1,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    gradient_accumulation_steps=2,
    warmup_steps=50,
    weight_decay=0.01,
    logging_steps=100,
    eval_strategy="no",
    save_strategy="no",
    fp16=True,
    gradient_checkpointing=True,
    dataloader_num_workers=2,
    report_to=None,
)

def compute_sent_metrics(p):
    preds = np.argmax(p.predictions, axis=1)
    labels = p.label_ids
    return {'accuracy': f1_score(labels, preds, average='micro', zero_division=0)}

trainer_sent = Trainer(
    model=sent_model,
    args=training_args_sent,
    train_dataset=train_sent_dataset,
    compute_metrics=compute_sent_metrics
)

trainer_sent.train()
trainer_sent.save_model('./sentiment_model')
logging.info(f"Sentiment model training completed. Time taken: {time.time() - start_time:.2f} seconds.")

# Шаг 6: Предсказание на кастомном отзыве
def predict_topics(text):
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    topic_model.to(device)
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=256)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = topic_model(**inputs)
    probs = torch.sigmoid(outputs.logits)
    predicted_topics = [topics_list[i] for i in range(len(topics_list)) if probs[0][i] > 0.5]
    return predicted_topics

def predict_sentiment(text, topic):
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    sent_model.to(device)
    prompt = f"Текст: {text} [SEP] Тема: {topic}"
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, padding=True, max_length=256)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = sent_model(**inputs)
    pred_id = torch.argmax(outputs.logits, dim=1).item()
    return sentiments_list[pred_id]

def predict_full_review(text):
    predicted_topics = predict_topics(text)
    predicted_sentiments = [predict_sentiment(text, topic) for topic in predicted_topics]
    return {
        "data": {"text": text},
        "predictions": {"topics": predicted_topics, "sentiments": predicted_sentiments}
    }

# Ввод текста отзыва от пользователя (можно запустить в интерактивном режиме или передать как аргумент)
print("\n=== Предсказание для конкретного отзыва ===")
user_text = "Кредитную карту одобрили за день, лимит хороший, кэшбэк начисляют исправно. Однако в отделении долго ждал в очереди, хотя был записан на конкретное время."
processed_text = preprocess_text(user_text)
if user_text:
    start_time = time.time()
    result = predict_full_review(processed_text)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    logging.info(f"User review prediction completed. Time taken: {time.time() - start_time:.2f} seconds.")
else:
    logging.info("No user text provided, skipping prediction.")