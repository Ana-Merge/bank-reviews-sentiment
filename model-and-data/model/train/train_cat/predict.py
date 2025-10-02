# predict.py

import json
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import logging
import os
import time
from preprocess import preprocess_text  # Импорт функции из отдельного файла

# Настройка логирования
os.makedirs('/app/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/predict_log.txt'),
        logging.StreamHandler()
    ]
)

# Загрузка моделей и токенайзера
model_name = 'DeepPavlov/rubert-base-cased'
tokenizer = AutoTokenizer.from_pretrained(model_name)

# Загрузка модели для тем
topic_model = AutoModelForSequenceClassification.from_pretrained('./topic_model')
topic_model.eval()  # Перевод в режим inference

# Загрузка модели для сентиментов
sent_model = AutoModelForSequenceClassification.from_pretrained('./sentiment_model')
sent_model.eval()

# Список тем и сентиментов (взяты из train_cat.py; при необходимости скорректировать)
topics_list = []  # Заполнить реальными темами из логов или конфига после обучения
sentiments_list = ['негативная', 'положительная', 'нейтральная']  # На основе данных

# Функция для загрузки topics_list и sentiments_list из логов (если они сохранены)
def load_lists_from_logs(log_path='/app/logs/training_log.txt'):
    global topics_list, sentiments_list
    with open(log_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            if "Unique topics:" in line:
                topics_str = line.split("Unique topics: ")[1].strip()
                topics_list = eval(topics_str)  # Безопасно, если это список
            if "Unique sentiments:" in line:
                sents_str = line.split("Unique sentiments: ")[1].strip()
                sentiments_list = eval(sents_str)
                if 'нейтральная' not in sentiments_list:
                    sentiments_list.append('нейтральная')

load_lists_from_logs()  # Загрузка списков из логов обучения

def predict_topics(text):
    device = topic_model.device
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=256)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = topic_model(**inputs)
    probs = torch.sigmoid(outputs.logits)
    predicted_topics = [topics_list[i] for i in range(len(topics_list)) if probs[0][i] > 0.5]
    return predicted_topics

def predict_sentiment(text, topic):
    device = sent_model.device
    prompt = f"Текст: {text} [SEP] Тема: {topic}"
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, padding=True, max_length=256)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = sent_model(**inputs)
    pred_id = torch.argmax(outputs.logits, dim=1).item()
    return sentiments_list[pred_id]

def predict_full_review(raw_text):
    start_time = time.time()
    processed_text = preprocess_text(raw_text)
    logging.info(f"Processed text: {processed_text[:100]}...")
    
    predicted_topics = predict_topics(processed_text)
    predicted_sentiments = []
    for topic in predicted_topics:
        sent = predict_sentiment(processed_text, topic)
        predicted_sentiments.append(sent)
    
    prediction = {
        "data": {
            "text": raw_text  # Сохраняем оригинальный текст
        },
        "predictions": {
            "topics": predicted_topics,
            "sentiments": predicted_sentiments
        }
    }
    logging.info(f"Prediction completed. Time taken: {time.time() - start_time:.2f} seconds.")
    return prediction

if __name__ == "__main__":
    print("\n=== Предсказание для конкретного отзыва ===")
    # Пример сырого отзыва (можно передать как аргумент или из stdin)
    user_text = input("Введите сырой текст отзыва: ") or "вчера получил предложение офромил новый прекрасный кредит остался доволен всем. но раньше давно оформил ужасный отвратительный кредитный карта обманул все условия никогда больше не пришел"
    result = predict_full_review(user_text)
    print(json.dumps(result, ensure_ascii=False, indent=2))