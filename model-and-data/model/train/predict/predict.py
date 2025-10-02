import json
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging
import os
import time
from preprocess import preprocess_text
import uvicorn
from typing import List

os.makedirs('/app/results/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/results/logs/predict_log.txt'),
        logging.StreamHandler()
    ]
)

app = FastAPI(
    title="API классификации отзывов",
    description="API для предсказания тем и сентиментов отзывов с использованием моделей rubert-tiny2",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

model_name = 'cointegrated/rubert-tiny2'
tokenizer = AutoTokenizer.from_pretrained(model_name)

topic_model_path = '/app/results/topic_model'
if not os.path.exists(topic_model_path):
    logging.error(f"Директория модели тем не найдена: {topic_model_path}")
    raise FileNotFoundError(f"Директория модели тем не найдена: {topic_model_path}")
topic_model = AutoModelForSequenceClassification.from_pretrained(topic_model_path, local_files_only=True)
topic_model.eval()

sent_model_path = '/app/results/sentiment_model'
if not os.path.exists(sent_model_path):
    logging.error(f"Директория модели сентиментов не найдена: {sent_model_path}")
    raise FileNotFoundError(f"Директория модели сентиментов не найдена: {sent_model_path}")
sent_model = AutoModelForSequenceClassification.from_pretrained(sent_model_path, local_files_only=True)
sent_model.eval()

topics_list = []
sentiments_list = ['негативная', 'положительная', 'нейтральная']

def load_lists_from_logs(log_path='/app/results/logs/training_log.txt'):
    global topics_list, sentiments_list
    if os.path.exists(log_path):
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                if "Уникальные темы:" in line:
                    topics_str = line.split("Уникальные темы: ")[1].strip()
                    topics_list = eval(topics_str)
                if "Уникальные сентименты:" in line:
                    sents_str = line.split("Уникальные сентименты: ")[1].strip()
                    sentiments_list = eval(sents_str)
                    if 'нейтральная' not in sentiments_list:
                        sentiments_list.append('нейтральная')

load_lists_from_logs()

@app.get("/status")
async def status():
    return {"status": "running", "message": "API готово к использованию, модели загружены"}

def predict_topics_batch(texts, batch_size=32):
    predictions = []
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i + batch_size]
        inputs = tokenizer(batch_texts, return_tensors="pt", truncation=True, padding=True, max_length=128)
        inputs = {k: v.to('cpu') for k, v in inputs.items()}
        with torch.no_grad():
            outputs = topic_model(**inputs)
        probs = torch.sigmoid(outputs.logits)
        batch_preds = [[topics_list[j] for j in range(len(topics_list)) if probs[k][j] > 0.2] for k in range(len(batch_texts))]
        predictions.extend(batch_preds)
    return predictions

def predict_sentiment_batch(text_topic_pairs, batch_size=32):
    predictions = []
    prompts = [f"Текст: {text} [SEP] Тема: {topic}" for text, topic in text_topic_pairs]
    for i in range(0, len(prompts), batch_size):
        batch_prompts = prompts[i:i + batch_size]
        inputs = tokenizer(batch_prompts, return_tensors="pt", truncation=True, padding=True, max_length=128)
        inputs = {k: v.to('cpu') for k, v in inputs.items()}
        with torch.no_grad():
            outputs = sent_model(**inputs)
        pred_ids = torch.argmax(outputs.logits, dim=1).tolist()
        batch_sents = [sentiments_list[pred_id] for pred_id in pred_ids]
        predictions.extend(batch_sents)
    return predictions

def predict_batch_reviews(raw_texts):
    start_time = time.time()
    processed_texts = [preprocess_text(text) for text in raw_texts]
    logging.info(f"Обработано {len(processed_texts)} текстов.")

    predicted_topics_list = predict_topics_batch(processed_texts)

    text_topic_pairs = []
    sentiment_indices = []
    current_index = 0
    for topics in predicted_topics_list:
        for topic in topics:
            text_topic_pairs.append((processed_texts[current_index], topic))
        sentiment_indices.append((len(text_topic_pairs) - len(topics), len(text_topic_pairs)))
        current_index += 1

    predicted_sentiments_all = predict_sentiment_batch(text_topic_pairs)

    results = []
    for i, (start, end) in enumerate(sentiment_indices):
        result = {
            "data": {
                "text": raw_texts[i]
            },
            "predictions": {
                "topics": predicted_topics_list[i],
                "sentiments": predicted_sentiments_all[start:end]
            }
        }
        results.append(result)

    logging.info(f"Батч-предсказание завершено. Время: {time.time() - start_time:.2f} секунд.")
    return results

class ReviewItem(BaseModel):
    id: int
    text: str

class ReviewRequest(BaseModel):
    data: List[ReviewItem]

@app.post("/predict")
async def predict_review(request: ReviewRequest):
    try:
        raw_texts = [item.text for item in request.data]
        prediction = predict_batch_reviews(raw_texts)
        
        for i, result in enumerate(prediction):
            result["data"]["id"] = request.data[i].id
        
        return {"predictions": prediction}
    except Exception as e:
        logging.error(f"Ошибка во время предсказания: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)