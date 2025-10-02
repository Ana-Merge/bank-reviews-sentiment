import json
import nltk
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from preprocess import preprocess_text
import numpy as np
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация FastAPI с документацией для Swagger UI
app = FastAPI(
    title="API классификации отзывов",
    description="API для предсказания тем и сентиментов отзывов с использованием моделей rubert-tiny2",
    version="1.0.0",
    docs_url="/docs",  # Явно указываем путь для Swagger UI
    redoc_url="/redoc"  # Путь для ReDoc
)

# Проверка наличия файлов модели
def check_model_files(path):
    required_files = ["config.json", "model.safetensors"]
    return all(os.path.exists(os.path.join(path, f)) for f in required_files)

# Загрузка токенизаторов и моделей
try:
    logger.info("Загрузка токенизаторов...")
    topic_tokenizer = AutoTokenizer.from_pretrained("cointegrated/rubert-tiny2")
    sentiment_tokenizer = AutoTokenizer.from_pretrained("cointegrated/rubert-tiny2")
except Exception as e:
    logger.error(f"Ошибка загрузки токенизаторов: {e}")
    raise

try:
    logger.info("Проверка наличия файлов модели в /app/results/topic_model...")
    if not check_model_files("/app/results/topic_model"):
        raise FileNotFoundError("Отсутствуют файлы модели в /app/results/topic_model")
    logger.info("Проверка наличия файлов модели в /app/results/sentiment_model...")
    if not check_model_files("/app/results/sentiment_model"):
        raise FileNotFoundError("Отсутствуют файлы модели в /app/results/sentiment_model")

    logger.info("Загрузка моделей...")
    topic_model = AutoModelForSequenceClassification.from_pretrained(
        "/app/results/topic_model", local_files_only=True
    )
    sentiment_model = AutoModelForSequenceClassification.from_pretrained(
        "/app/results/sentiment_model", local_files_only=True
    )
except Exception as e:
    logger.error(f"Ошибка загрузки моделей: {e}")
    raise

# Установка устройства (CPU для виртуальной машины)
device = torch.device("cpu")
topic_model.to(device)
sentiment_model.to(device)
logger.info(f"Модели загружены на устройство: {device}")

# Определение классов
class ReviewRequest(BaseModel):
    reviews: list[str]

# Предсказание
@app.post("/predict", summary="Классификация отзывов", description="Принимает список отзывов и возвращает предсказанные темы и сентименты")
async def predict(request: ReviewRequest):
    reviews = request.reviews
    results = []

    for review in reviews:
        # Предобработка текста
        processed_review = preprocess_text(review)

        # Токенизация для модели тем
        topic_inputs = topic_tokenizer(
            processed_review, return_tensors="pt", truncation=True, padding=True, max_length=128
        ).to(device)

        # Предсказание тем
        with torch.no_grad():
            topic_outputs = topic_model(**topic_inputs).logits
            topic_probs = torch.softmax(topic_outputs, dim=1).cpu().numpy()
            topic_id = np.argmax(topic_probs, axis=1)[0]
            topic = topic_model.config.id2label[topic_id]

        # Токенизация для модели сентиментов
        sentiment_inputs = sentiment_tokenizer(
            processed_review, return_tensors="pt", truncation=True, padding=True, max_length=128
        ).to(device)

        # Предсказание сентимента
        with torch.no_grad():
            sentiment_outputs = sentiment_model(**sentiment_inputs).logits
            sentiment_probs = torch.softmax(sentiment_outputs, dim=1).cpu().numpy()
            sentiment_id = np.argmax(sentiment_probs, axis=1)[0]
            sentiment = sentiment_model.config.id2label[sentiment_id]

        results.append({"review": review, "topic": topic, "sentiment": sentiment})

    return {"predictions": results}