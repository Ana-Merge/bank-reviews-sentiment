import torch
import pandas as pd
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from typing import List, Dict, Any
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.repositories import ReviewsForModelRepository
from app.models.models import ReviewsForModel, Review, Product, ReviewProduct
from sqlalchemy import select
from datetime import datetime, date
import asyncio

logger = logging.getLogger(__name__)

class ReviewAnalysisModel:
    def __init__(self, model_path: str = "models/best_model.pt"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model_name = "DeepPavlov/rubert-base-cased"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = self._load_model(model_path)
        self.max_length = 128
        
        # Предопределенные темы (замените на актуальные из вашей модели)
        self.topic_classes = [
            "качество_обслуживания", "работа_приложения", "проценты_и_тарифы",
            "безопасность", "оформление_продукта", "другое"
        ]
        
        self.sentiment_map = {
            0: "negative",
            1: "neutral", 
            2: "positive",
            3: "unknown"
        }
    
    def _load_model(self, model_path: str):
        """Загрузка предварительно обученной модели"""
        try:
            # Загружаем архитектуру модели
            model = AutoModelForSequenceClassification.from_pretrained(
                self.model_name, 
                num_labels=1,
                output_hidden_states=True
            )
            
            # Загружаем веса
            state_dict = torch.load(model_path, map_location=self.device)
            model.load_state_dict(state_dict)
            model.to(self.device)
            model.eval()
            
            logger.info(f"Модель успешно загружена на {self.device}")
            return model
            
        except Exception as e:
            logger.error(f"Ошибка загрузки модели: {e}")
            raise
    
    def preprocess_text(self, text: str) -> str:
        """Предобработка текста отзыва"""
        # Базовая очистка текста
        text = str(text).strip()
        # Можно добавить более сложную предобработку при необходимости
        return text
    
    def predict_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Предсказание для батча текстов"""
        results = []
        
        try:
            # Токенизация
            encodings = self.tokenizer(
                texts,
                max_length=self.max_length,
                padding="max_length",
                truncation=True,
                return_tensors="pt"
            )
            
            # Перенос на устройство
            input_ids = encodings["input_ids"].to(self.device)
            attention_mask = encodings["attention_mask"].to(self.device)
            
            # Предсказание
            with torch.no_grad():
                outputs = self.model(input_ids=input_ids, attention_mask=attention_mask)
                # Здесь нужно адаптировать под вашу архитектуру модели
                # Это пример - замените на вашу логику предсказания
                
            # Примерная логика обработки результатов
            for i, text in enumerate(texts):
                # Временные заглушки - замените на реальные предсказания
                predicted_topic = self.topic_classes[0]  # Замените на реальное предсказание
                predicted_sentiment = "positive"  # Замените на реальное предсказание
                sentiment_score = 0.8  # Замените на реальное предсказание
                
                results.append({
                    "text": text,
                    "predicted_topic": predicted_topic,
                    "predicted_sentiment": predicted_sentiment,
                    "sentiment_score": sentiment_score,
                    "product_suggestion": self._suggest_product(predicted_topic)
                })
                
        except Exception as e:
            logger.error(f"Ошибка предсказания: {e}")
            # Возвращаем результаты по умолчанию в случае ошибки
            for text in texts:
                results.append({
                    "text": text,
                    "predicted_topic": "другое",
                    "predicted_sentiment": "neutral",
                    "sentiment_score": 0.0,
                    "product_suggestion": "general"
                })
        
        return results
    
    def _suggest_product(self, topic: str) -> str:
        """Сопоставление темы с продуктом"""
        product_mapping = {
            "качество_обслуживания": "customer_service",
            "работа_приложения": "mobile_app", 
            "проценты_и_тарифы": "credit_cards",
            "безопасность": "security",
            "оформление_продукта": "account_opening",
            "другое": "general"
        }
        return product_mapping.get(topic, "general")