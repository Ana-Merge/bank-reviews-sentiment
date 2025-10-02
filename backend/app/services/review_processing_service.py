import logging
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.model_service import ReviewAnalysisModel
from app.repositories.repositories import ReviewsForModelRepository, ReviewRepository, ProductRepository
from app.models.models import Review, ReviewProduct, Product, Sentiment
from datetime import datetime, date
import asyncio

logger = logging.getLogger(__name__)

class ReviewProcessingService:
    def __init__(self, model_service: ReviewAnalysisModel):
        self.model_service = model_service
        self.batch_size = 50
    
    async def process_unprocessed_reviews(
        self, 
        session: AsyncSession, 
        limit: int = 250
    ) -> Dict[str, Any]:
        """
        Основной метод обработки непроанализированных отзывов
        """
        try:
            reviews_for_model_repo = ReviewsForModelRepository()
            unprocessed_reviews = await reviews_for_model_repo.get_unprocessed(
                session, limit=limit
            )
            
            if not unprocessed_reviews:
                return {
                    "status": "success",
                    "message": "Нет непроанализированных отзывов",
                    "processed_count": 0,
                    "results": []
                }
            
            logger.info(f"Найдено {len(unprocessed_reviews)} отзывов для анализа")
            
            # Подготавливаем тексты для модели
            texts = [review.review_text for review in unprocessed_reviews]
            
            # Анализируем с помощью модели
            analysis_results = self.model_service.predict_batch(texts)
            
            # Сохраняем результаты в основную таблицу отзывов
            saved_reviews = await self._save_analysis_results(
                session, unprocessed_reviews, analysis_results
            )
            
            # Помечаем отзывы как обработанные
            review_ids = [review.id for review in unprocessed_reviews]
            await reviews_for_model_repo.mark_bulk_as_processed(session, review_ids)
            
            return {
                "status": "success",
                "message": f"Успешно обработано {len(saved_reviews)} отзывов",
                "processed_count": len(saved_reviews),
                "results": analysis_results
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки отзывов: {e}")
            return {
                "status": "error",
                "message": f"Ошибка обработки: {str(e)}",
                "processed_count": 0,
                "results": []
            }
    
    async def _save_analysis_results(
        self,
        session: AsyncSession,
        original_reviews: List[Any],
        analysis_results: List[Dict[str, Any]]
    ) -> List[Review]:
        """
        Сохраняет результаты анализа в основную таблицу отзывов
        """
        saved_reviews = []
        review_repo = ReviewRepository()
        product_repo = ProductRepository()
        
        for i, (original_review, analysis) in enumerate(zip(original_reviews, analysis_results)):
            try:
                # Создаем отзыв для основной таблицы
                review = Review(
                    text=analysis["text"],
                    date=original_review.review_timestamp.date() if original_review.review_timestamp else date.today(),
                    rating=self._sentiment_to_rating(analysis["sentiment_score"]),
                    sentiment=analysis["predicted_sentiment"],
                    sentiment_score=analysis["sentiment_score"],
                    source=original_review.bank_slug or "model_analysis",
                    created_at=datetime.utcnow()
                )
                
                # Сохраняем отзыв
                saved_review = await review_repo.save(session, review)
                
                # Находим или создаем продукт
                product = await self._get_or_create_product(
                    session, product_repo, analysis["product_suggestion"]
                )
                
                # Связываем отзыв с продуктом
                await review_repo.add_products_to_review(
                    session, saved_review.id, [product.id]
                )
                
                saved_reviews.append(saved_review)
                
            except Exception as e:
                logger.error(f"Ошибка сохранения отзыва {i}: {e}")
                continue
        
        return saved_reviews
    
    def _sentiment_to_rating(self, sentiment_score: float) -> int:
        """Конвертирует sentiment score в рейтинг 1-5"""
        if sentiment_score >= 0.8:
            return 5
        elif sentiment_score >= 0.6:
            return 4
        elif sentiment_score >= 0.4:
            return 3
        elif sentiment_score >= 0.2:
            return 2
        else:
            return 1
    
    async def _get_or_create_product(
        self, 
        session: AsyncSession, 
        product_repo: ProductRepository,
        product_suggestion: str
    ) -> Product:
        """
        Находит существующий продукт или создает новый
        """
        # Пытаемся найти продукт по названию
        product_name_map = {
            "customer_service": "Качество обслуживания",
            "mobile_app": "Мобильное приложение",
            "credit_cards": "Кредитные карты", 
            "security": "Безопасность",
            "account_opening": "Открытие счета",
            "general": "Общее"
        }
        
        product_name = product_name_map.get(product_suggestion, "Общее")
        existing_product = await product_repo.get_by_name(session, product_name)
        
        if existing_product:
            return existing_product
        
        # Создаем новый продукт
        from app.models.models import ProductType, ClientType
        new_product = Product(
            name=product_name,
            type=ProductType.PRODUCT,
            client_type=ClientType.BOTH,
            level=1,
            description=f"Автоматически созданный продукт для темы: {product_suggestion}"
        )
        
        return await product_repo.save(session, new_product)