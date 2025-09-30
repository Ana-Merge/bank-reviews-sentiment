import asyncio
import logging
from app.core.db_manager import get_db
from app.services.model_service import ReviewAnalysisModel
from app.services.review_processing_service import ReviewProcessingService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def batch_process_reviews(limit: int = 250):
    """
    Скрипт для пакетной обработки отзывов
    """
    try:
        async for session in get_db():
            model_service = ReviewAnalysisModel()
            processing_service = ReviewProcessingService(model_service)
            
            result = await processing_service.process_unprocessed_reviews(
                session, limit=limit
            )
            
            logger.info(f"Результат обработки: {result}")
            return result
            
    except Exception as e:
        logger.error(f"Ошибка в пакетной обработке: {e}")
        raise

if __name__ == "__main__":
    # Запуск обработки 250 отзывов
    asyncio.run(batch_process_reviews(250))