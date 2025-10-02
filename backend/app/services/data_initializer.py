import os
import logging
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct, tuple_
from app.scripts.jsonl_loader import JSONLLoader
from app.repositories.repositories import ReviewsForModelRepository
from app.services.parser_service import ParserService
from app.models.models import ReviewsForModel

logger = logging.getLogger(__name__)

class DataInitializer:
    def __init__(self):
        self.reviews_repo = ReviewsForModelRepository()
        self.jsonl_loader = JSONLLoader(self.reviews_repo)
        self.parser_service = ParserService(self.reviews_repo)

    async def initialize_data(self, session: AsyncSession) -> Dict[str, Any]:
        """
        Основная функция инициализации данных при запуске
        """
        results = {
            "base_categories": {},
            "jsonl_loading": {},
            "data_processing": {}
        }

        base_categories_result = await self.parser_service.create_base_categories(session)
        results["base_categories"] = {
            "status": "completed",
            "categories_created": base_categories_result
        }

        jsonl_results = await self._load_jsonl_data(session)
        results["jsonl_loading"] = jsonl_results

        if jsonl_results.get("total_loaded", 0) > 0:
            processing_results = await self._process_loaded_data(session)
            results["data_processing"] = processing_results
        else:
            results["data_processing"] = {
                "status": "skipped",
                "message": "No new data to process"
            }

        return results

    async def _load_jsonl_data(self, session: AsyncSession) -> Dict[str, Any]:
        """
        Загружает данные из всех JSONL файлов в директории data
        """
        data_dir = "/app/app/data"
        total_loaded = 0
        file_results = []

        if not os.path.exists(data_dir):
            logger.warning(f"Data directory {data_dir} does not exist")
            return {
                "status": "skipped",
                "message": f"Data directory {data_dir} not found",
                "total_loaded": 0
            }

        jsonl_files = [f for f in os.listdir(data_dir) if f.endswith('.jsonl')]
        
        if not jsonl_files:
            logger.info("No JSONL files found for loading")
            return {
                "status": "skipped", 
                "message": "No JSONL files found",
                "total_loaded": 0
            }

        for jsonl_file in jsonl_files:
            file_path = os.path.join(data_dir, jsonl_file)
            logger.info(f"Loading data from {file_path}")
            
            result = await self.jsonl_loader.load_from_jsonl_file(session, file_path)
            file_results.append({
                "file": jsonl_file,
                "result": result
            })
            
            if result.get("status") == "success":
                total_loaded += result.get("reviews_saved", 0)

        return {
            "status": "completed",
            "files_processed": len(jsonl_files),
            "total_loaded": total_loaded,
            "file_results": file_results
        }

    async def _process_loaded_data(self, session: AsyncSession) -> Dict[str, Any]:
        """
        Обрабатывает загруженные данные - переносит в основные таблицы reviews и review_products
        """
        processing_results = {}
        total_processed = 0
        total_created = 0
        
        try:
            unique_combinations = await self._get_unique_bank_product_combinations(session)
            
            if not unique_combinations:
                logger.info("No unprocessed reviews found for processing")
                return {
                    "status": "skipped",
                    "message": "No unprocessed reviews found for processing"
                }

            logger.info(f"Found {len(unique_combinations)} unique bank-product combinations to process")

            for bank_slug, product_name in unique_combinations:
                logger.info(f"Processing data for {bank_slug} - {product_name}")
                
                result = await self.parser_service.process_parsed_reviews(
                    session, 
                    bank_slug, 
                    product_name, 
                    limit=900000,
                    mark_processed=True
                )
                
                processing_results[f"{bank_slug}_{product_name}"] = result
                
                if result.get("status") == "success":
                    total_processed += result.get("reviews_processed", 0)
                    total_created += result.get("reviews_created", 0)

            return {
                "status": "completed",
                "combinations_processed": len(unique_combinations),
                "total_reviews_processed": total_processed,
                "total_reviews_created": total_created,
                "results": processing_results
            }

        except Exception as e:
            logger.error(f"Error processing loaded data: {str(e)}")
            return {
                "status": "error",
                "message": f"Processing failed: {str(e)}"
            }

    async def _get_unique_bank_product_combinations(self, session: AsyncSession) -> List[tuple]:
        """
        Получает уникальные комбинации банк-продукт из непереработанных отзывов
        Теперь учитываем, что у одного отзыва может быть несколько продуктов
        """
        from sqlalchemy import select
        from app.models.models import ReviewsForModel

        statement = select(ReviewsForModel).where(ReviewsForModel.processed == False)
        result = await session.execute(statement)
        unprocessed_reviews = result.scalars().all()
        
        combinations = set()
        
        for review in unprocessed_reviews:
            bank_slug = review.bank_slug
            additional_data = review.additional_data or {}
            
            predictions = additional_data.get('predictions', {})
            topics = predictions.get('topics', [])
            
            if topics:
                for topic in topics:
                    combinations.add((bank_slug, topic))
            else:
                combinations.add((bank_slug, review.product_name))
        
        combinations_list = list(combinations)
        logger.info(f"Found {len(combinations_list)} unique combinations: {combinations_list}")
        return combinations_list