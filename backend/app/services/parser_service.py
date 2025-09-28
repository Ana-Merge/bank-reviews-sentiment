import asyncio
import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.parser_config import ParserConfig
from app.services.banki_parser import BankiRuParser
from app.repositories.repositories import ReviewsForModelRepository

logger = logging.getLogger(__name__)

class ParserService:
    def __init__(self, reviews_for_model_repo: ReviewsForModelRepository):
        self._reviews_for_model_repo = reviews_for_model_repo

    async def run_parser(
        self, 
        session: AsyncSession, 
        bank_slug: str,
        products: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_pages: int = 100,
        delay_between_requests: float = 1.0
    ) -> Dict[str, Any]:
        """
        Запуск парсера для указанного банка и продуктов
        """
        try:
            config = ParserConfig(
                bank_slug=bank_slug,
                products=products,
                start_date=start_date,
                end_date=end_date,
                max_pages=max_pages,
                delay_between_requests=delay_between_requests
            )

            # Запускаем парсер в отдельном потоке
            parsed_data = await asyncio.to_thread(self._run_sync_parser, config)
            
            # Сохраняем данные в базу
            total_saved = 0
            for product, reviews in parsed_data.items():
                if reviews:
                    saved_count = await self._reviews_for_model_repo.save_parsed_reviews(
                        session, reviews, product
                    )
                    total_saved += saved_count
                    logger.info(f"Saved {saved_count} reviews for product {product}")

            return {
                "status": "success",
                "bank_slug": bank_slug,
                "products_processed": products,
                "total_reviews_parsed": sum(len(reviews) for reviews in parsed_data.values()),
                "total_saved": total_saved,
                "start_date": start_date,
                "end_date": end_date,
                "message": f"Successfully parsed and saved {total_saved} reviews for bank {bank_slug}"
            }
            
        except Exception as e:
            logger.error(f"Parser error: {str(e)}")
            return {
                "status": "error",
                "message": f"Parser failed: {str(e)}"
            }

    def _run_sync_parser(self, config: ParserConfig) -> Dict[str, List]:
        """Запуск синхронного парсера"""
        parser = BankiRuParser(config)
        return parser.parse_bank_products()

    async def get_parsing_status(self, session: AsyncSession, bank_slug: str) -> Dict[str, Any]:
        """Получить статистику по спарсенным данным"""
        # Получаем количество отзывов по продуктам
        products_stats = {}
        total_reviews = 0
        
        # Здесь можно добавить логику для получения статистики из базы
        # Например, количество отзывов по каждому продукту
        
        return {
            "bank_slug": bank_slug,
            "total_reviews": total_reviews,
            "products_stats": products_stats,
            "last_parsed": None  # Можно добавить поле с временем последнего парсинга
        }
    

    async def run_sravni_parser(
        self, 
        session: AsyncSession, 
        bank_slugs: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_pages: int = 100,
        delay_between_requests: float = 1.0
    ) -> Dict[str, Any]:
        """
        Запуск парсера sravni.ru для указанных банков
        """
        try:
            # Запускаем парсер в отдельном потоке
            parsed_data = await asyncio.to_thread(
                self._run_sravni_sync_parser, 
                bank_slugs, start_date, end_date, max_pages, delay_between_requests
            )
            
            # Сохраняем данные в базу
            total_saved = 0
            for bank_slug, reviews in parsed_data.items():
                if reviews:
                    # Для sravni.ru сохраняем все отзывы под общим продуктом "sravni_reviews"
                    # или можно разбить по reviewTag если нужно
                    saved_count = await self._reviews_for_model_repo.save_sravni_reviews(
                        session, reviews, bank_slug
                    )
                    total_saved += saved_count
                    logger.info(f"Saved {saved_count} sravni.ru reviews for bank {bank_slug}")

            return {
                "status": "success",
                "bank_slugs": bank_slugs,
                "total_reviews_parsed": sum(len(reviews) for reviews in parsed_data.values()),
                "total_saved": total_saved,
                "start_date": start_date,
                "end_date": end_date,
                "message": f"Successfully parsed and saved {total_saved} reviews from sravni.ru"
            }
            
        except Exception as e:
            logger.error(f"Sravni parser error: {str(e)}")
            return {
                "status": "error",
                "message": f"Sravni parser failed: {str(e)}"
            }

    def _run_sravni_sync_parser(
        self, 
        bank_slugs: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_pages: int = 100,
        delay_between_requests: float = 1.0
    ) -> Dict[str, List]:
        """Запуск синхронного парсера sravni.ru"""
        from app.services.sravni_parser import SravniRuParser
        
        config = {
            "bank_slugs": bank_slugs,
            "start_date": start_date,
            "end_date": end_date,
            "max_pages": max_pages,
            "delay_between_requests": delay_between_requests
        }
        
        parser = SravniRuParser(config)
        return parser.parse_banks()