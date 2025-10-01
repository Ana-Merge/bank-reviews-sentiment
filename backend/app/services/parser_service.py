import asyncio
import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date
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
        products_stats = {}
        total_reviews = 0
    
        return {
            "bank_slug": bank_slug,
            "total_reviews": total_reviews,
            "products_stats": products_stats,
            "last_parsed": None
        }
    
    async def process_parsed_reviews(
        self,
        session: AsyncSession,
        bank_slug: str,
        product_name: str,
        limit: int = 1000,
        mark_processed: bool = True
    ) -> Dict[str, Any]:
        """
        Обработка спарсенных отзывов и перенос в основные таблицы
        """
        try:
            # Получаем непереработанные отзывы
            unprocessed_reviews = await self._reviews_for_model_repo.get_all(
                session, page=0, size=limit, processed=False
            )
            
            # Фильтруем по банку и продукту
            filtered_reviews = [
                review for review in unprocessed_reviews 
                if review.bank_slug == bank_slug and review.product_name == product_name
            ]
            
            if not filtered_reviews:
                return {
                    "status": "success",
                    "bank_slug": bank_slug,
                    "product_name": product_name,
                    "reviews_processed": 0,
                    "reviews_created": 0,
                    "products_created": 0,
                    "message": "No unprocessed reviews found for specified bank and product"
                }
            
            from app.repositories.repositories import ProductRepository, ReviewRepository
            product_repo = ProductRepository()
            review_repo = ReviewRepository()
            
            product = await product_repo.get_by_name(session, product_name)
            products_created = 0
            
            if not product:
                from app.models.models import Product, ProductType, ClientType
                product = Product(
                    name=product_name,
                    type=ProductType.PRODUCT,
                    client_type=ClientType.BOTH,
                    level=0
                )
                product = await product_repo.save(session, product)
                products_created = 1
                logger.info(f"Created new product: {product_name}")
            
            # Обрабатываем отзывы
            reviews_created = 0
            review_ids_to_mark = []
            
            for parsed_review in filtered_reviews:
                try:
                    # Пропускаем отзывы с оценкой 0
                    rating = self._parse_rating(parsed_review.rating)
                    if rating == 0:
                        continue
                    
                    sentiment = self._determine_sentiment(rating)
                    
                    review_date = self._parse_review_date(parsed_review.review_date)
                    if not review_date:
                        continue
                    
                    source = self._parse_source_from_url(parsed_review.source_url)
                    
                    from app.models.models import Review
                    review = Review(
                        text=parsed_review.review_text,
                        date=review_date,
                        rating=rating,
                        sentiment=sentiment,
                        sentiment_score=self._calculate_sentiment_score(sentiment),
                        source=source
                    )
                    
                    saved_review = await review_repo.save(session, review)
                    await review_repo.add_products_to_review(
                        session, saved_review.id, [product.id]
                    )
                    
                    reviews_created += 1
                    review_ids_to_mark.append(parsed_review.id)
                    
                except Exception as e:
                    logger.error(f"Error processing review {parsed_review.id}: {str(e)}")
                    continue
            
            if mark_processed and review_ids_to_mark:
                await self._reviews_for_model_repo.mark_bulk_as_processed(
                    session, review_ids_to_mark
                )
            
            return {
                "status": "success",
                "bank_slug": bank_slug,
                "product_name": product_name,
                "reviews_processed": len(filtered_reviews),
                "reviews_created": reviews_created,
                "products_created": products_created,
                "message": f"Successfully processed {reviews_created} reviews"
            }
            
        except Exception as e:
            logger.error(f"Error processing parsed reviews: {str(e)}")
            return {
                "status": "error",
                "message": f"Processing failed: {str(e)}"
            }

    def _parse_source_from_url(self, url: str) -> str:
        """Парсит источник из URL строки"""
        if not url:
            return "unknown"
        
        try:
            url_lower = url.lower()
            
            if 'banki.ru' in url_lower:
                return "Banki.ru"
            elif 'sravni.ru' in url_lower:
                return "Sravni.ru"
            else:
                from urllib.parse import urlparse
                parsed_url = urlparse(url)
                domain = parsed_url.netloc
                if domain.startswith('www.'):
                    domain = domain[4:]
                return domain if domain else "unknown"
                
        except Exception as e:
            logger.warning(f"Could not parse source from URL {url}: {str(e)}")
            return "unknown"
        
    def _parse_rating(self, rating_str: str) -> int:
        """Парсит строку с рейтингом в число для обоих источников"""
        if not rating_str or rating_str == 'Без оценки':
            return 0
        
        try:
            import re
            
            cleaned_rating = rating_str.lower()
            cleaned_rating = re.sub(r'[зз]в[её]зд?', '', cleaned_rating) 
            cleaned_rating = re.sub(r'[^0-9/]', '', cleaned_rating)
            
            numbers = re.findall(r'\d+', cleaned_rating)
            if numbers:
                rating = int(numbers[0])
                return min(rating, 5)
            return 0
        except (ValueError, TypeError):
            return 0

    def _parse_review_date(self, date_str: str) -> Optional[date]:
        """Парсит дату отзыва для обоих источников"""
        try:
            from datetime import datetime
            
            if not date_str:
                return None
                
            date_formats = [
                '%d.%m.%Y %H:%M',     # 01.01.2023 14:30 (Banki.ru)
                '%d.%m.%Y',           # 01.01.2023
                '%Y-%m-%d',           # 2023-01-01
                '%d %B %Y',           # 01 января 2023 (Sravni.ru)
                '%d %b %Y',           # 01 янв 2023 (Sravni.ru)
            ]
            
            for date_format in date_formats:
                try:
                    return datetime.strptime(date_str, date_format).date()
                except ValueError:
                    continue
            
            import re
            date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
            if date_match:
                day, month, year = date_match.groups()
                return datetime(int(year), int(month), int(day)).date()
                
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing date {date_str}: {str(e)}")
            return None

    def _determine_sentiment(self, rating: int) -> str:
        """Определяет тональность на основе рейтинга"""
        if rating >= 4:
            return "positive"
        elif rating == 3:
            return "neutral"
        elif rating >= 1:
            return "negative"
        else:
            return "neutral"

    def _calculate_sentiment_score(self, sentiment: str) -> float:
        """Рассчитывает числовой score тональности"""
        scores = {
            "positive": 0.8,
            "neutral": 0.0,
            "negative": -0.8
        }
        return scores.get(sentiment, 0.0)



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
            parsed_data = await asyncio.to_thread(
                self._run_sravni_sync_parser, 
                bank_slugs, start_date, end_date, max_pages, delay_between_requests
            )
            
            total_saved = 0
            for bank_slug, reviews in parsed_data.items():
                if reviews:
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