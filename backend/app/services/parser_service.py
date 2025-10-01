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
        product_name: str,  # Это оригинальное английское название из reviews_for_model
        limit: int = 10000,
        mark_processed: bool = True
    ) -> Dict[str, Any]:
        """
        Обработка спарсенных отзывов и перенос в основные таблицы
        """
        try:
            logger.info(f"Starting processing for bank_slug: {bank_slug}, product_name: {product_name}")
            
            # Получаем непереработанные отзывы
            unprocessed_reviews = await self._reviews_for_model_repo.get_all(
                session, page=0, size=limit, processed=False
            )
            
            logger.info(f"Total unprocessed reviews: {len(unprocessed_reviews)}")
            
            # Фильтруем по банку и продукту (оригинальное английское название)
            filtered_reviews = [
                review for review in unprocessed_reviews 
                if review.bank_slug == bank_slug and review.product_name == product_name
            ]
            
            logger.info(f"Filtered reviews for {bank_slug}-{product_name}: {len(filtered_reviews)}")
            
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
            
            # ПЕРЕВОДИМ АНГЛИЙСКОЕ НАЗВАНИЕ НА РУССКОЕ
            russian_product_name = self._translate_product_name(product_name)
            logger.info(f"Translated product name: '{product_name}' -> '{russian_product_name}'")
            
            # ПОЛУЧАЕМ РОДИТЕЛЯ ДЛЯ ПРОДУКТА
            parent_product = await self._get_or_create_parent_product(session, product_repo, russian_product_name)
            if parent_product:
                logger.info(f"Parent product: {parent_product.name} (id: {parent_product.id})")
            
            # ИЩЕМ ИЛИ СОЗДАЕМ ПРОДУКТ С РУССКИМ НАЗВАНИЕМ
            product = await product_repo.get_by_name(session, russian_product_name)
            products_created = 0
            
            if not product:
                from app.models.models import Product, ProductType, ClientType
                
                # ОПРЕДЕЛЯЕМ ТИП ПРОДУКТА И УРОВЕНЬ
                product_type, level = self._determine_product_type_and_level(russian_product_name, parent_product)
                
                logger.info(f"Creating new product: {russian_product_name}, type: {product_type}, level: {level}")
                
                product = Product(
                    name=russian_product_name,
                    type=product_type,
                    client_type=ClientType.BOTH,
                    level=level,
                    parent_id=parent_product.id if parent_product else None
                )
                product = await product_repo.save(session, product)
                products_created = 1
                logger.info(f"Created new product: {russian_product_name} (id: {product.id})")
            else:
                logger.info(f"Product already exists: {russian_product_name} (id: {product.id})")
            
            # Обрабатываем отзывы
            reviews_created = 0
            review_ids_to_mark = []
            
            for i, parsed_review in enumerate(filtered_reviews):
                try:
                    logger.info(f"Processing review {i+1}/{len(filtered_reviews)}: ID {parsed_review.id}")
                    
                    # Пропускаем отзывы с оценкой 0
                    rating = self._parse_rating(parsed_review.rating)
                    logger.info(f"Parsed rating: {parsed_review.rating} -> {rating}")
                    
                    if rating == 0:
                        logger.info(f"Skipping review with rating 0: {parsed_review.id}")
                        continue
                    
                    sentiment = self._determine_sentiment(rating)
                    sentiment_score = self._calculate_sentiment_score(sentiment)
                    logger.info(f"Determined sentiment: {sentiment}, score: {sentiment_score}")
                    
                    review_date = self._parse_review_date(parsed_review.review_date)
                    if not review_date:
                        logger.warning(f"Could not parse date: {parsed_review.review_date}")
                        continue
                    
                    logger.info(f"Parsed date: {parsed_review.review_date} -> {review_date}")
                    
                    source = self._parse_source_from_url(parsed_review.source_url)
                    logger.info(f"Parsed source: {source}")
                    
                    from app.models.models import Review, ReviewProduct
                    
                    # СОЗДАЕМ ОТЗЫВ В ОСНОВНОЙ ТАБЛИЦЕ
                    review = Review(
                        text=parsed_review.review_text,
                        date=review_date,
                        rating=rating,
                        sentiment=sentiment,
                        sentiment_score=sentiment_score,
                        source=source
                    )
                    
                    saved_review = await review_repo.save(session, review)
                    logger.info(f"Created review in main table: ID {saved_review.id}")
                    
                    # СОЗДАЕМ СВЯЗЬ В REVIEW_PRODUCTS
                    review_product = ReviewProduct(
                        review_id=saved_review.id,
                        product_id=product.id,
                        sentiment=sentiment,
                        sentiment_score=sentiment_score
                    )
                    session.add(review_product)
                    await session.flush()
                    logger.info(f"Created review_product link: review_id={saved_review.id}, product_id={product.id}")
                    
                    reviews_created += 1
                    review_ids_to_mark.append(parsed_review.id)
                    
                except Exception as e:
                    logger.error(f"Error processing review {parsed_review.id}: {str(e)}", exc_info=True)
                    continue
            
            logger.info(f"Successfully processed {reviews_created} reviews")
            
            if mark_processed and review_ids_to_mark:
                logger.info(f"Marking {len(review_ids_to_mark)} reviews as processed")
                await self._reviews_for_model_repo.mark_bulk_as_processed(
                    session, review_ids_to_mark
                )
            
            return {
                "status": "success",
                "bank_slug": bank_slug,
                "product_name": product_name,
                "russian_product_name": russian_product_name,
                "reviews_processed": len(filtered_reviews),
                "reviews_created": reviews_created,
                "products_created": products_created,
                "product_id": product.id if product else None,
                "message": f"Successfully processed {reviews_created} reviews"
            }
            
        except Exception as e:
            logger.error(f"Error processing parsed reviews: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Processing failed: {str(e)}"
            }

    def _translate_product_name(self, english_name: str) -> str:
        """
        Переводит английские названия продуктов на русский
        """
        translation_map = {
            # Основные категории
            'cards': 'Карты',
            'debitcards': 'Дебетовые карты',
            'creditcards': 'Кредитные карты',
            'credits': 'Кредиты',
            'deposits': 'Вклады и счета',
            'service': 'Обслуживание',
            'other': 'Другие услуги',
            'restructing': 'Реструктуризация',
            'remote': 'Дистанционное обслуживание',
            'mobile_app': 'Мобильное приложение',
            'hypothec': 'Ипотека',
            
            # Конкретные продукты (если есть в данных)
            'gazpromDEB': 'Дебетовая карта Газпромбанк',
            'DEB Supreme': 'Дебетовая карта Supreme',
            'Для dep школьников': 'Дебетовая карта для школьников',
            'gazpromDEBNEW': 'Новая дебетовая карта Газпромбанк',
            'Golden Deb': 'Золотая дебетовая карта',
            'Deb New Brilliant': 'Дебетовая карта Brilliant',
            'Золотая golden Deb': 'Золотая дебетовая карта',
            
            'карта "Мир"': 'Карта Мир',
            'Mir Supreme': 'Карта Мир Supreme', 
            'Для школьников': 'Карта для школьников',
            'карта "Мир2"': 'Карта Мир 2',
            'Mir Supreme2': 'Карта Мир Supreme 2',
            'Для школьников2': 'Карта для школьников 2',
            'Золотая карта': 'Золотая карта',
            'Платиновая карта': 'Платиновая карта',
            
            'Вклад лучшие проценты': 'Вклад "Лучшие проценты"',
            'Вклад Накопилка': 'Вклад "Накопилка"',
            'Накопление для школьников': 'Накопление для школьников',
            'Винстон черчиль': 'Вклад "Винстон Черчилль"',
            
            # Общие
            'general': 'Общие услуги'
        }
        
        return translation_map.get(english_name, english_name)

    def _determine_product_type_and_level(self, product_name: str, parent_product=None) -> tuple:
        """
        Определяет тип продукта и уровень в иерархии
        """
        from app.models.models import ProductType
        
        product_lower = product_name.lower()
        
        # Если есть родитель, то это продукт нижнего уровня
        if parent_product:
            return ProductType.PRODUCT, parent_product.level + 1
        
        # Определяем категории верхнего уровня
        top_level_categories = [
            'карты', 'кредиты', 'вклады и счета', 'обслуживание', 'другие услуги'
        ]
        
        if any(cat in product_lower for cat in top_level_categories):
            return ProductType.CATEGORY, 0
        else:
            # Если не категория верхнего уровня, но и нет родителя - создаем как продукт уровня 0
            return ProductType.PRODUCT, 0

    async def create_base_categories(self, session: AsyncSession):
        """
        Создает базовую структуру категорий продуктов
        """
        from app.repositories.repositories import ProductRepository
        from app.models.models import Product, ProductType, ClientType
        
        product_repo = ProductRepository()
        
        base_categories = [
            {
                'name': 'Карты',
                'type': ProductType.CATEGORY,
                'level': 0,
                'children': [
                    {'name': 'Дебетовые карты', 'type': ProductType.SUBCATEGORY, 'level': 1},
                    {'name': 'Кредитные карты', 'type': ProductType.SUBCATEGORY, 'level': 1}
                ]
            },
            {
                'name': 'Кредиты', 
                'type': ProductType.CATEGORY,
                'level': 0,
                'children': [
                    {'name': 'Ипотека', 'type': ProductType.SUBCATEGORY, 'level': 1},
                    {'name': 'Реструктуризация', 'type': ProductType.SUBCATEGORY, 'level': 1}
                ]
            },
            {
                'name': 'Вклады и счета',
                'type': ProductType.CATEGORY, 
                'level': 0
            },
            {
                'name': 'Обслуживание',
                'type': ProductType.CATEGORY,
                'level': 0,
                'children': [
                    {'name': 'Дистанционное обслуживание', 'type': ProductType.SUBCATEGORY, 'level': 1},
                    {'name': 'Мобильное приложение', 'type': ProductType.SUBCATEGORY, 'level': 1}
                ]
            },
            {
                'name': 'Другие услуги',
                'type': ProductType.CATEGORY,
                'level': 0
            }
        ]
        
        created_count = 0
        for category_data in base_categories:
            # Проверяем существует ли категория
            existing_category = await product_repo.get_by_name(session, category_data['name'])
            if not existing_category:
                # Создаем основную категорию
                category = Product(
                    name=category_data['name'],
                    type=category_data['type'],
                    client_type=ClientType.BOTH,
                    level=category_data['level']
                )
                category = await product_repo.save(session, category)
                created_count += 1
                logger.info(f"Created base category: {category_data['name']}")
                
                # Создаем дочерние категории если есть
                if 'children' in category_data:
                    for child_data in category_data['children']:
                        existing_child = await product_repo.get_by_name(session, child_data['name'])
                        if not existing_child:
                            child = Product(
                                name=child_data['name'],
                                type=child_data['type'],
                                client_type=ClientType.BOTH,
                                level=child_data['level'],
                                parent_id=category.id
                            )
                            await product_repo.save(session, child)
                            created_count += 1
                            logger.info(f"Created child category: {child_data['name']} under {category_data['name']}")
        
        if created_count > 0:
            await session.commit()
            logger.info(f"Created {created_count} base categories")
        
        return created_count

    async def _get_or_create_parent_product(self, session: AsyncSession, product_repo, product_name: str):
        """
        Получает или создает родительский продукт для указанного продукта
        """
        from app.models.models import Product, ProductType, ClientType
        
        product_lower = product_name.lower()
        
        # ОПРЕДЕЛЯЕМ РОДИТЕЛЯ ДЛЯ КАЖДОГО ТИПА ПРОДУКТА
        parent_mapping = {
            # Карты и связанные продукты
            'дебетовые карты': ('Карты', ProductType.SUBCATEGORY),
            'кредитные карты': ('Карты', ProductType.SUBCATEGORY),
            
            # Кредиты и связанные продукты  
            'ипотека': ('Кредиты', ProductType.SUBCATEGORY),
            'реструктуризация': ('Кредиты', ProductType.SUBCATEGORY),
            
            # Вклады (без родителя - верхний уровень)
            'вклады и счета': (None, ProductType.CATEGORY),
            
            # Обслуживание и связанные продукты
            'дистанционное обслуживание': ('Обслуживание', ProductType.SUBCATEGORY),
            'мобильное приложение': ('Обслуживание', ProductType.SUBCATEGORY),
            'обслуживание': (None, ProductType.CATEGORY),
            
            # Другие (без родителя)
            'другие услуги': (None, ProductType.CATEGORY),
            'кредиты': (None, ProductType.CATEGORY),
        }
        
        # Ищем подходящего родителя
        parent_name = None
        parent_type = ProductType.CATEGORY
        
        for key, (p_name, p_type) in parent_mapping.items():
            if key in product_lower:
                parent_name = p_name
                parent_type = p_type
                break
        
        # Если родитель не найден, возвращаем None
        if not parent_name:
            return None
        
        # Получаем или создаем родителя
        parent_product = await product_repo.get_by_name(session, parent_name)
        
        if not parent_product:
            parent_product = Product(
                name=parent_name,
                type=parent_type,
                client_type=ClientType.BOTH,
                level=0,  # Родители всегда уровень 0
                parent_id=None
            )
            parent_product = await product_repo.save(session, parent_product)
            logger.info(f"Created parent product: {parent_name}")
        
        return parent_product

    def _parse_source_from_url(self, url: str) -> str:
        """
        Определяет источник отзыва из URL
        """
        if not url:
            return "unknown"
        
        url_lower = url.lower()
        
        if 'banki' in url_lower:
            return 'Banki.ru'
        elif 'sravni' in url_lower:
            return 'Sravni.ru'
        elif 'imported_from_jsonl' in url_lower:
            return 'JSONL Import'
        elif 'manual' in url_lower:
            return 'Manual Input'
        else:
            return 'Other'
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
            return "neutral"  # fallback

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