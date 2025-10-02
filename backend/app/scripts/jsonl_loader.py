import json
import logging
import os
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.repositories.repositories import ReviewsForModelRepository

logger = logging.getLogger(__name__)

class JSONLLoader:
    def __init__(self, reviews_for_model_repo: ReviewsForModelRepository):
        self._reviews_for_model_repo = reviews_for_model_repo

    async def load_from_jsonl_file(
        self, 
        session: AsyncSession, 
        file_path: str,
        source: str = "jsonl_import"
    ) -> Dict[str, Any]:
        """
        Загружает данные из JSONL файла в таблицу reviews_for_model
        """
        try:
            reviews_data = []
            processed_count = 0
            error_count = 0

            # Проверяем существование файла
            if not os.path.exists(file_path):
                return {
                    "status": "error",
                    "message": f"File {file_path} does not exist"
                }

            with open(file_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    try:
                        line = line.strip()
                        if not line:
                            continue

                        review_data = json.loads(line)
                        transformed_review = self._transform_review_data(review_data, source)
                        
                        # Проверяем обязательные поля
                        if not transformed_review.get('review_text'):
                            logger.warning(f"Skipping line {line_num}: missing review_text")
                            continue
                            
                        reviews_data.append(transformed_review)
                        processed_count += 1

                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing JSON on line {line_num}: {e}")
                        error_count += 1
                    except Exception as e:
                        logger.error(f"Error processing line {line_num}: {e}")
                        error_count += 1

            # Сохраняем данные в базу
            if reviews_data:
                saved_count = await self._reviews_for_model_repo.bulk_create_from_jsonl(
                    session, reviews_data
                )
                await session.commit()
                
                logger.info(f"Successfully loaded {saved_count} reviews from {file_path}")
            else:
                saved_count = 0
                logger.warning(f"No valid reviews found in {file_path}")

            return {
                "status": "success",
                "file_path": file_path,
                "lines_processed": processed_count,
                "reviews_saved": saved_count,
                "errors": error_count,
                "message": f"Successfully loaded {saved_count} reviews from {file_path}"
            }

        except Exception as e:
            logger.error(f"Error loading JSONL file {file_path}: {str(e)}")
            return {
                "status": "error",
                "file_path": file_path,
                "message": f"Failed to load JSONL file: {str(e)}"
            }

    def _transform_review_data(self, review_data: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        Преобразует данные из нового формата JSONL в формат для ReviewsForModel
        """
        try:
            # Новый формат: {"data": {...}, "predictions": {...}}
            data = review_data.get('data', {})
            predictions = review_data.get('predictions', {})
            
            # Получаем массивы из predictions - исправляем ключ с двоеточием
            topics = predictions.get('topics', [])
            sentiments = predictions.get('sentiments', [])
            sources = predictions.get('sources', [])
            # ИСПРАВЛЕНИЕ: используем правильный ключ с двоеточием
            review_dates = predictions.get('review_dates:', []) or predictions.get('review_dates', [])
            ratings = predictions.get('ratings', [])
            
            # Используем первый элемент массивов для основной записи
            primary_topic = topics[0] if topics else 'general'
            primary_sentiment = sentiments[0] if sentiments else 'нейтральная'
            primary_source = sources[0] if sources else 'unknown'
            primary_date = review_dates[0] if review_dates else ''
            primary_rating = ratings[0] if ratings else 'Без оценки'
            
            # ИСПРАВЛЕНИЕ: определяем source_url из данных
            source_url = data.get('source_url', 'unknown')
            # Если source_url неизвестен, но есть sources в predictions, используем первый source
            if source_url == 'unknown' and sources:
                source_url = sources[0]
            
            # Определяем тип источника для основного поля source_url
            if 'banki' in source_url.lower():
                source_url_final = 'banki'
            elif 'sravni' in source_url.lower():
                source_url_final = 'sravni'
            else:
                source_url_final = source_url
            
            # Парсим дату
            review_timestamp = self._parse_review_date(primary_date)
            
            # Создаем bank_slug из bank_name если не указан
            bank_name = data.get('bank_name', '')
            bank_slug = data.get('bank_slug') or self._create_bank_slug(bank_name)
            
            return {
                'bank_name': bank_name,
                'bank_slug': bank_slug,
                'product_name': primary_topic,  # Основной продукт для фильтрации
                'review_theme': data.get('review_theme', ''),
                'rating': str(primary_rating),
                'verification_status': data.get('verification_status', ''),
                'review_text': data.get('text', ''),
                'review_date': primary_date,  # Сохраняем дату в основное поле
                'review_timestamp': review_timestamp,
                'source_url': source_url_final,  # ИСПРАВЛЕНИЕ: используем определенный источник
                'parsed_at': datetime.utcnow(),
                'processed': False,
                'additional_data': {
                    'source': source,
                    'original_data': data,
                    'predictions': predictions,
                    'all_topics': topics,  # Сохраняем все топики
                    'all_sentiments': sentiments,  # Сохраняем все sentiments
                    'all_sources': sources,  # Сохраняем все sources
                    'all_review_dates': review_dates,  # Сохраняем все даты
                    'all_ratings': ratings,  # Сохраняем все рейтинги
                    'import_timestamp': datetime.utcnow().isoformat(),
                    'original_source_url': source_url  # Сохраняем оригинальный URL
                }
            }
        except Exception as e:
            logger.error(f"Error transforming review data: {str(e)}")
            # Fallback для старого формата
            return self._transform_old_format(review_data, source)

    def _determine_source_type(self, source_url: str) -> str:
        """
        Определяет тип источника по URL
        """
        if not source_url or source_url == 'unknown':
            return 'unknown'
        
        source_lower = source_url.lower()
        
        if 'banki' in source_lower:
            return 'banki'
        elif 'sravni' in source_lower:
            return 'sravni'
        else:
            return source_url


    def _transform_old_format(self, review_data: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        Обработка старого формата JSONL для обратной совместимости
        """
        review_date = review_data.get('review_date', '')
        review_timestamp = self._parse_review_date(review_date)
        
        english_product_name = review_data.get('topic', 'general')
        bank_name = review_data.get('bank_name', '')
        bank_slug = review_data.get('bank_slug') or self._create_bank_slug(bank_name)
        
        # ИСПРАВЛЕНИЕ: определяем source_url из старых данных
        original_source = review_data.get('source', 'unknown')
        if 'banki' in original_source.lower():
            source_url_final = 'banki'
        elif 'sravni' in original_source.lower():
            source_url_final = 'sravni'
        else:
            source_url_final = original_source
        
        return {
            'bank_name': bank_name,
            'bank_slug': bank_slug,
            'product_name': english_product_name,
            'review_theme': review_data.get('review_theme', '')[:5000],
            'rating': str(review_data.get('rating', 'Без оценки')),
            'verification_status': review_data.get('verification_status', ''),
            'review_text': review_data.get('review_text', ''),
            'review_date': review_date,
            'review_timestamp': review_timestamp,
            'source_url': source_url_final,  # ИСПРАВЛЕНИЕ: используем определенный источник
            'parsed_at': datetime.utcnow(),
            'processed': False,
            'additional_data': {
                'source': source,
                'original_topic': review_data.get('topic'),
                'english_product_name': english_product_name,
                'import_timestamp': datetime.utcnow().isoformat(),
                'original_source': original_source
            }
        }

    def _parse_review_date(self, date_str: str) -> datetime:
        """
        Парсит строку даты в datetime объект
        """
        try:
            date_formats = [
                '%d.%m.%Y %H:%M',  # 27.05.2025 10:14
                '%d.%m.%Y',        # 27.05.2025
                '%Y-%m-%d %H:%M:%S', # 2025-05-27 10:14:00
                '%Y-%m-%d',        # 2025-05-27
            ]
            
            for date_format in date_formats:
                try:
                    return datetime.strptime(date_str, date_format)
                except ValueError:
                    continue
            
            # Если ни один формат не подошел, возвращаем текущую дату
            logger.warning(f"Could not parse date: {date_str}, using current time")
            return datetime.utcnow()
            
        except Exception as e:
            logger.warning(f"Error parsing date {date_str}: {e}, using current time")
            return datetime.utcnow()

    def _create_bank_slug(self, bank_name: str) -> str:
        """
        Создает slug из названия банка
        """
        if not bank_name:
            return 'unknown'
        
        # Простая транслитерация и создание slug
        slug = bank_name.lower()
        translit_map = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
            'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
            'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
            'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
            'ы': 'y', 'э': 'e', 'ю': 'yu', 'я': 'ya'
        }
        
        result = []
        for char in slug:
            if char in translit_map:
                result.append(translit_map[char])
            elif char.isalnum():
                result.append(char)
            elif char in ' -_':
                result.append('_')
        
        slug_result = ''.join(result)
        return slug_result if slug_result else 'unknown'