# [file name]: jsonl_loader.py
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
        Преобразует данные из JSONL в формат для ReviewsForModel
        """
        # Парсим дату
        review_date = review_data.get('review_date', '')
        review_timestamp = self._parse_review_date(review_date)

        # Определяем product_name из topic или используем значение по умолчанию
        product_name = review_data.get('topic', 'general')
        if not product_name or product_name == 'null':
            product_name = 'general'

        # Создаем bank_slug из bank_name если не указан
        bank_name = review_data.get('bank_name', '')
        bank_slug = review_data.get('bank_slug') or self._create_bank_slug(bank_name)

        return {
            'bank_name': bank_name,
            'bank_slug': bank_slug,
            'product_name': product_name,
            'review_theme': review_data.get('review_theme', '')[:500],  # Обрезаем до 500 символов
            'rating': str(review_data.get('rating', 'Без оценки')),
            'verification_status': review_data.get('verification_status', ''),
            'review_text': review_data.get('review_text', ''),
            'review_date': review_date,
            'review_timestamp': review_timestamp,
            'source_url': review_data.get('source', 'unknown'),
            'parsed_at': datetime.utcnow(),
            'processed': False,
            'additional_data': {
                'source': source,
                'original_topic': review_data.get('topic'),
                'import_timestamp': datetime.utcnow().isoformat(),
                'original_source': review_data.get('source', 'unknown')
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