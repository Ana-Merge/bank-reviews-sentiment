import time
import json
import requests
import uuid
from typing import List, Dict, Optional
from datetime import datetime, timezone
from random import randint

class SravniRuParser:
    def __init__(self, config: dict):
        self.config = config
        self.base_url = "https://www.sravni.ru/proxy-reviews/reviews"
        
    def parse_date_string(self, date_str: str) -> Optional[datetime]:
        """Преобразует строку даты в datetime объект"""
        try:
            # Формат "2025-09-19T12:10:42.081749Z"
            # Преобразуем в наивный datetime (без временной зоны)
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.replace(tzinfo=None)  # Убираем временную зону
        except ValueError:
            return None

    def is_date_in_range(self, date_str: str) -> bool:
        """Проверяет, находится ли дата в указанном диапазоне"""
        if not self.config.get('start_date') and not self.config.get('end_date'):
            return True
            
        review_date = self.parse_date_string(date_str)
        if not review_date:
            return False
            
        start_date = self.config.get('start_date')
        end_date = self.config.get('end_date')
            
        if start_date:
            start_date_parsed = datetime.strptime(start_date, '%Y-%m-%d')
            if review_date.date() < start_date_parsed.date():
                return False
                
        if end_date:
            end_date_parsed = datetime.strptime(end_date, '%Y-%m-%d')
            if review_date.date() > end_date_parsed.date():
                return False
                
        return True

    def fetch_reviews(self, bank_slug: str, page_size: int = 100) -> List[Dict]:
        """
        Получение всех отзывов с API sravni.ru для конкретного банка.
        """
        all_reviews = []
        page_index = 0
        total = None
        
        # Заголовки для запросов
        headers = {
            "Host": "www.sravni.ru",
            "X-Request-Id": str(uuid.uuid4()),
            "Baggage": "sentry-environment=production,sentry-release=dc82ee86,sentry-public_key=eca1eed372c03cdff0768b2d1069488d,sentry-trace_id=54577e1cb1e74f07a828887a3b6f00fa,sentry-transaction=%2Flist,sentry-sampled=true,sentry-sample_rand=0.9727468654279157,sentry-sample_rate=1",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://www.sravni.ru/"
        }
        
        # Получаем review_object_id для банка из маппинга
        review_object_id = self.get_review_object_id(bank_slug)
        if not review_object_id:
            print(f"Не найден review_object_id для банка {bank_slug}")
            return []
        
        while page_index < self.config.get('max_pages', 100):
            params = {
                "NewIds": "true",
                "OrderBy": "byDate",
                "PageIndex": str(page_index),
                "PageSize": str(page_size),
                "ReviewObjectType": "banks",
                "ReviewObjectId": review_object_id,
                "fingerPrint": "b43cf2076ffe330eadb7902007ae7038"
            }
            
            try:
                response = requests.get(self.base_url, params=params, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    print(f"Ошибка при получении страницы {page_index} для {bank_slug}: {response.status_code}")
                    break
                
                data = response.json()
                items = data.get("items", [])
                
                # Фильтруем отзывы по дате
                filtered_items = []
                for item in items:
                    date_str = item.get("date", "")
                    if not date_str:
                        # Если даты нет, пропускаем отзыв
                        continue
                        
                    if self.is_date_in_range(date_str):
                        # Преобразуем данные в нужный формат
                        transformed_review = self.transform_review_data(item, bank_slug)
                        filtered_items.append(transformed_review)
                    else:
                        # Если дата вне диапазона, останавливаем парсинг (предполагаем, что отзывы идут от новых к старым)
                        if page_index > 0:  # Не останавливаемся на первой странице
                            print(f"Дата {date_str} вне диапазона, останавливаем парсинг для {bank_slug}")
                            return all_reviews
                
                all_reviews.extend(filtered_items)
                
                if total is None:
                    total = data.get("total", 0)
                
                print(f"Банк {bank_slug}: получена страница {page_index} ({len(filtered_items)} отзывов), всего: {len(all_reviews)} / {total}")
                
                if len(items) < page_size or (total and len(all_reviews) >= total):
                    break
                
                page_index += 1
                time.sleep(self.config.get('delay_between_requests', 1.0))
                
            except Exception as e:
                print(f"Ошибка при парсинге страницы {page_index} для {bank_slug}: {e}")
                break
        
        return all_reviews

    def get_review_object_id(self, bank_slug: str) -> Optional[str]:
        """
        Получает review_object_id для банка по его slug.
        Здесь нужно реализовать маппинг slug -> review_object_id
        """
        # Пример маппинга - нужно дополнить актуальными данными
        bank_mapping = {
        # Английские slug
            "gazprombank": "5bb4f768245bc22a520a6115",
            "sberbank": "5bb4f768245bc22a520a6116",
            "vtb": "5bb4f768245bc22a520a6117",
            "alfabank": "5bb4f768245bc22a520a6118",
            "tinkoff": "5bb4f768245bc22a520a6119",
            
            # Русские названия
            "газпромбанк": "5bb4f768245bc22a520a6115",
            "сбербанк": "5bb4f768245bc22a520a6116",
            "втб": "5bb4f768245bc22a520a6117",
            "альфабанк": "5bb4f768245bc22a520a6118",
            "тинькофф": "5bb4f768245bc22a520a6119",
            "россия": "5bb4f768245bc22a520a6120",
            "открытие": "5bb4f768245bc22a520a6121"
        }
        
        return bank_mapping.get(bank_slug.lower())

    def transform_review_data(self, item: Dict, bank_slug: str) -> Dict:
        """
        Преобразует данные отзыва из формата sravni.ru в наш формат.
        """
        # Преобразуем дату
        review_date = self.parse_date_string(item.get("date", ""))
        
        # Получаем рейтинг
        rating = item.get("rating")
        if rating is not None:
            rating_str = str(rating)
        else:
            rating_str = "Без оценки"
        
        # Получаем product_name из reviewTag
        product_name = item.get("reviewTag", "general")
        if not product_name or product_name == "null":
            product_name = "general"
        
        # Получаем bank_name из item или используем bank_slug
        bank_name = item.get("bank_name", bank_slug)
        if not bank_name:
            bank_name = bank_slug
        
        return {
            'bank_name': bank_name,
            'bank_slug': bank_slug,
            'product_name': product_name,
            'review_theme': item.get("title", ""),
            'rating': rating_str,
            'verification_status': item.get("ratingStatus", ""),
            'review_text': item.get("text", ""),
            'review_date': review_date.strftime("%d.%m.%Y %H:%M") if review_date else "",
            'review_timestamp': review_date,
            'source_url': f"https://www.sravni.ru/bank/{bank_slug}/reviews/",
            'additional_data': {
                'author_name': item.get("authorName"),
                'author_last_name': item.get("authorLastName"),
                'location': item.get("locationData", {}).get("name"),
                'comments_count': item.get("commentsCount", 0),
                'is_legal': item.get("isLegal", False),
                'problem_solved': item.get("problemSolved", False),
                'review_id': item.get("id"),
                'user_id': item.get("userId")
            }
        }

    def parse_banks(self) -> Dict[str, List[Dict]]:
        """Основная функция парсинга для всех указанных банков"""
        results = {}
        
        for bank_slug in self.config.get('bank_slugs', []):
            print(f'Парсинг банка: {bank_slug}')
            
            try:
                reviews = self.fetch_reviews(bank_slug)
                results[bank_slug] = reviews
                
                print(f'Завершен парсинг банка {bank_slug}: {len(reviews)} отзывов')
                
            except Exception as e:
                print(f"Ошибка при парсинге банка {bank_slug}: {e}")
                results[bank_slug] = []
            
            # Задержка между банками
            time.sleep(self.config.get('delay_between_requests', 1.0) * 2)

        return results