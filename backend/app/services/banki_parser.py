import time
import json
import gc
import psutil
import os
from datetime import datetime, timedelta
from random import uniform
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from typing import List, Dict, Optional

from app.services.parser_config import ParserConfig

class BankiRuParser:
    def __init__(self, config: ParserConfig):
        self.config = config
        self.base_url = 'https://www.banki.ru/services/responses/bank/'
        
    def parse_date_string(self, date_str: str) -> Optional[datetime]:
        """Преобразует строку даты в datetime объект"""
        try:
            return datetime.strptime(date_str, '%d.%m.%Y %H:%M')
        except ValueError:
            return None

    def is_date_in_range(self, date_str: str) -> bool:
        """Проверяет, находится ли дата в указанном диапазоне"""
        if not self.config.start_date and not self.config.end_date:
            return True
            
        review_date = self.parse_date_string(date_str)
        if not review_date:
            return False
            
        if self.config.start_date:
            start_date = datetime.strptime(self.config.start_date, '%Y-%m-%d')
            if review_date < start_date:
                return False
                
        if self.config.end_date:
            end_date = datetime.strptime(self.config.end_date, '%Y-%m-%d') + timedelta(days=1)
            if review_date >= end_date:
                return False
                
        return True

    def try_to_surf(self, context, url, wait_class):
        """Функция для навигации по страницам"""
        from app.core.try_to_surf import try_to_surf as original_try_to_surf
        return original_try_to_surf(context, url, wait_class)

    def get_reviews_page(self, context, bank_slug: str, product: str, page_num: int) -> List[Dict]:
        """Получает отзывы с одной страницы"""
        url = f'{self.base_url}{bank_slug}/product/{product}/?page={page_num}&type=all&bank={bank_slug}'
        
        try:
            html_content = self.try_to_surf(context, url, 'Panel__sc-1g68tnu-1')
            soup = BeautifulSoup(html_content, 'html.parser')

            reviews = []
            main_div = soup.find('div', class_='Responsesstyled__StyledList-sc-150koqm-5')
            if not main_div:
                return reviews

            all_resp = main_div.find_all('div', {'data-test': 'responses__response'})
            
            for x in all_resp:
                bank_name = x.get('data-test-bank', '')
                
                title_link = x.find('h3', class_='TextResponsive__sc-hroye5-0')
                review_theme = title_link.get_text(strip=True) if title_link else ''
                # Обрезаем тему отзыва до 500 символов
                if len(review_theme) > 500:
                    review_theme = review_theme[:497] + "..."

                grade_div = x.find('div', class_='Grade__sc-m0t12o-0')
                rating = grade_div.get_text(strip=True) if grade_div else 'Без оценки'

                status_span = x.find('span', class_='GradeAndStatusstyled__StyledText-sc-11h7ddv-0')
                verification_status = status_span.get_text(strip=True) if status_span else ''

                review_text_div = x.find('div', class_='Responsesstyled__StyledItemText-sc-150koqm-3')
                if review_text_div:
                    review_link = review_text_div.find('a')
                    review_text = review_link.get_text(strip=True) if review_link else ''
                else:
                    review_text = ''

                date_span = x.find('span', class_='Responsesstyled__StyledItemSmallText-sc-150koqm-4')
                review_date = date_span.get_text(strip=True) if date_span else ''

                if review_date and not self.is_date_in_range(review_date):
                    continue

                review_data = {
                    'bank_name': bank_name,
                    'bank_slug': self.config.bank_slug,
                    'product_name': product,
                    'review_theme': review_theme,
                    'rating': rating,
                    'verification_status': verification_status,
                    'review_text': review_text,
                    'review_date': review_date,
                    'review_timestamp': self.parse_date_string(review_date),
                    'source_url': url
                }
                reviews.append(review_data)

            return reviews
            
        except Exception as e:
            print(f"Error parsing page {page_num} for {product}: {e}")
            return []
        finally:
            if 'soup' in locals():
                soup.decompose()
                del soup
            gc.collect()

    def parse_bank_products(self) -> Dict[str, List[Dict]]:
        """Основная функция парсинга для всех продуктов банка"""
        results = {}
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            
            MEMORY_THRESHOLD = 4 * 1024 * 1024 * 1024  # 4 ГБ
            pages_processed = 0

            for product in self.config.products:
                print(f'Парсинг продукта: {product} для банка {self.config.bank_slug}')
                product_reviews = []
                page_num = 1
                consecutive_empty_pages = 0
                max_consecutive_empty = 3

                while page_num <= self.config.max_pages and consecutive_empty_pages < max_consecutive_empty:
                    print(f'Страница {page_num} для продукта {product}')
                    
                    reviews = self.get_reviews_page(context, self.config.bank_slug, product, page_num)
                    
                    if not reviews:
                        consecutive_empty_pages += 1
                        print(f'Пустая страница {page_num}, счетчик: {consecutive_empty_pages}')
                    else:
                        consecutive_empty_pages = 0
                        product_reviews.extend(reviews)
                        print(f'Найдено {len(reviews)} отзывов на странице {page_num}')

                    process = psutil.Process(os.getpid())
                    memory_usage = process.memory_info().rss
                    if memory_usage > MEMORY_THRESHOLD or pages_processed >= 1000:
                        print(f"Перезапуск браузера из-за потребления памяти: {memory_usage / 1024 / 1024:.2f} MB")
                        context.close()
                        browser.close()
                        browser = p.chromium.launch(headless=True)
                        context = browser.new_context()
                        pages_processed = 0
                        gc.collect()

                    page_num += 1
                    pages_processed += 1
                    
                    time.sleep(self.config.delay_between_requests)

                results[product] = product_reviews
                print(f'Завершен парсинг продукта {product}: {len(product_reviews)} отзывов')

            context.close()
            browser.close()

        return results