# import time
# import json
# from random import uniform
# from bs4 import BeautifulSoup
# from playwright.sync_api import sync_playwright
# from try_to_surf import try_to_surf
# import re
# import os
# from math import floor


# BASE_URL = 'https://www.banki.ru/services/responses/list/product/'
# PARAMETR = '?type=all'
# # LINKS = ['debitcards', 'creditcards', 'hypothec', 'autocredits', 'credits', 'restructing', 'deposits', 'transfers', 'remote', 'other', 'mobile_app', 'individual']

# LINKS = ['debitcards']

# # Константа для количества записей перед сохранением
# SAVE_BATCH_SIZE = 200

# def get_data_good(context, link, page):
#     BASE_URL = 'https://www.banki.ru/services/responses/list/product/'
#     PARAMETR = '?type=all'
#     url = f'{BASE_URL}{link}/{PARAMETR}&page={page}'
#     html_content = try_to_surf(context, url, 'Panel__sc-1g68tnu-1')
#     soup = BeautifulSoup(html_content, 'html.parser')
    
#     res = {
#         'link': url,
#         'page': page
#     }
    
#     script_tag = soup.find('script', type='application/ld+json')
#     if not script_tag:
#         return {}
    
#     try:
#         json_data = json.loads(script_tag.string)
#         reviews = json_data.get('review', [])
#         if not reviews:
#             return {}
        
#         for idx, review in enumerate(reviews):
#             res[str(idx)] = {}
#             res[str(idx)]['bank_name'] = json_data.get('name', '')  # 'Газпромбанк'
#             res[str(idx)]['review_theme'] = review.get('name', '')
#             rating = review.get('reviewRating', {}).get('ratingValue', 'Без оценки')
#             res[str(idx)]['rating'] = rating
#             res[str(idx)]['verification_status'] = 'Подтвержден'  # Assuming default, as not present in JSON
#             res[str(idx)]['review_text'] = review.get('description', '')
#             res[str(idx)]['review_date'] = review.get('datePublished', '')
        
#         return res
#     except json.JSONDecodeError:
#         return {}
#     except Exception:
#         return {}

# def save_batch_to_jsonl(data_batch, output_file):
#     """Сохраняет батч данных в JSONL файл"""
#     with open(output_file, 'a', encoding='utf-8') as f:
#         for item in data_batch:
#             f.write(json.dumps(item, ensure_ascii=False) + '\n')

# def load_existing_data(output_file):
#     """Загружает существующие данные из JSONL файла"""
#     data = []
#     if os.path.exists(output_file):
#         try:
#             with open(output_file, 'r', encoding='utf-8') as f:
#                 for line in f:
#                     if line.strip():
#                         data.append(json.loads(line.strip()))
#         except (json.JSONDecodeError, FileNotFoundError):
#             pass
#     return data

# def count_total_reviews(data):
#     """Подсчитывает общее количество отзывов во всех страницах"""
#     total = 0
#     for page_data in data:
#         # Исключаем служебные поля 'link' и 'page'
#         for key in page_data:
#             if key not in ['link', 'page']:
#                 total += 1
#     return total

# def main():
#     output_dir = "jsonl_data"
#     os.makedirs(output_dir, exist_ok=True) 
    
#     with sync_playwright() as p:
#         browser = p.chromium.launch(headless=True)
#         context = browser.new_context()

#         for category_name in LINKS:
#             print(f'Обрабатывается категория: {category_name}')
            
#             output_file = f'{output_dir}/{category_name}.jsonl'
            
#             # Загружаем существующие данные для определения последней страницы
#             existing_data = load_existing_data(output_file)
#             total_reviews = count_total_reviews(existing_data)
            
#             print(f'Уже собрано отзывов: {total_reviews}')
            
#             # Определяем с какой страницы начинать
#             if existing_data:
#                 last_page = max([item.get('page', 0) for item in existing_data])
#                 start_page = last_page + 1
#                 print(f'Продолжаем со страницы: {start_page}')
#             else:
#                 start_page = 1
            
#             current_batch = []
#             page_num = start_page
            
#             while True:
#                 good_data = None
#                 for i in range(5):  # Пытаемся 5 раз
#                     try:
#                         good_data = get_data_good(context, category_name, page_num)
#                         if good_data and len(good_data) > 2:  # Проверяем, что есть отзывы (больше 2 полей: link, page + отзывы)
#                             break
#                     except KeyboardInterrupt:
#                         # Сохраняем текущий батч перед выходом
#                         if current_batch:
#                             save_batch_to_jsonl(current_batch, output_file)
#                             print(f'Сохранено {len(current_batch)} страниц перед выходом')
#                         browser.close()
#                         return  # Выходим при прерывании
#                     except Exception as e:
#                         print(f'Ошибка при получении страницы {page_num}: {e}')
#                         time.sleep(uniform(2, 5))  # Увеличиваем паузу при ошибке
                
#                 # Если ничего не получили после 5 попыток, останавливаемся
#                 if not good_data or len(good_data) <= 2:
#                     print(f'Не удалось получить данные со страницы {page_num}. Завершение категории.')
#                     break
                
#                 current_batch.append(good_data)
#                 current_reviews_count = count_total_reviews([good_data])
#                 total_reviews += current_reviews_count
                
#                 print(f'Страница {page_num}: получено {current_reviews_count} отзывов. Всего: {total_reviews}')
                
#                 # Сохраняем батч при достижении лимита
#                 if len(current_batch) >= SAVE_BATCH_SIZE:
#                     save_batch_to_jsonl(current_batch, output_file)
#                     print(f'Сохранено {len(current_batch)} страниц в файл')
#                     current_batch = []  # Очищаем батч после сохранения
                
#                 # Пауза между запросами
#                 time.sleep(uniform(1, 3))
#                 page_num += 1
            
#             # Сохраняем оставшиеся данные после завершения категории
#             if current_batch:
#                 save_batch_to_jsonl(current_batch, output_file)
#                 print(f'Финальное сохранение: {len(current_batch)} страниц для категории {category_name}')
            
#             print(f'Завершена обработка категории: {category_name}. Всего отзывов: {total_reviews}')

#         browser.close()

# def print_stats():
#     """Функция для вывода статистики по собранным данным"""
#     output_dir = "jsonl_data"
#     if not os.path.exists(output_dir):
#         print("Директория с данными не найдена")
#         return
    
#     for category_name in LINKS:
#         output_file = f'{output_dir}/{category_name}.jsonl'
#         if os.path.exists(output_file):
#             data = load_existing_data(output_file)
#             total_reviews = count_total_reviews(data)
#             print(f'{category_name}: {len(data)} страниц, {total_reviews} отзывов')
#         else:
#             print(f'{category_name}: файл не найден')

# if __name__ == '__main__':
#     main()
#     # После завершения можно посмотреть статистику
#     print("\n=== СТАТИСТИКА ===")
#     print_stats()

import time
import json
from random import uniform
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from try_to_surf import try_to_surf
import re
import os
from math import floor


BASE_URL = 'https://www.banki.ru/services/responses/bank/gazprombank/product/'
PARAMETR = '?is_countable=on'
LINKS = ['hypothec']


def get_data_good(context, link, page):
    BASE_URL = 'https://www.banki.ru/services/responses/bank/gazprombank/product/'
    PARAMETR = '?is_countable=on'
    url = f'{BASE_URL}{link}/{PARAMETR}&page={page}'
    html_content = try_to_surf(context, url, 'Panel__sc-1g68tnu-1')  # Assuming try_to_surf is defined elsewhere
    soup = BeautifulSoup(html_content, 'html.parser')
    
    res = {
        'link': url,
    }
    
    script_tag = soup.find('script', type='application/ld+json')
    if not script_tag:
        return {}
    
    try:
        json_data = json.loads(script_tag.string)
        reviews = json_data.get('review', [])
        if not reviews:
            return {}
        
        for idx, review in enumerate(reviews):
            res[str(idx)] = {}
            res[str(idx)]['bank_name'] = json_data.get('name', '')  # 'Газпромбанк'
            res[str(idx)]['review_theme'] = review.get('name', '')
            rating = review.get('reviewRating', {}).get('ratingValue', 'Без оценки')
            res[str(idx)]['rating'] = rating
            res[str(idx)]['verification_status'] = 'Подтвержден'  # Assuming default, as not present in JSON пизда
            res[str(idx)]['review_text'] = review.get('description', '')
            res[str(idx)]['review_date'] = review.get('datePublished', '')
        
        return res
    except json.JSONDecodeError:
        return {}
    except Exception:
        return {}

def main():
    output_dir = "jsons"
    os.makedirs(output_dir, exist_ok=True) 
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        for category_name in LINKS:
            print(f'Обрабатывается категория: {category_name}')
            
            output_file = f'jsons/{category_name}.json'

            # Инициализируем пустой JSON файл
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=4)
            
            page_num = 1
            while True:  # Заменил range(1, 100000) на while, чтобы избежать ненужных итераций
                good_data = None
                for i in range(5):  # Пытаемся 5 раз
                    try:
                        good_data = get_data_good(context, category_name, page_num)
                        if good_data:  # Проверяем, не пустой ли (изменено с != [] на if good_data, для ясности)
                            break
                    except KeyboardInterrupt:
                        browser.close()
                        return  # Выходим при прерывании
                    except Exception:
                        pass  # Игнорируем другие ошибки и пробуем снова
                
                if not good_data:  # Если ничего не получили, останавливаемся
                    break
                
                # Читаем текущий файл, добавляем новую страницу, перезаписываем
                with open(output_file, 'r', encoding='utf-8') as f:
                    current_data = json.load(f)
                
                current_data.append(good_data)
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(current_data, f, ensure_ascii=False, indent=4)
                
                # Небольшая пауза, чтобы не нагружать сервер (раскомментировано для стабильности)
                time.sleep(uniform(1, 3))
                
                page_num += 1

        browser.close()



if __name__ == '__main__':
    main()