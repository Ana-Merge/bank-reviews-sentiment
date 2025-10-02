import time
import json
import html
import re
from random import uniform
import requests
from bs4 import BeautifulSoup
import os

BASE_URL = 'https://www.banki.ru/services/responses/list/product/'
PARAMETR = '&is_countable=on'
LINKS = ['debitcards']

def preprocess_json_string(json_string):
    """Preprocess JSON string to remove problematic control characters."""
    # Replace \r\n with \n and remove other control characters
    json_string = json_string.replace('\r\n', '\n').replace('\r', '')
    # Remove control characters except \n and \t
    json_string = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', json_string)
    return json_string

def get_data_good(link, page):
    url = f'{BASE_URL}{link}/?page={page}{PARAMETR}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive'
    }
    
    print(f"Загружаем URL: {url}")
    
    for attempt in range(5):  # Пять попыток при ошибках
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Проверяем статус ответа (200 OK)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # # Сохраняем HTML для отладки
            # with open(f'page_{link}_{page}_debug.html', 'w', encoding='utf-8') as f:
            #     f.write(response.text)
            
            res = {'link': url}
            script_tag = soup.find('script', type='application/ld+json')
            
            if not script_tag or not script_tag.string:
                print(f"JSON-LD не найден или пустой на странице: {url}")
                return {}
            
            json_string = script_tag.string.strip()
            if not json_string:
                print(f"JSON-строка пустая на странице {url}")
                return {}
            
            # Предобработка JSON-строки
            json_string = preprocess_json_string(json_string)
            
            try:
                json_data = json.loads(json_string, strict=False)
                reviews = json_data.get('review', [])
                if not reviews:
                    print(f"Отзывы не найдены в JSON-LD: {url}")
                    return {}
                
                for idx, review in enumerate(reviews):
                    # Декодируем HTML-теги в reviewBody для читаемости
                    review_body = html.unescape(review.get('reviewBody', ''))
                    
                    res[str(idx)] = {
                        'bank_name': review.get('itemReviewed', {}).get('name', ''),
                        'review_theme': review.get('name', ''),
                        'rating': review.get('reviewRating', {}).get('ratingValue', 'Без оценки'),
                        'verification_status': 'Подтвержден',
                        'review_text': review_body.replace('<p>', '').replace('</p>', '').replace('<br>', ''),
                        'review_date': review.get('datePublished', ''),
                        'address': review.get('itemReviewed', {}).get('address', {}).get('streetAddress', ''),
                        'telephone': review.get('itemReviewed', {}).get('telephone', '')
                    }
                
                print(f"Успешно обработано {len(reviews)} отзывов на странице {page} для {link}")
                return res
            
            except json.JSONDecodeError as e:
                print(f"Ошибка декодирования JSON на странице {url}: {e}")
                print(f"Длина JSON-строки: {len(json_string)}")
                print(f"Начало JSON-строки: {json_string[:200]}")
                # Сохраняем сырую строку для отладки
                with open(f'json_error_{link}_page_{page}.txt', 'w', encoding='utf-8') as f:
                    f.write(json_string or "Пустой JSON")
                res['raw_data'] = json_string
                return res
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print(f"Ошибка 429 (Too Many Requests) на странице {url}. Ожидание перед повтором...")
                time.sleep(uniform(5, 10) * (attempt + 1))
            elif response.status_code in [403, 503, 502, 500]:
                print(f"Ошибка {response.status_code} на странице {url}: {e}. Возможно, защита от ботов.")
                time.sleep(uniform(5, 10) * (attempt + 1))
            else:
                print(f"HTTP-ошибка на странице {url}: {e}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса на странице {url}: {e}")
            time.sleep(uniform(5, 10) * (attempt + 1))
        except Exception as e:
            print(f"Неизвестная ошибка при обработке данных на странице {url}: {e}")
            return {}
    
    print(f"Не удалось обработать страницу {url} после 5 попыток.")
    return {}

def main():
    output_dir = "jsons"
    os.makedirs(output_dir, exist_ok=True)
    
    for category_name in LINKS:
        print(f'Обрабатывается категория: {category_name}')
        output_file = f'jsons/{category_name}.json'
        
        # Инициализируем пустой список в файле
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=4)
        
        page_num = 2827
        while True:
            try:
                good_data = get_data_good(category_name, page_num)
                if good_data:
                    with open(output_file, 'r', encoding='utf-8') as f:
                        current_data = json.load(f, strict=False)
                    current_data.append(good_data)
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(current_data, f, ensure_ascii=False, indent=4)
                    print(f"Страница {page_num} для {category_name} успешно сохранена")
                    page_num += 1
                    time.sleep(uniform(3, 7))  # Задержка между страницами
                else:
                    print(f"Нет данных на странице {page_num} для {category_name}. Завершение.")
                    break
                
            except KeyboardInterrupt:
                print("Парсинг прерван пользователем")
                return
            except Exception as e:
                print(f"Ошибка на странице {page_num} для {category_name}: {e}")
                break

if __name__ == '__main__':
    main()