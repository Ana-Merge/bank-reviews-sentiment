import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from typing import Dict

# Функция для извлечения ID из URL логотипа
def extract_id_from_logo_url(logo_url: str) -> str:
    """
    Извлекает ID банка из URL логотипа.
    URL вида: https://.../square/5bb4f767245bc22a520a5fd5.svg?v=0.a4
    Возвращает: '5bb4f767245bc22a520a5fd5'
    """
    # Убираем параметры запроса (?v=...)
    parsed_url = urlparse(logo_url)
    path = parsed_url.path
    # Берем последнюю часть пути перед расширением
    filename = path.split('/')[-1]
    id_part = filename.split('.')[0]  # Убираем .svg или другие расширения
    return id_part

def fetch_bank_mapping() -> Dict[str, str]:
    """
    Получает страницу с рейтингом банков и извлекает маппинг: название банка -> ID.
    
    :return: Словарь {bank_name: review_object_id}
    """
    url = "https://www.sravni.ru/banki/rating/"
    headers = {
        "Host": "www.sravni.ru",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.sravni.ru/"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Ошибка при получении страницы: {response.status_code} - {response.text}")
    
    # Парсим HTML для поиска скрипта с данными
    soup = BeautifulSoup(response.text, 'html.parser')
    script_tag = soup.find('script', id='__NEXT_DATA__')
    
    if not script_tag:
        raise Exception("Не найден скрипт с данными __NEXT_DATA__")
    
    # Извлекаем JSON из скрипта
    data = json.loads(script_tag.string)
    
    # Путь к списку рейтингов: props.initialReduxState.ratings.list
    ratings_list = data.get('props', {}).get('initialReduxState', {}).get('ratings', {}).get('list', {})
    if not ratings_list:
        raise Exception("Не найден список рейтингов в данных")
        
    
    bank_mapping = {}
    for item in ratings_list:
        bank_name = item.get('organizationName')
        logo_url = item.get('organizationLogo')
        if bank_name and logo_url:
            bank_id = extract_id_from_logo_url(logo_url)
            bank_mapping[bank_id] = bank_name
    
    return bank_mapping

def save_mapping_to_json(mapping: Dict[str, str], filename: str = "sravniru/bank_mapping.json"):
    """
    Сохраняет маппинг в JSON-файл.
    
    :param mapping: Словарь с маппингом
    :param filename: Имя файла для сохранения
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=4)
    print(f"Сохранен маппинг {len(mapping)} банков в {filename}")

if __name__ == "__main__":
    print("Сбор маппинга банков...")
    mapping = fetch_bank_mapping()
    save_mapping_to_json(mapping)
    # Пример вывода первых нескольких
    print("Пример маппинга:")
    for name, id_ in list(mapping.items())[:5]:
        print(f"- {name}: {id_}")