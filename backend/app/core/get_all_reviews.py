import requests
import json
import time
from typing import Dict, List, Optional
import uuid
from random import randint
import os

# Маппинг ReviewObjectId на названия банков
BANK_MAPPING = {
}

def fetch_reviews(review_object_id: Optional[str] = None, page_size: int = 100) -> List[Dict]:
    """
    Получение всех отзывов с API sravni.ru.
    
    :param review_object_id: ID конкретного банка или None для всех банков
    :param page_size: Количество отзывов на страницу (макс. около 100-500, тестируйте)
    :return: Список всех элементов отзывов
    """
    base_url = "https://www.sravni.ru/proxy-reviews/reviews"
    all_reviews = []
    page_index = 0
    total = None
    
    # Заголовки из примера - могут потребоваться корректировки для избежания блокировки
    headers = {
        "Host": "www.sravni.ru",
        "X-Request-Id": str(uuid.uuid4()),  # Генерируем новый UUID для каждого запроса
        "Baggage": "sentry-environment=production,sentry-release=dc82ee86,sentry-public_key=eca1eed372c03cdff0768b2d1069488d,sentry-trace_id=54577e1cb1e74f07a828887a3b6f00fa,sentry-transaction=%2Flist,sentry-sampled=true,sentry-sample_rand=0.9727468654279157,sentry-sample_rate=1",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.sravni.ru/"
    }
    
    while True:
        params = {
            "NewIds": "true",
            "OrderBy": "byDate",
            "PageIndex": str(page_index),
            "PageSize": str(page_size),
            "ReviewObjectType": "banks",
            "fingerPrint": "b43cf2076ffe330eadb7902007ae7038"  # может потребовать обновления
        }
        if review_object_id:
            params["ReviewObjectId"] = review_object_id
        
        response = requests.get(base_url, params=params, headers=headers)
        
        if response.status_code != 200:
            print(f"Ошибка при получении страницы {page_index}: {response.status_code} - {response.text}")
            break
        
        data = response.json()
        
        items = data.get("items", [])
        # Добавляем bank_name в каждый отзыв
        for item in items:
            obj_id = item.get("reviewObjectId")
            item["bank_name"] = BANK_MAPPING.get(obj_id, None)  # None, если ID отсутствует в маппинге
        
        all_reviews.extend(items)
        
        if total is None:
            total = data.get("total", 0)
        
        print(f"Получена страница {page_index} ({len(items)} отзывов), всего: {len(all_reviews)} / {total}")
        
        if len(items) < page_size or (total and len(all_reviews) >= total):
            break
        
        page_index += 1
        # time.sleep(randint(3, 50)/100)  # Пауза для избежания ограничений по скорости запросов
        
    return all_reviews

def save_reviews_to_json(reviews: List[Dict], filename: str):
    """
    Сохранение отзывов в JSON-файл в формате, аналогичном ответу API,
    с добавленным полем bank_name.
    
    :param reviews: Список отзывов
    :param filename: Имя файла для сохранения
    """
    output = {
        "items": reviews,
        "pageIndex": 0,
        "pageSize": len(reviews),
        "total": len(reviews)
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    print(f"Сохранено {len(reviews)} отзывов в {filename}")

if __name__ == "__main__":
    
    os.makedirs('sravniru/reviews', exist_ok=True)
    filename = "sravniru/bank_mapping.json"
    id_to_name = {}
    try:
        with open(filename, "r", encoding="utf-8") as f:
            id_to_name  = json.load(f)
            name_to_id = {id_: name for name, id_ in id_to_name.items()}
    except FileNotFoundError:
        print(f"Файл {filename} не найден. Используем пустой маппинг.")
    except json.JSONDecodeError:
        print(f"Ошибка декодирования JSON в файле {filename}. Используем пустой маппинг.")
    # print(id_to_name)
    BANK_MAPPING = id_to_name
    for object_id, bank_name in BANK_MAPPING.items():
        print(f"Сбор отзывов для {bank_name} ({object_id})...")
        specific_reviews = fetch_reviews(review_object_id=object_id)
        save_reviews_to_json(specific_reviews, f"sravniru/reviews/reviews_{bank_name}.json")

    # Затем собираем отзывы для всех банков (без ReviewObjectId)
    # print("Сбор отзывов для всех банков...")
    # all_reviews = fetch_reviews(review_object_id=None)
    # save_reviews_to_json(all_reviews, "sravniru/reviews_all_banks.json")
    