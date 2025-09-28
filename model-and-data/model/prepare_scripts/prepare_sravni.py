import os
import json
import hashlib
import re
from collections import defaultdict, Counter
from datetime import datetime, timezone
import gc
import random
import ijson  # Для потокового парсинга

# Определение дерева тегов
tag_tree = {
    'Эквайринг': None,
    'Автокредиты': ['На автомобиль'],
    'Кредиты для бизнеса': None,
    'Расчетно-кассовое обслуживание': None,
    'Кредитные карты': ['180 дней без %', 'Автодрайв Platinum Credit', 'Кредитная карта', 'Кредитная карта 180 дней',
                        'Кредитная карта 180 дней Премиум', 'Кредитная карта 90 дней', 'Кредитная кэшбэк карта',
                        'Простая кредитная карта', 'Удобная', 'Умная', 'Умная карта'],
    'Рефинансирование кредитов': None,
    'Потребительские кредиты': ['Наличными', 'Под залог недвижимости', 'Рефинансирование'],
    'Обмен валюты': None,
    'Дебетовые карты': ['Автодрайв Platinum', 'Бесплатная', 'Газпромнефть', 'Дебетовая карта', 'Дебитовая',
                        'Молодежная карта ГПБ&РСМ', 'Пенсионная', 'Пенсионная карта', 'Премиум UP', 'Путешественник',
                        'Самая выгодная', 'Умная', 'Умная карта', 'Умная карта (Премиум)'],
    'Мобильное приложение': None,
    'Денежные переводы': None,
    'Ипотека': ['Вторичное жилье', 'Льготная', 'На вторичное жилье', 'На новостройку', 'Рефинансирование', 'Семейная',
                'Семейная ипотека'],
    'Рефинансирование ипотеки': None,
    'Прочее': None,
    'Онлайн-обслуживание': None,
    'Вклады и сбережения': ['В Плюсе', 'В Плюсе (% в конце срока)', 'Ваш успех', 'Ваш успех (Хит сезона)',
                            'Ежедневный процент', 'Заоблачный процент', 'Копить', 'Копить (% в конце срока)',
                            'Копить (% ежемесячно)', 'Накопительный', 'Накопительный счет', 'Накопительный счёт',
                            'Новые деньги', 'Пенсионный доход', 'Трать и копи', 'Управляй процентом', 'Управлять',
                            'процент успеха'],
    'Уровень обслуживания': None,
    'Условия обслуживания': None
}

# Словарь для перевода тем из Sravni.ru в темы Banki.ru
translation_dict = {
    "savings": "deposits",
    "debitcards": "debitcards",
    "servicelevel": "individual",
    "remoteservice": "remote",
    "creditcards": "creditcards",
    "credits": "credits",
    "other": "other",
    "currencyexchange": "currencyexchange",
    "mortgage": "hypothec",
    "autocredits": "autocredits",
    "mobilnoyeprilozheniye": "mobile_app",
    "usloviya": "usloviya",
    "creditrefinancing": "creditrefinancing",
    "mortgagerefinancing": "mortgagerefinancing",
    "businessrko": "",
    "acquiring": "",
    "businesscredits": "",
    "moneyorder": "moneyorder",
    "unknown": "unknown"  # Для случаев, когда reviewTag отсутствует или не распознается
}

# Функция предобработки текста
def preprocess_text(text):
    if not isinstance(text, str):
        return ''
    text = text.lower()
    text = re.sub(r'[^a-zа-я0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Функция маппинга сентимента
def map_sentiment(rating):
    if rating is None or rating == 0:
        return 'unknown'
    elif rating <= 2:
        return 'отрицательно'
    elif rating == 3:
        return 'нейтрально'
    else:
        return 'положительно'

# Функция фильтрации отзыва
def filter_review(review, filters, start_date, end_date):
    for field, filter_dict in filters.items():
        value = review.get(field, '')
        allowed = filter_dict.get('allowed', [])
        disallowed = filter_dict.get('disallowed', [])
        if allowed and str(value) not in [str(x) for x in allowed]:
            # print(f"Отфильтровано по {field} (allowed): {value}")
            return True  # Skip
        if disallowed and str(value) in [str(x) for x in disallowed]:
            # print(f"Отфильтровано по {field} (disallowed): {value}")
            return True  # Skip
    # Фильтр по дате
    date_str = review.get('review_date', '')
    if not date_str:
        # print(f"Отфильтровано по отсутствию даты: {date_str}")
        return True  # Skip если дата отсутствует
    try:
        review_date = datetime.strptime(date_str, '%d.%m.%Y %H:%M')
        if start_date and review_date < start_date.replace(tzinfo=None):  # Сравниваем без часового пояса
            # print(f"Отфильтровано по дате (раньше start_date): {date_str}")
            return True
        if end_date and review_date > end_date.replace(tzinfo=None):  # Сравниваем без часового пояса
            # print(f"Отфильтровано по дате (позже end_date): {date_str}")
            return True
    except ValueError:
        # print(f"Отфильтровано по невалидной дате: {date_str}")
        return True  # Skip invalid date
    return False  # Keep

# Функция для обновления статистики
def update_stats(review, counters, sum_rating_product, sum_rating_bank, sum_rating_product_bank):
    topic = review['topic']
    bank = review['bank_name']
    rating = float(review['rating']) if review['rating'] and str(review['rating']).isdigit() else 0.0
    theme = review.get('review_theme', 'unknown')
    text = review['review_text']
    date_str = review['review_date']
    is_long = len(text) > 200
    
    year = None
    month = None
    try:
        date_obj = datetime.strptime(date_str, '%d.%m.%Y %H:%M')
        year = date_obj.year
        month = date_obj.strftime('%Y-%m')
    except ValueError:
        pass
    
    counters['total'] += 1
    counters['product_count'][topic] += 1
    counters['bank_count'][bank] += 1
    counters['product_per_bank'][bank][topic] += 1
    counters['rating_count'][str(rating)] += 1
    sum_rating_product[topic] += rating
    sum_rating_bank[bank] += rating
    sum_rating_product_bank[bank][topic] += rating
    if year:
        counters['date_year_count'][year] += 1
    if month:
        counters['date_month_count'][month] += 1
    if is_long:
        counters['long_reviews'] += 1
    counters['themes'][theme] += 1

# Функция для расчёта средней
def calculate_avg(sum_dict, count_dict):
    return {k: sum_dict[k] / count_dict[k] if count_dict[k] > 0 else 0 for k in count_dict}

# Функция для расчёта средней по nested
def calculate_nested_avg(sum_nested, count_nested):
    return {bank: calculate_avg(sum_nested[bank], count_nested[bank]) for bank in count_nested}

# Функция для сохранения статистики
def save_stats(counters, sum_rating_product, sum_rating_bank, sum_rating_product_bank, output_path):
    stats = {
        'total': counters['total'],
        'product_count': dict(counters['product_count']),
        'bank_count': dict(counters['bank_count']),
        'product_per_bank': {bank: dict(counters['product_per_bank'][bank]) for bank in counters['product_per_bank']},
        'rating_count': dict(counters['rating_count']),
        'avg_rating_per_product': calculate_avg(sum_rating_product, counters['product_count']),
        'avg_rating_per_bank': calculate_avg(sum_rating_bank, counters['bank_count']),
        'avg_rating_per_product_bank': calculate_nested_avg(sum_rating_product_bank, counters['product_per_bank']),
        'date_year_count': dict(counters['date_year_count']),
        'date_month_count': dict(counters['date_month_count']),
        'long_reviews_count': counters['long_reviews'],
        'top_themes': counters['themes'].most_common(10)
    }
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=4)

# Функция для обработки JSON-файла
def process_json_reviews(json_file, filters, start_date, end_date, output_dir, is_gazprom=False, subsample=None):
    seen_hashes = set()
    
    # Инициализация статистики
    unique_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                       'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                       'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews': 0, 
                       'themes': Counter()}
    unique_sum_rating_product = defaultdict(float)
    unique_sum_rating_bank = defaultdict(float)
    unique_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    dup_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                    'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                    'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews': 0, 
                    'themes': Counter()}
    dup_sum_rating_product = defaultdict(float)
    dup_sum_rating_bank = defaultdict(float)
    dup_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    all_reviews_path = os.path.join(output_dir, 'all_reviews.jsonl')
    gazprom_reviews_path = os.path.join(output_dir, 'gazprom_reviews.jsonl')
    duplicates_path = os.path.join(output_dir, 'duplicates.jsonl')
    
    # Проверка существования входного файла
    if not os.path.exists(json_file):
        print(f"Файл {json_file} не существует")
        return (unique_counters, unique_sum_rating_product, unique_sum_rating_bank, unique_sum_rating_product_bank,
                dup_counters, dup_sum_rating_product, dup_sum_rating_bank, dup_sum_rating_product_bank)
    
    with open(all_reviews_path, 'a', encoding='utf-8') as f_all, \
         open(gazprom_reviews_path, 'a', encoding='utf-8') as f_gazprom, \
         open(duplicates_path, 'a', encoding='utf-8') as f_dup, \
         open(json_file, 'rb') as f:  # binary для ijson
        
        items = ijson.items(f, 'items.item')  # Корректный парсинг списка items
        processed = 0
        for item in items:
            processed += 1
            if subsample and processed > subsample:
                break
            
            # Дебаг для проверки считывания
            # print(f"Обработка отзыва: {item}")
            
            # Определение topic
            raw_topic = item.get('reviewTag', 'unknown').lower()
            topic = translation_dict.get(raw_topic, raw_topic) if raw_topic in translation_dict else 'other'
            if not topic:
                topic = 'other'
            
            # Преобразование даты из ISO 8601 в нужный формат
            date_iso = item.get('date', '1970-01-01T00:00:00Z')
            try:
                date_obj = datetime.fromisoformat(date_iso.replace('Z', '+00:00'))
                review_date = date_obj.strftime('%d.%m.%Y %H:%M')
            except ValueError:
                review_date = '01.01.1970 00:00'
            
            # Формирование отзыва с новыми полями
            review = {
                'bank_name': item.get('bank_name', 'Другие банки') if not is_gazprom else 'Газпромбанк',
                'review_theme': item.get('title', 'unknown'),
                'rating': item.get('rating', 0),  # Сохраняем как число
                'verification_status': item.get('ratingStatus', 'unknown'),
                'review_text': item.get('text', 'unknown'),
                'review_date': review_date,
                'topic': topic
            }
            
            # Фильтрация
            if filter_review(review, filters, start_date, end_date):
                continue
            
            # Хэш
            unique_str = f"{review['review_text']}|{review['review_date']}|{review['bank_name']}|{str(review['rating'])}"
            review_hash = hashlib.sha256(unique_str.encode('utf-8')).hexdigest()
            
            if review_hash in seen_hashes:
                update_stats(review, dup_counters, dup_sum_rating_product, dup_sum_rating_bank, dup_sum_rating_product_bank)
                json.dump(review, f_dup, ensure_ascii=False)
                f_dup.write('\n')
            else:
                seen_hashes.add(review_hash)
                update_stats(review, unique_counters, unique_sum_rating_product, unique_sum_rating_bank, unique_sum_rating_product_bank)
                json.dump(review, f_all, ensure_ascii=False)
                f_all.write('\n')
                if review['bank_name'] == 'Газпромбанк':
                    json.dump(review, f_gazprom, ensure_ascii=False)
                    f_gazprom.write('\n')
    
    print(f"Файл {json_file}: обработано отзывов: {processed}, записано уникальных: {unique_counters['total']}, дубликатов: {dup_counters['total']}")
    gc.collect()
    return (unique_counters, unique_sum_rating_product, unique_sum_rating_bank, unique_sum_rating_product_bank,
            dup_counters, dup_sum_rating_product, dup_sum_rating_bank, dup_sum_rating_product_bank)

# Главная функция
def prepare_data(process_gazprom=True, process_all_banks=True,
                 gazprom_file='data/sravni_raw/gazprom_reviews.json',
                 all_banks_file='data/sravni_raw/all_reviews.json',
                 subsample_all=None,
                 filters=None, start_date=None, end_date=None):
    if filters is None:
        filters = {}
    
    # Дефолтные даты (offset-aware)
    if start_date is None:
        start_date = datetime(2000, 1, 1, tzinfo=timezone.utc)
    if end_date is None:
        end_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    
    output_dir = 'data/processed/sravni'
    os.makedirs(output_dir, exist_ok=True)
    
    # Очистка файлов перед обработкой
    open(os.path.join(output_dir, 'all_reviews.jsonl'), 'w').close()
    open(os.path.join(output_dir, 'gazprom_reviews.jsonl'), 'w').close()
    open(os.path.join(output_dir, 'duplicates.jsonl'), 'w').close()
    
    if process_gazprom:
        u_c, u_s_p, u_s_b, u_s_p_b, d_c, d_s_p, d_s_b, d_s_p_b = process_json_reviews(
            gazprom_file, filters, start_date, end_date, output_dir, is_gazprom=True)
        save_stats(u_c, u_s_p, u_s_b, u_s_p_b, os.path.join(output_dir, 'stats_gazprom.json'))
        save_stats(d_c, d_s_p, d_s_b, d_s_p_b, os.path.join(output_dir, 'stats_gazprom_duplicates.json'))
    
    if process_all_banks:
        u_c, u_s_p, u_s_b, u_s_p_b, d_c, d_s_p, d_s_b, d_s_p_b = process_json_reviews(
            all_banks_file, filters, start_date, end_date, output_dir, 
            subsample=subsample_all if subsample_all is not None else None)
        save_stats(u_c, u_s_p, u_s_b, u_s_p_b, os.path.join(output_dir, 'stats_all_banks.json'))
        save_stats(d_c, d_s_p, d_s_b, d_s_p_b, os.path.join(output_dir, 'stats_all_banks_duplicates.json'))

if __name__ == "__main__":
    example_filters = {
        # 'rating': {'disallowed': [0]},
        'topic': {'disallowed': [""]},
        # 'verification_status': {'allowed': ['rateChecking', 'approved']},
    }
    prepare_data(process_gazprom=True, process_all_banks=True, filters=example_filters)