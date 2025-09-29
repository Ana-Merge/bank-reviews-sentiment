import os
import json
import hashlib
import re
from collections import defaultdict, Counter
from datetime import datetime, timezone
import gc
import ijson  # Для потокового парсинга

translation_dict = {
    "savings": "deposits",
    "debitcards": "debitcards",
    "servicelevel": "individual",
    "remoteservice": "remote",
    "creditcards": "creditcards",
    "credits": "credits",
    "other": "other",
    "currencyexchange": "other",
    "mortgage": "hypothec",
    "autocredits": "credits",
    "mobilnoyeprilozheniye": "mobile_app",
    "usloviya": "individual",
    "creditrefinancing": "restructing",
    "mortgagerefinancing": "restructing",
    "businessrko": "",
    "acquiring": "",
    "businesscredits": "",
    "moneyorder": "other",
    "unknown": "other",
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
            return True
        if disallowed and str(value) in [str(x) for x in disallowed]:
            # print(f"Отфильтровано по {field} (disallowed): {value}")
            return True
    # Фильтр по дате
    date_str = review.get('review_date', '')
    if not date_str:
        # print(f"Отфильтровано по отсутствию даты: {date_str}")
        return True
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
        return True 
    return False

# Функция для обновления статистики
def update_stats(review, counters, sum_rating_product, sum_rating_bank, sum_rating_product_bank):
    topic = review['topic']
    bank = review['bank_name']
    rating = float(review['rating']) if review['rating'] else 0.0  # Упрощено, так как rating теперь всегда int
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
        counters['long_reviews_count'] += 1
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
        'long_reviews_count': counters['long_reviews_count'],
        'top_themes': counters.get('themes', Counter()).most_common(10)
    }
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=4)

# Функция очистки JSON (из prepare_banki.py)
def clean_json(string):
    string = re.sub(r':\s*([}\]])', r': null\1', string)
    string = re.sub(r':\s*,', r': null,', string)
    string = re.sub(r',\s*([}\]])', r'\1', string)
    return string

# Функция нормализации verification_status
def normalize_verification_status(status, source, rating):
    if source == 'sravni':
        if status == 'rateApproved':
            return 'rateApproved'
        elif status == 'rateRejected':
            return 'rateRejected'
        else:
            return status  # Оставляем как есть, или маппим на rateRejected если нужно
    elif source == 'banki':
        if status == 'Оценка:':
            return 'rateApproved'
        elif status == 'Без оценки':
            return 'rateRejected'
        else:
            return 'rateRejected'
    return 'unknown'

# Функция для обработки JSON-файла (Sravni)
def process_json_reviews(json_file, filters, start_date, end_date, output_dir, source, is_gazprom=False, subsample=None):
    seen_hashes = set()
    
    unique_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                       'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                       'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews_count': 0, 
                       'themes': Counter()}
    unique_sum_rating_product = defaultdict(float)
    unique_sum_rating_bank = defaultdict(float)
    unique_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    dup_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                    'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                    'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews_count': 0, 
                    'themes': Counter()}
    dup_sum_rating_product = defaultdict(float)
    dup_sum_rating_bank = defaultdict(float)
    dup_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    # Отдельные счётчики для Газпромбанка (уникальные и дубликаты)
    gazprom_unique_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                               'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                               'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews_count': 0, 
                               'themes': Counter()}
    gazprom_unique_sum_rating_product = defaultdict(float)
    gazprom_unique_sum_rating_bank = defaultdict(float)
    gazprom_unique_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    gazprom_dup_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                            'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                            'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews_count': 0, 
                            'themes': Counter()}
    gazprom_dup_sum_rating_product = defaultdict(float)
    gazprom_dup_sum_rating_bank = defaultdict(float)
    gazprom_dup_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    all_reviews_path = os.path.join(output_dir, 'all_reviews.jsonl')
    gazprom_reviews_path = os.path.join(output_dir, 'gazprom_reviews.jsonl')
    duplicates_path = os.path.join(output_dir, 'duplicates.jsonl')
    
    if not os.path.exists(json_file):
        print(f"Файл {json_file} не существует")
        return (unique_counters, unique_sum_rating_product, unique_sum_rating_bank, unique_sum_rating_product_bank,
                dup_counters, dup_sum_rating_product, dup_sum_rating_bank, dup_sum_rating_product_bank,
                gazprom_unique_counters, gazprom_unique_sum_rating_product, gazprom_unique_sum_rating_bank, gazprom_unique_sum_rating_product_bank,
                gazprom_dup_counters, gazprom_dup_sum_rating_product, gazprom_dup_sum_rating_bank, gazprom_dup_sum_rating_product_bank)
    
    with open(all_reviews_path, 'a', encoding='utf-8') as f_all, \
         open(gazprom_reviews_path, 'a', encoding='utf-8') as f_gazprom, \
         open(duplicates_path, 'a', encoding='utf-8') as f_dup, \
         open(json_file, 'rb') as f:
        
        items = ijson.items(f, 'items.item')
        processed = 0
        filtered = 0  # Добавлен счётчик отфильтрованных
        for item in items:
            processed += 1
            if subsample and processed > subsample:
                break
            
            raw_topic = item.get('reviewTag', 'unknown').lower()
            topic = translation_dict.get(raw_topic, 'other')
            
            date_iso = item.get('date', '1970-01-01T00:00:00Z')
            try:
                date_obj = datetime.fromisoformat(date_iso.replace('Z', '+00:00'))
                review_date = date_obj.strftime('%d.%m.%Y %H:%M')
            except ValueError:
                review_date = '01.01.1970 00:00'
            
            rating = item.get('rating', 0)
            try:
                rating = int(rating)  # Преобразование в int
            except ValueError:
                rating = 0
            
            status = item.get('ratingStatus', 'unknown')
            normalized_status = normalize_verification_status(status, source, rating)
            if normalized_status == 'rateRejected':
                rating = 0
            
            review = {
                'bank_name': item.get('bank_name', 'Другие банки') if not is_gazprom else 'Газпромбанк',
                'review_theme': item.get('title', 'unknown'),
                'rating': rating,
                'verification_status': normalized_status,
                'review_text': item.get('text', 'unknown'),
                'review_date': review_date,
                'topic': topic,
                'source': source
            }
            
            if filter_review(review, filters, start_date, end_date):
                filtered += 1
                continue
            
            unique_str = f"{review['review_text']}|{review['review_date']}|{review['bank_name']}|{str(review['rating'])}"
            review_hash = hashlib.sha256(unique_str.encode('utf-8')).hexdigest()
            
            if review_hash in seen_hashes:
                update_stats(review, dup_counters, dup_sum_rating_product, dup_sum_rating_bank, dup_sum_rating_product_bank)
                if review['bank_name'] == 'Газпромбанк':
                    update_stats(review, gazprom_dup_counters, gazprom_dup_sum_rating_product, gazprom_dup_sum_rating_bank, gazprom_dup_sum_rating_product_bank)
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
                    update_stats(review, gazprom_unique_counters, gazprom_unique_sum_rating_product, gazprom_unique_sum_rating_bank, gazprom_unique_sum_rating_product_bank)
    
    print(f"Файл {json_file}: обработано отзывов: {processed}, записано уникальных: {unique_counters['total']}, дубликатов: {dup_counters['total']}, отфильтровано: {filtered}")
    gc.collect()
    return (unique_counters, unique_sum_rating_product, unique_sum_rating_bank, unique_sum_rating_product_bank,
            dup_counters, dup_sum_rating_product, dup_sum_rating_bank, dup_sum_rating_product_bank,
            gazprom_unique_counters, gazprom_unique_sum_rating_product, gazprom_unique_sum_rating_bank, gazprom_unique_sum_rating_product_bank,
            gazprom_dup_counters, gazprom_dup_sum_rating_product, gazprom_dup_sum_rating_bank, gazprom_dup_sum_rating_product_bank)

# Функция для обработки JSONL-файлов (Banki)
def process_jsonl_reviews(directory, filters, start_date, end_date, output_dir, source, subsample=None):
    seen_hashes = set()
    
    unique_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                       'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                       'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews_count': 0, 
                       'themes': Counter()}
    unique_sum_rating_product = defaultdict(float)
    unique_sum_rating_bank = defaultdict(float)
    unique_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    dup_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                    'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                    'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews_count': 0, 
                    'themes': Counter()}
    dup_sum_rating_product = defaultdict(float)
    dup_sum_rating_bank = defaultdict(float)
    dup_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    # Отдельные счётчики для Газпромбанка (уникальные и дубликаты)
    gazprom_unique_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                               'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                               'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews_count': 0, 
                               'themes': Counter()}
    gazprom_unique_sum_rating_product = defaultdict(float)
    gazprom_unique_sum_rating_bank = defaultdict(float)
    gazprom_unique_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    gazprom_dup_counters = {'total': 0, 'product_count': defaultdict(int), 'bank_count': defaultdict(int), 
                            'product_per_bank': defaultdict(lambda: defaultdict(int)), 'rating_count': defaultdict(int), 
                            'date_year_count': defaultdict(int), 'date_month_count': defaultdict(int), 'long_reviews_count': 0, 
                            'themes': Counter()}
    gazprom_dup_sum_rating_product = defaultdict(float)
    gazprom_dup_sum_rating_bank = defaultdict(float)
    gazprom_dup_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    
    all_reviews_path = os.path.join(output_dir, 'all_reviews.jsonl')
    gazprom_reviews_path = os.path.join(output_dir, 'gazprom_reviews.jsonl')
    duplicates_path = os.path.join(output_dir, 'duplicates.jsonl')
    
    with open(all_reviews_path, 'a', encoding='utf-8') as f_all, \
         open(gazprom_reviews_path, 'a', encoding='utf-8') as f_gazprom, \
         open(duplicates_path, 'a', encoding='utf-8') as f_dup:
        
        processed_total = 0
        filtered_total = 0  # Общий счётчик отфильтрованных для всех файлов
        
        for filename in os.listdir(directory):
            if filename.endswith('.jsonl'):
                filepath = os.path.join(directory, filename)
                raw_topic = os.path.splitext(filename)[0].lower()
                topic = translation_dict.get(raw_topic, 'other')
                
                with open(filepath, 'r', encoding='utf-8') as f_in:
                    processed = 0
                    filtered = 0  # Счётчик отфильтрованных для файла
                    for line_num, line in enumerate(f_in):
                        line = clean_json(line.strip())
                        if not line:
                            continue
                        
                        try:
                            data = json.loads(line)
                            for key in data:
                                if key.isdigit():
                                    review_data = data[key]
                                    review_data['topic'] = topic
                                    
                                    date_str = review_data.get('review_date', '')
                                    if not date_str:
                                        review_date = '01.01.1970 00:00'
                                    else:
                                        try:
                                            date_obj = datetime.strptime(date_str, '%d.%m.%Y %H:%M')
                                            review_date = date_obj.strftime('%d.%m.%Y %H:%M')
                                        except ValueError:
                                            review_date = '01.01.1970 00:00'
                                    
                                    rating = review_data.get('rating', 0)
                                    try:
                                        rating = int(rating)  # Преобразование в int (если строка)
                                    except ValueError:
                                        rating = 0
                                    
                                    status = review_data.get('verification_status', 'unknown')
                                    normalized_status = normalize_verification_status(status, source, rating)
                                    if normalized_status == 'rateRejected':
                                        rating = 0
                                    
                                    review = {
                                        'bank_name': review_data.get('bank_name', 'Другие банки'),
                                        'review_theme': review_data.get('review_theme', 'unknown'),
                                        'rating': rating,
                                        'verification_status': normalized_status,
                                        'review_text': review_data.get('review_text', 'unknown'),
                                        'review_date': review_date,
                                        'topic': topic,
                                        'source': source
                                    }
                                    
                                    if filter_review(review, filters, start_date, end_date):
                                        filtered += 1
                                        continue
                                    
                                    unique_str = f"{review['review_text']}|{review['review_date']}|{review['bank_name']}|{str(review['rating'])}"
                                    review_hash = hashlib.sha256(unique_str.encode('utf-8')).hexdigest()
                                    
                                    if review_hash in seen_hashes:
                                        update_stats(review, dup_counters, dup_sum_rating_product, dup_sum_rating_bank, dup_sum_rating_product_bank)
                                        if review['bank_name'] == 'Газпромбанк':
                                            update_stats(review, gazprom_dup_counters, gazprom_dup_sum_rating_product, gazprom_dup_sum_rating_bank, gazprom_dup_sum_rating_product_bank)
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
                                            update_stats(review, gazprom_unique_counters, gazprom_unique_sum_rating_product, gazprom_unique_sum_rating_bank, gazprom_unique_sum_rating_product_bank)
                                    processed += 1
                                    processed_total += 1
                                    if subsample and processed_total > subsample:
                                        break
                        except json.JSONDecodeError:
                            continue
                    filtered_total += filtered
                    print(f"Файл {filepath}: обработано отзывов: {processed}, отфильтровано: {filtered}")
        
        print(f"Директория {directory}: всего обработано отзывов: {processed_total}, записано уникальных: {unique_counters['total']}, дубликатов: {dup_counters['total']}, отфильтровано: {filtered_total}")
    
    gc.collect()
    return (unique_counters, unique_sum_rating_product, unique_sum_rating_bank, unique_sum_rating_product_bank,
            dup_counters, dup_sum_rating_product, dup_sum_rating_bank, dup_sum_rating_product_bank,
            gazprom_unique_counters, gazprom_unique_sum_rating_product, gazprom_unique_sum_rating_bank, gazprom_unique_sum_rating_product_bank,
            gazprom_dup_counters, gazprom_dup_sum_rating_product, gazprom_dup_sum_rating_bank, gazprom_dup_sum_rating_product_bank)

# Функция для объединения статистики
def combine_stats(stats_list):
    combined = {
        'total': 0,
        'product_count': defaultdict(int),
        'bank_count': defaultdict(int),
        'product_per_bank': defaultdict(lambda: defaultdict(int)),
        'rating_count': defaultdict(int),
        'date_year_count': defaultdict(int),
        'date_month_count': defaultdict(int),
        'long_reviews_count': 0,
        'themes': Counter()
    }
    combined_sum_rating_product = defaultdict(float)
    combined_sum_rating_bank = defaultdict(float)
    combined_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))

    for counters, sum_rating_product, sum_rating_bank, sum_rating_product_bank in stats_list:
        combined['total'] += counters['total']
        for k, v in counters['product_count'].items():
            combined['product_count'][k] += v
        for k, v in counters['bank_count'].items():
            combined['bank_count'][k] += v
        for bank, topics in counters['product_per_bank'].items():
            for topic, v in topics.items():
                combined['product_per_bank'][bank][topic] += v
        for k, v in counters['rating_count'].items():
            combined['rating_count'][k] += v
        for k, v in counters.get('date_year_count', {}).items():
            combined['date_year_count'][k] += v
        for k, v in counters.get('date_month_count', {}).items():
            combined['date_month_count'][k] += v
        combined['long_reviews_count'] += counters.get('long_reviews_count', 0)
        combined['themes'].update(counters.get('themes', Counter()))

        # Накопление сумм рейтингов (используем count для пересчёта)
        for k in counters['product_count']:
            combined_sum_rating_product[k] += sum_rating_product.get(k, 0)
        for k in counters['bank_count']:
            combined_sum_rating_bank[k] += sum_rating_bank.get(k, 0)
        for bank in counters['product_per_bank']:
            for topic in counters['product_per_bank'][bank]:
                combined_sum_rating_product_bank[bank][topic] += sum_rating_product_bank[bank].get(topic, 0)

    return (combined, combined_sum_rating_product, combined_sum_rating_bank, combined_sum_rating_product_bank)

# Главная функция
def prepare_common(process_sravni=True, process_banki=True,
                  sravni_dir='data/sravni_raw',
                  banki_dir='data/bankiru_raw',
                  subsample_sravni=None,
                  subsample_banki=None,
                  filters=None, start_date=None, end_date=None):
    if filters is None:
        filters = {}
    
    if start_date is None:
        start_date = datetime(2000, 1, 1, tzinfo=timezone.utc)
    if end_date is None:
        end_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    
    output_dir = 'data/processed/common'
    os.makedirs(output_dir, exist_ok=True)
    
    # Очистка файлов перед обработкой
    open(os.path.join(output_dir, 'all_reviews.jsonl'), 'w').close()
    open(os.path.join(output_dir, 'gazprom_reviews.jsonl'), 'w').close()
    open(os.path.join(output_dir, 'duplicates.jsonl'), 'w').close()
    
    unique_stats_list = []
    dup_stats_list = []
    gazprom_unique_stats_list = []
    gazprom_dup_stats_list = []
    
    if process_sravni:
        sravni_files = ['gazprom_reviews.json', 'all_reviews.json']
        for sravni_file in sravni_files:
            file_path = os.path.join(sravni_dir, sravni_file)
            u_c, u_s_p, u_s_b, u_s_p_b, d_c, d_s_p, d_s_b, d_s_p_b, g_u_c, g_u_s_p, g_u_s_b, g_u_s_p_b, g_d_c, g_d_s_p, g_d_s_b, g_d_s_p_b = process_json_reviews(
                file_path, filters, start_date, end_date, output_dir, source='sravni', is_gazprom=(sravni_file == 'gazprom_reviews.json'), subsample=subsample_sravni)
            save_stats(u_c, u_s_p, u_s_b, u_s_p_b, os.path.join(output_dir, f'stats_sravni_{sravni_file.replace(".json", "")}.json'))
            save_stats(d_c, d_s_p, d_s_b, d_s_p_b, os.path.join(output_dir, f'stats_sravni_{sravni_file.replace(".json", "")}_duplicates.json'))
            unique_stats_list.append((u_c, u_s_p, u_s_b, u_s_p_b))
            dup_stats_list.append((d_c, d_s_p, d_s_b, d_s_p_b))
            gazprom_unique_stats_list.append((g_u_c, g_u_s_p, g_u_s_b, g_u_s_p_b))
            gazprom_dup_stats_list.append((g_d_c, g_d_s_p, g_d_s_b, g_d_s_p_b))
    
    if process_banki:
        u_c, u_s_p, u_s_b, u_s_p_b, d_c, d_s_p, d_s_b, d_s_p_b, g_u_c, g_u_s_p, g_u_s_b, g_u_s_p_b, g_d_c, g_d_s_p, g_d_s_b, g_d_s_p_b = process_jsonl_reviews(
            banki_dir, filters, start_date, end_date, output_dir, source='banki', subsample=subsample_banki)
        save_stats(u_c, u_s_p, u_s_b, u_s_p_b, os.path.join(output_dir, 'stats_banki.json'))
        save_stats(d_c, d_s_p, d_s_b, d_s_p_b, os.path.join(output_dir, 'stats_banki_duplicates.json'))
        unique_stats_list.append((u_c, u_s_p, u_s_b, u_s_p_b))
        dup_stats_list.append((d_c, d_s_p, d_s_b, d_s_p_b))
        gazprom_unique_stats_list.append((g_u_c, g_u_s_p, g_u_s_b, g_u_s_p_b))
        gazprom_dup_stats_list.append((g_d_c, g_d_s_p, g_d_s_b, g_d_s_p_b))
    
    # Сохранение общей статистики
    if unique_stats_list:
        combined_unique, c_u_s_p, c_u_s_b, c_u_s_p_b = combine_stats(unique_stats_list)
        save_stats(combined_unique, c_u_s_p, c_u_s_b, c_u_s_p_b, os.path.join(output_dir, 'stats_common.json'))
    
    if dup_stats_list:
        combined_dup, c_d_s_p, c_d_s_b, c_d_s_p_b = combine_stats(dup_stats_list)
        save_stats(combined_dup, c_d_s_p, c_d_s_b, c_d_s_p_b, os.path.join(output_dir, 'stats_common_duplicates.json'))
    
    # Сохранение статистики по Газпромбанку (из всех источников)
    if gazprom_unique_stats_list:
        combined_gazprom_unique, c_g_u_s_p, c_g_u_s_b, c_g_u_s_p_b = combine_stats(gazprom_unique_stats_list)
        save_stats(combined_gazprom_unique, c_g_u_s_p, c_g_u_s_b, c_g_u_s_p_b, os.path.join(output_dir, 'stats_gazprom.json'))
    
    if gazprom_dup_stats_list:
        combined_gazprom_dup, c_g_d_s_p, c_g_d_s_b, c_g_d_s_p_b = combine_stats(gazprom_dup_stats_list)
        save_stats(combined_gazprom_dup, c_g_d_s_p, c_g_d_s_b, c_g_d_s_p_b, os.path.join(output_dir, 'stats_gazprom_duplicates.json'))

if __name__ == "__main__":
    example_filters = {
        # 'rating': {'disallowed': ['0', '', 0]},
        'topic': {'disallowed': [""]}
    }
    prepare_common(process_sravni=True, process_banki=True, filters=example_filters)