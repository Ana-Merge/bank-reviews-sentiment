import os
import json
import hashlib
import re
from collections import defaultdict, Counter
from datetime import datetime


def clean_json(string):
    string = re.sub(r':\s*([}\]])', r': null\1', string)
    string = re.sub(r':\s*,', r': null,', string)
    string = re.sub(r',\s*([}\]])', r'\1', string)
    return string


def collect_reviews(directory, output_directory, output_all='all_reviews.jsonl', output_gazprom='gazprom_reviews.jsonl', output_duplicates='duplicates.jsonl'):
    os.makedirs(output_directory, exist_ok=True)  # Гарантируем создание папки
    
    output_all = os.path.join(output_directory, output_all)
    output_gazprom = os.path.join(output_directory, output_gazprom)
    output_duplicates = os.path.join(output_directory, output_duplicates)
    seen_hashes = set()
    duplicates_count = 0
    
    total_unique = 0
    unique_product_count = defaultdict(int)
    unique_bank_count = defaultdict(int)
    unique_product_per_bank = defaultdict(lambda: defaultdict(int))
    unique_rating_count = defaultdict(int)
    unique_sum_rating_product = defaultdict(float)
    unique_sum_rating_bank = defaultdict(float)
    unique_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    unique_date_year_count = defaultdict(int)
    unique_date_month_count = defaultdict(int)
    unique_long_reviews = 0
    unique_themes = Counter()
    
    total_dup = 0
    dup_product_count = defaultdict(int)
    dup_bank_count = defaultdict(int)
    dup_product_per_bank = defaultdict(lambda: defaultdict(int))
    dup_rating_count = defaultdict(int)
    dup_sum_rating_product = defaultdict(float)
    dup_sum_rating_bank = defaultdict(float)
    dup_sum_rating_product_bank = defaultdict(lambda: defaultdict(float))
    dup_date_year_count = defaultdict(int)
    dup_date_month_count = defaultdict(int)
    dup_long_reviews = 0
    dup_themes = Counter()
    
    with open(output_all, 'w', encoding='utf-8') as f_all, \
         open(output_gazprom, 'w', encoding='utf-8') as f_gazprom, \
         open(output_duplicates, 'w', encoding='utf-8') as f_duplicates:
        
        for filename in os.listdir(directory):
            if filename.endswith('.jsonl'):
                filepath = os.path.join(directory, filename)
                topic = os.path.splitext(filename)[0]
                
                with open(filepath, 'r', encoding='utf-8') as f_in:
                    for line_num, line in enumerate(f_in):
                        line = clean_json(line.strip())
                        if not line:
                            continue
                        
                        try:
                            data = json.loads(line)
                            
                            for key in data:
                                if key.isdigit():
                                    review = data[key]
                                    review['topic'] = topic
                                    
                                    unique_str = f"{review.get('review_text', '')}|{review.get('review_date', '')}|{review.get('bank_name', '')}|{review.get('rating', '')}"
                                    review_hash = hashlib.sha256(unique_str.encode('utf-8')).hexdigest()
                                    
                                    bank = review.get('bank_name', '')
                                    rating_str = review.get('rating', '')
                                    theme = review.get('review_theme', '')
                                    text = review.get('review_text', '')
                                    date_str = review.get('review_date', '')
                                    
                                    year = None
                                    month = None
                                    try:
                                        date_obj = datetime.strptime(date_str, "%d.%m.%Y %H:%M")
                                        year = date_obj.year
                                        month = date_obj.strftime("%Y-%m")
                                    except ValueError:
                                        pass
                                    
                                    rating = float(rating_str) if rating_str.isdigit() else 0.0

                                    is_long = len(text) > 200
                                    
                                    if review_hash in seen_hashes:
                                        json.dump(review, f_duplicates, ensure_ascii=False)
                                        f_duplicates.write('\n')
                                        duplicates_count += 1
                                        total_dup += 1
                                        dup_product_count[topic] += 1
                                        dup_bank_count[bank] += 1
                                        dup_product_per_bank[bank][topic] += 1
                                        dup_rating_count[rating_str] += 1
                                        dup_sum_rating_product[topic] += rating
                                        dup_sum_rating_bank[bank] += rating
                                        dup_sum_rating_product_bank[bank][topic] += rating
                                        if year:
                                            dup_date_year_count[year] += 1
                                        if month:
                                            dup_date_month_count[month] += 1
                                        if is_long:
                                            dup_long_reviews += 1
                                        dup_themes[theme] += 1
                                    else:
                                        seen_hashes.add(review_hash)
                                        
                                        json.dump(review, f_all, ensure_ascii=False)
                                        f_all.write('\n')
                                        if rating == 0:
                                            with open(output_directory + "/what.txt", "a", encoding='utf-8') as f:
                                                json.dump(review, f, ensure_ascii=False)
                                                f.write('\n')
                                        
                                        total_unique += 1
                                        unique_product_count[topic] += 1
                                        unique_bank_count[bank] += 1
                                        unique_product_per_bank[bank][topic] += 1
                                        unique_rating_count[rating_str] += 1
                                        unique_sum_rating_product[topic] += rating
                                        unique_sum_rating_bank[bank] += rating
                                        unique_sum_rating_product_bank[bank][topic] += rating
                                        if year:
                                            unique_date_year_count[year] += 1
                                        if month:
                                            unique_date_month_count[month] += 1
                                        if is_long:
                                            unique_long_reviews += 1
                                        unique_themes[theme] += 1
                                        
                                        if review.get('bank_name') == 'Газпромбанк':
                                            json.dump(review, f_gazprom, ensure_ascii=False)
                                            f_gazprom.write('\n')
                        except json.JSONDecodeError:
                            continue
    
    unique_avg_rating_per_product = {k: unique_sum_rating_product[k] / unique_product_count[k] if unique_product_count[k] > 0 else 0 for k in unique_product_count}
    unique_avg_rating_per_bank = {k: unique_sum_rating_bank[k] / unique_bank_count[k] if unique_bank_count[k] > 0 else 0 for k in unique_bank_count}
    unique_avg_rating_per_product_bank = {bank: {topic: unique_sum_rating_product_bank[bank][topic] / unique_product_per_bank[bank][topic] if unique_product_per_bank[bank][topic] > 0 else 0 for topic in unique_product_per_bank[bank]} for bank in unique_product_per_bank}
    
    dup_avg_rating_per_product = {k: dup_sum_rating_product[k] / dup_product_count[k] if dup_product_count[k] > 0 else 0 for k in dup_product_count}
    dup_avg_rating_per_bank = {k: dup_sum_rating_bank[k] / dup_bank_count[k] if dup_bank_count[k] > 0 else 0 for k in dup_bank_count}
    dup_avg_rating_per_product_bank = {bank: {topic: dup_sum_rating_product_bank[bank][topic] / dup_product_per_bank[bank][topic] if dup_product_per_bank[bank][topic] > 0 else 0 for topic in dup_product_per_bank[bank]} for bank in dup_product_per_bank}
    
    unique_stats = {
        'total_unique': total_unique,
        'product_count': dict(unique_product_count),
        'bank_count': dict(unique_bank_count),
        'product_per_bank': {bank: dict(unique_product_per_bank[bank]) for bank in unique_product_per_bank},
        'rating_count': dict(unique_rating_count),
        'avg_rating_per_product': unique_avg_rating_per_product,
        'avg_rating_per_bank': unique_avg_rating_per_bank,
        'avg_rating_per_product_bank': unique_avg_rating_per_product_bank,
        'date_year_count': dict(unique_date_year_count),
        'date_month_count': dict(unique_date_month_count),
        'long_reviews_count': unique_long_reviews,
        'top_themes': unique_themes.most_common(10)
    }
    
    dup_stats = {
        'total_duplicates': total_dup,
        'product_count': dict(dup_product_count),
        'bank_count': dict(dup_bank_count),
        'product_per_bank': {bank: dict(dup_product_per_bank[bank]) for bank in dup_product_per_bank},
        'rating_count': dict(dup_rating_count),
        'avg_rating_per_product': dup_avg_rating_per_product,
        'avg_rating_per_bank': dup_avg_rating_per_bank,
        'avg_rating_per_product_bank': dup_avg_rating_per_product_bank,
        'date_year_count': dict(dup_date_year_count),
        'date_month_count': dict(dup_date_month_count),
        'long_reviews_count': dup_long_reviews,
        'top_themes': dup_themes.most_common(10)
    }
    
    try:
        with open(os.path.join(output_directory, 'stats_unique.json'), 'w', encoding='utf-8') as f:
            json.dump(unique_stats, f, ensure_ascii=False, indent=4)
    except Exception:
        pass
    
    try:
        with open(os.path.join(output_directory, 'stats_duplicates.json'), 'w', encoding='utf-8') as f:
            json.dump(dup_stats, f, ensure_ascii=False, indent=4)
    except Exception:
        pass
    
    with open(output_duplicates, 'a', encoding='utf-8') as f_duplicates:
        f_duplicates.write(f"\nВсего дубликатов: {duplicates_count}\n")


if __name__ == "__main__":
    collect_reviews("data/bankiru_raw", "data/processed/banki")