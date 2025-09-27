import os
import json
import hashlib
import re
from collections import defaultdict


def clean_json(string):
    string = re.sub(r':\s*([}\]])', r': null\1', string)
    string = re.sub(r':\s*,', r': null,', string)
    string = re.sub(r',\s*([}\]])', r'\1', string)
    return string


def collect_reviews(directory, output_directory, output_all='all_reviews.jsonl', output_gazprom='gazprom_reviews.jsonl', output_duplicates='duplicates.jsonl'):
    output_all = os.path.join(output_directory, output_all)
    output_gazprom = os.path.join(output_directory, output_gazprom)
    output_duplicates = os.path.join(output_directory, output_duplicates)
    seen_hashes = set()
    duplicates_count = 0
    
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
                                    
                                    if review_hash in seen_hashes:
                                        json.dump(review, f_duplicates, ensure_ascii=False)
                                        f_duplicates.write('\n')
                                        duplicates_count += 1
                                    else:
                                        seen_hashes.add(review_hash)
                                    
                                    json.dump(review, f_all, ensure_ascii=False)
                                    f_all.write('\n')
                                    
                                    if review.get('bank_name') == 'Газпромбанк':
                                        json.dump(review, f_gazprom, ensure_ascii=False)
                                        f_gazprom.write('\n')
                        except json.JSONDecodeError as e:
                            print(f"Ошибка {filename} в строке {line_num + 1}: {e}")
                            print(f"Содержание: {line[:50]}...")
                            continue
    
    with open(output_duplicates, 'a', encoding='utf-8') as f_duplicates:
        f_duplicates.write(f"\nВсего дубликатов: {duplicates_count}\n")


if __name__ == "__main__":
    collect_reviews("data\\bankiru_raw", "data/processed/banki")