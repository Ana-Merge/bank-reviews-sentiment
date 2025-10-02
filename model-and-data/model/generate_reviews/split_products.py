import json
from collections import defaultdict
import os

def determine_sentiment(rating):
    try:
        rating_int = int(rating)
        if rating_int >= 4:
            return "positive"
        elif rating_int == 3:
            return "neutral"
        else:
            return "negative"
    except ValueError:
        return "negative"  # Если rating не число, считать negative по умолчанию

# Путь к исходному файлу
input_file = 'data\\prepared\\common\\all_reviews.jsonl'
output_dir = 'data\\prepared\\splited\\'

# Убедимся, что выходная директория существует
os.makedirs(output_dir, exist_ok=True)

# Словарь для группировки: ключ - (topic, sentiment), значение - список JSON объектов
grouped_reviews = defaultdict(list)
stats = defaultdict(int)

# Чтение файла
with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
        if line.strip():
            review = json.loads(line)
            topic = review.get('topic', 'unknown')  # Если topic отсутствует, использовать 'unknown'
            rating = review.get('rating', 0)  # По умолчанию 0
            sentiment = determine_sentiment(rating)
            grouped_reviews[(topic, sentiment)].append(review)
            stats[(topic, sentiment)] += 1

# Создание отдельных файлов
output_files = []
for (topic, sentiment), reviews in grouped_reviews.items():
    output_file = f'{output_dir}{topic}_{sentiment}.jsonl'
    output_files.append((topic, sentiment, output_file, len(reviews)))
    with open(output_file, 'w', encoding='utf-8') as f_out:
        for review in reviews:
            f_out.write(json.dumps(review, ensure_ascii=False) + '\n')

# Создание файла со статистикой в древовидной структуре
stats_file = f'{output_dir}_reviews_stats.json'
stats_data = {
    "total_reviews": sum(stats.values()),
    "topics": {}
}

# Формирование древовидной структуры
for topic, sentiment, output_file, count in output_files:
    if topic not in stats_data["topics"]:
        stats_data["topics"][topic] = {
            "total": 0,
            "sentiments": {
                "positive": 0,
                "neutral": 0,
                "negative": 0
            },
            "files": {}
        }
    stats_data["topics"][topic]["total"] += count
    stats_data["topics"][topic]["sentiments"][sentiment] = count
    stats_data["topics"][topic]["files"][sentiment] = {
        "file_name": os.path.basename(output_file),
        "review_count": count
    }

with open(stats_file, 'w', encoding='utf-8') as f_stats:
    json.dump(stats_data, f_stats, ensure_ascii=False, indent=4)

print(f"Файлы созданы успешно. Статистика сохранена в {stats_file}")