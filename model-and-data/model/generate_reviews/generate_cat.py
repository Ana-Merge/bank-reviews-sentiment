import json
import random
from collections import defaultdict
import os
import itertools

# Маппинг тональностей (на русском для вывода)
sentiments_map = {
    "positive": "положительная",
    "neutral": "нейтральная",
    "negative": "негативная"
}

# Символы для соединения фрагментов
join_chars = [",", ".", ";", "...", " ", "-", "/", "?", "!", "&", ":"]

def load_reviews(stats_file, input_dir):
    reviews_by_theme_sentiment = defaultdict(list)
    topic_sentiment_counts = defaultdict(lambda: defaultdict(int))
    topic_total_counts = defaultdict(int)
    
    # Чтение статистики
    with open(stats_file, 'r', encoding='utf-8') as f:
        stats_data = json.load(f)
    
    # Получение списка тем из статистики
    themes = list(stats_data["topics"].keys())
    
    # Загрузка отзывов из всех файлов и сбор статистики
    for topic in themes:
        if topic in stats_data["topics"]:
            topic_data = stats_data["topics"][topic]
            topic_total_counts[topic] = topic_data["total"]
            for eng_sentiment in ["positive", "neutral", "negative"]:
                if eng_sentiment in topic_data["files"]:
                    file_name = topic_data["files"][eng_sentiment]["file_name"]
                    file_path = os.path.join(input_dir, file_name)
                    if os.path.exists(file_path):
                        with open(file_path, 'r', encoding='utf-8') as f:
                            for line in f:
                                if line.strip():
                                    review = json.loads(line.strip())
                                    text = review.get('review_text', '').strip()
                                    source = review.get('source', '').strip()  # Extract source
                                    rating = review.get('rating', '')
                                    review_date = review.get('review_date', '')
                                    if text and source:
                                        reviews_by_theme_sentiment[
                                            (topic, eng_sentiment)].append(
                                                {"text": text, "source": source, "rating": rating, "review_date": review_date})
                        count = len(reviews_by_theme_sentiment[(topic, eng_sentiment)])
                        topic_sentiment_counts[topic][eng_sentiment] = count
    
    # Общий итог отзывов
    total_reviews = sum(topic_total_counts.values())
    
    # Веса для тем (пропорции)
    theme_weights = [topic_total_counts.get(theme, 0) / total_reviews if total_reviews > 0 else 0 for theme in themes]
    
    # Для каждой темы веса сентиментов
    sentiment_weights_by_theme = {}
    for theme in themes:
        total_sent = sum(topic_sentiment_counts[theme].values())
        sentiment_weights_by_theme[theme] = [
            topic_sentiment_counts[theme].get("positive", 0) / total_sent if total_sent > 0 else 0,
            topic_sentiment_counts[theme].get("neutral", 0) / total_sent if total_sent > 0 else 0,
            topic_sentiment_counts[theme].get("negative", 0) / total_sent if total_sent > 0 else 0
        ]
    
    # Длины списков для расчета циклов
    list_lengths = {key: len(reviews_by_theme_sentiment[key]) for key in reviews_by_theme_sentiment if reviews_by_theme_sentiment[key]}
    
    return reviews_by_theme_sentiment, theme_weights, sentiment_weights_by_theme, list_lengths, themes

def generate_multi_label_review(reviews_by_theme_sentiment, theme_weights, sentiment_weights_by_theme, iterators, usage_counts, list_lengths, themes, review_id):
    # Определение количества тем (1-4)
    num_themes = random.randint(1, 4)
    
    # Выбор уникальных тем с учетом весов
    selected_themes = set()
    while len(selected_themes) < num_themes:
        theme = random.choices(themes, weights=theme_weights, k=1)[0]
        selected_themes.add(theme)
    selected_themes = list(selected_themes)
    
    # Сбор фрагментов, меток и источников
    fragments = []
    topics = []
    sentiments = []
    sources = []
    ratings = []
    review_dates = []
    
    for theme in selected_themes:
        # Выбор сентимента с учетом весов для темы
        sentiment_probs = sentiment_weights_by_theme[theme]
        eng_sentiment = random.choices(["positive", "neutral", "negative"], weights=sentiment_probs, k=1)[0]
        
        key = (theme, eng_sentiment)
        if key in reviews_by_theme_sentiment and reviews_by_theme_sentiment[key]:
            # Получаем итератор и берем следующий отзыв
            iterator = iterators[key]
            review_data = next(iterator)
            fragment = review_data["text"]
            source = review_data["source"]
            rating = review_data["rating"]
            review_date = review_data["review_date"]
            
            # Увеличиваем счетчик использования
            usage_counts[key] += 1
            
        else:
            # Если нет отзывов, пропускаем
            continue
        
        fragments.append(fragment)
        topics.append(theme)
        sentiments.append(sentiments_map[eng_sentiment])
        sources.append(source)
        ratings.append(rating)
        review_dates.append(review_date)
    
    if not fragments:
        return None  # Если ничего не собрали, пропустить
    
    # Объединение фрагментов случайным символом
    join_char = random.choice(join_chars)
    full_text = f" {join_char} ".join(fragments)
    
    return {
        "data": {
            "id": review_id,
            "text": full_text
        },
        "predictions": {
            "id": review_id,
            "topics": topics,
            "sentiments": sentiments,
            "sources": sources,
            "review_dates:": review_dates,
            "ratings": ratings
        }
    }

def main(stats_file, input_dir, output_dir, num_reviews=1000):
    # Загрузка данных
    reviews_by_theme_sentiment, theme_weights, sentiment_weights_by_theme, list_lengths, themes = load_reviews(stats_file, input_dir)
    
    # Итераторы для цикличного обхода
    iterators = {}
    usage_counts = defaultdict(int)
    for key in reviews_by_theme_sentiment:
        if reviews_by_theme_sentiment[key]:
            iterators[key] = itertools.cycle(reviews_by_theme_sentiment[key])
    
    # Статистика
    stats = {
        "total_generated": 0,
        "combinations": defaultdict(int),
        "topics": defaultdict(lambda: {"total": 0, "sentiments": defaultdict(int)}),
        "cycle_counts": defaultdict(lambda: defaultdict(int))
    }
    
    # Создание выходной директории
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'generated_multi_label_reviews.jsonl')
    
    generated_reviews = []
    review_id = 1
    while len(generated_reviews) < num_reviews:
        review = generate_multi_label_review(
            reviews_by_theme_sentiment, theme_weights, sentiment_weights_by_theme, iterators, usage_counts, list_lengths, themes, review_id
        )
        if review:
            generated_reviews.append(review)
            stats["total_generated"] += 1
            
            # Обновление статистики
            topics = tuple(sorted(review["predictions"]["topics"]))
            stats["combinations"][str(topics)] += 1
            
            for i, topic in enumerate(review["predictions"]["topics"]):
                sentiment = review["predictions"]["sentiments"][i]
                stats["topics"][topic]["total"] += 1
                stats["topics"][topic]["sentiments"][sentiment] += 1
            
            review_id += 1
    
    # Расчет cycle_counts
    for key in usage_counts:
        theme, eng_sent = key
        if list_lengths.get(key, 0) > 0:
            stats["cycle_counts"][theme][sentiments_map[eng_sent]] = usage_counts[key] // list_lengths[key]
    
    # Сохранение отзывов
    with open(output_file, 'w', encoding='utf-8') as f:
        for review in generated_reviews:
            f.write(json.dumps(review, ensure_ascii=False) + '\n')
    
    # Сохранение статистики
    stats_file_out = os.path.join(output_dir, 'generated_stats.json')
    with open(stats_file_out, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=4)
    
    print(f"Сгенерировано {num_reviews} отзывов и сохранено в {output_file}")
    print(f"Статистика сохранена в {stats_file_out}")

if __name__ == "__main__":
    stats_file = 'data\\prepared\\splited\\_reviews_stats.json'
    input_dir = 'data\\prepared\\splited\\'
    output_dir = 'data\\generated\\'
    main(stats_file, input_dir, output_dir, num_reviews=500_000)