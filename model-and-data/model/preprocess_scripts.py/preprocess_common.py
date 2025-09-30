import json
import re
import os
from collections import defaultdict
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import random
import pymorphy3

# Загрузка необходимых данных NLTK
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')

# Инициализация лемматизатора
morph = pymorphy3.MorphAnalyzer()

# Определение пользовательских стоп-слов для проверки как подстрок
custom_stop_words = {'банка', 'банке', 'банк', 'газпром', 'газпромбанк', 'руб', 'рублей', 'деньги', 'счет', 'счета'}
stop_words_pattern = r'(?:\s|^)(?:' + '|'.join(map(re.escape, custom_stop_words)) + r')(?=\s|$)'

# Загрузка стоп-слов на русском языке
russian_stopwords = set(stopwords.words('Russian'))

def preprocess_text(text):
    # Приведение текста к нижнему регистру
    text = text.lower()
    # Токенизация текста с сохранением пунктуации
    tokens = word_tokenize(text)
    # Лемматизация и фильтрация стоп-слов с учетом подстрок
    lemmatized_tokens = []
    for token in tokens:
        lemma = morph.parse(token)[0].normal_form
        # Проверка на наличие стоп-слов как подстрок
        if not re.search(stop_words_pattern, ' ' + lemma + ' '):
            lemmatized_tokens.append(token)  # Сохраняем оригинальный токен с пунктуацией
    # Объединение токенов обратно в текст
    text = ' '.join(lemmatized_tokens)
    return text

def load_reviews(file_path):
    # Чтение отзывов из входного файла с поддержкой UTF-8
    reviews = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            review = json.loads(line.strip())
            # Определение сентимента и сохранение рейтинга
            rating = review.get('rating', 0)
            if rating <= 2:
                sentiment = 'negative'
            elif rating == 3:
                sentiment = 'neutral'
            elif rating >= 4:
                sentiment = 'positive'
            else:
                sentiment = 'unknown'
            reviews.append({
                'text': review.get('review_text', ''),
                'topic': review.get('topic', 'other'),
                'sentiment': sentiment,
                'bank_name': review.get('bank_name', 'Unknown'),
                'rating': rating  # Сохраняем рейтинг для расчета средней оценки
            })
    return reviews

def balance_by_category(reviews, max_reviews_per_category=100):
    # Группировка отзывов по темам
    topic_reviews = defaultdict(list)
    for review in reviews:
        topic_reviews[review['topic']].append(review)

    balanced_reviews = []
    for topic, topic_list in topic_reviews.items():
        # Разделение на положительные, отрицательные, нейтральные и неизвестные отзывы
        positive = [r for r in topic_list if r['sentiment'] == 'positive']
        negative = [r for r in topic_list if r['sentiment'] == 'negative']
        neutral = [r for r in topic_list if r['sentiment'] == 'neutral']
        unknown = [r for r in topic_list if r['sentiment'] == 'unknown']

        # Расчет целевого количества для достижения примерно 50% положительных и отрицательных
        total_target = min(max_reviews_per_category, len(topic_list))
        pos_neg_target = total_target // 2
        neutral_target = total_target - (pos_neg_target * 2)

        # Выборка с учетом баланса
        sampled_positive = random.sample(positive, min(pos_neg_target, len(positive))) if positive else []
        sampled_negative = random.sample(negative, min(pos_neg_target, len(negative))) if negative else []
        sampled_neutral = random.sample(neutral, min(neutral_target, len(neutral))) if neutral else []
        sampled_unknown = random.sample(unknown, min(neutral_target - len(sampled_neutral), len(unknown))) if unknown else []

        # Объединение выборок с ограничением по максимальному количеству
        sampled = sampled_positive + sampled_negative + sampled_neutral + sampled_unknown
        if len(sampled) > max_reviews_per_category:
            sampled = random.sample(sampled, max_reviews_per_category)
        balanced_reviews.extend(sampled)

    return balanced_reviews

def calculate_statistics(reviews):
    # Подсчет статистики по продуктам, банкам и продуктам в банках
    stats = {
        'total_reviews': len(reviews),
        'product_per_bank': defaultdict(lambda: defaultdict(int)),
        'by_bank': defaultdict(lambda: {'count': 0, 'average_rating': 0.0})
    }

    # Подсчет количества отзывов по продуктам для каждого банка
    for review in reviews:
        topic = review['topic']
        bank = review['bank_name']
        rating = review['rating']

        # Статистика по продуктам в банках
        stats['product_per_bank'][bank][topic] += 1

        # Статистика по банкам с накоплением рейтингов
        stats['by_bank'][bank]['count'] += 1
        stats['by_bank'][bank]['average_rating'] += rating

    # Расчет средней оценки для каждого банка
    for bank in stats['by_bank']:
        total_count = stats['by_bank'][bank]['count']
        stats['by_bank'][bank]['average_rating'] = (stats['by_bank'][bank]['average_rating'] / total_count) if total_count > 0 else 0.0

    return stats

def save_processed_reviews(reviews, output_file):
    # Создание директории, если она не существует, и запись обработанных отзывов
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        for review in reviews:
            processed_review = {
                'text': preprocess_text(review['text']),
                'topic': review['topic'],  # Явное сохранение темы
                'sentiment': review['sentiment'],
                'bank_name': review['bank_name']
            }
            f.write(json.dumps(processed_review, ensure_ascii=False) + '\n')

def save_statistics(stats, output_file):
    # Сохранение файла со статистикой
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def main():
    # Определение путей и параметров
    input_file = 'data/prepared/common/all_reviews.jsonl'
    output_dir = 'data/preprocessed/common'  # Пользовательская директория для выходных файлов
    output_file = os.path.join(output_dir, 'processed_reviews.jsonl')
    stats_initial_file = os.path.join(output_dir, 'statistics_initial.json')
    stats_final_file = os.path.join(output_dir, 'statistics_final.json')

    # Загрузка, обработка и сохранение отзывов
    reviews = load_reviews(input_file)
    balanced_reviews = balance_by_category(reviews)
    random.shuffle(balanced_reviews)
    save_processed_reviews(balanced_reviews, output_file)

    # Расчет и сохранение статистики для исходного и конечного набора
    initial_stats = calculate_statistics(reviews)
    final_stats = calculate_statistics(balanced_reviews)
    save_statistics(initial_stats, stats_initial_file)
    save_statistics(final_stats, stats_final_file)

    print(f"Обработано {len(balanced_reviews)} отзывов и сохранено в {output_file}")
    print(f"Начальная статистика сохранена в {stats_initial_file}")
    print(f"Конечная статистика сохранена в {stats_final_file}")

if __name__ == "__main__":
    main()