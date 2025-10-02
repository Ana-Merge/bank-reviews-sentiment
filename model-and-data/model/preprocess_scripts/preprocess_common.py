import json
import re
import os
from collections import defaultdict
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pymorphy3
import random
import string

# Загрузка необходимых данных NLTK
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')

# Инициализация лемматизатора
morph = pymorphy3.MorphAnalyzer()

# Определение пользовательских стоп-слов для проверки как подстрок
custom_stop_words = {'банка', 'банке', 'банк', 'газпром', 'газпромбанк', 'руб', 'рублей', 'деньги', 'санкт',
                     'счет', 'счета', 'альфа', 'втб', 'сбер', 'тинькоф', 'мтс', 'спб', 'гпб', 'газпром', 'санкт', '\\*'}
to_replace = list(string.punctuation) + ['(', ')', '«', '»', '`', '\'', '№', '“', '”', '‘', '’', '—', '–', '…']

# Загрузка стоп-слов на русском языке
russian_stopwords = set(stopwords.words('russian'))

def preprocess_text(text):
    # Замена всех символов из to_replace на пробелы
    for char in to_replace:
        text = text.replace(char, ' ')
    
    # Обработка зацензурированных слов
    def replace_censored(match):
        word = match.group(0)
        # Удаляем экранирование и проверяем наличие цифр
        word_clean = word.replace('\\', '')
        if any(char.isdigit() for char in word_clean):
            return ''  # Удаляем слово, если есть цифры
        return '*'  # Заменяем на "*" если нет цифр
    
    # Расширенное регулярное выражение для зацензурированных слов
    # Ищет буквы (с учетом регистра) + любые комбинации * или \* + (опционально) буквы
    text = re.sub(r'[а-яА-Яa-zA-Z]+(?:\\*\*+)+[а-яА-Яa-zA-Z]*', replace_censored, text, flags=re.IGNORECASE)
    
    # Приведение текста к нижнему регистру
    text = text.lower()
    # Токенизация текста
    tokens = word_tokenize(text)
    # Лемматизация и фильтрация стоп-слов, подстрок и чисел
    lemmatized_tokens = []
    for token in tokens:
        lemma = morph.parse(token)[0].normal_form
        # Проверяем на стандартные стоп-слова, пользовательские подстроки и числа
        if (lemma not in russian_stopwords and 
            not any(stop in lemma for stop in custom_stop_words) and 
            not lemma.isdigit() and 
            not any(char.isdigit() for char in lemma)):
            lemmatized_tokens.append(lemma)
    # Объединение токенов обратно в текст
    processed_text = ' '.join(lemmatized_tokens)
    if not processed_text.strip():
        print(f"Warning: Text became empty after preprocessing for review: original='{text[:50]}...'")
    # Дополнительная очистка от остаточных символов и лишних пробелов
    for char in to_replace:
        processed_text = processed_text.replace(char, '')
    processed_text = re.sub(r'\s+', ' ', processed_text).strip()  # Удаление лишних пробелов
    return processed_text

def load_reviews(file_path):
    # Чтение отзывов из входного файла с поддержкой UTF-8 с сохранением порядка
    reviews = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                item = json.loads(line.strip())
                data = item.get('data', {})
                predictions = item.get('predictions', {})
                text = data.get('text', '')
                topics = predictions.get('topics', [])
                sentiments = predictions.get('sentiments', [])
                # Проверка соответствия длин
                if len(topics) != len(sentiments):
                    continue  # Пропускаем некорректные записи
                reviews.append({
                    'id': data.get('id'),
                    'text': text,
                    'topics': topics,
                    'sentiments': sentiments,
                    'bank_name': 'Газпромбанк'
                })
    return reviews

def balance_by_category(reviews, max_reviews_per_category=500_000):
    # Группировка отзывов по уникальным комбинациям тем
    topic_comb_reviews = defaultdict(list)
    for review in reviews:
        topic_comb = tuple(sorted(review['topics']))
        topic_comb_reviews[topic_comb].append(review)

    balanced_reviews = []
    for topic_comb, comb_list in topic_comb_reviews.items():
        # Классификация по сентиментам
        positive = [r for r in comb_list if all(s in ['положительная', 'нейтральная'] for s in r['sentiments']) and any(s == 'положительная' for s in r['sentiments'])]
        negative = [r for r in comb_list if any(s == 'негативная' for s in r['sentiments'])]
        neutral = [r for r in comb_list if all(s == 'нейтральная' for s in r['sentiments'])]
        mixed = [r for r in comb_list if r not in positive and r not in negative and r not in neutral]

        # Расчет целевого количества
        total_reviews = len(comb_list)
        if total_reviews <= max_reviews_per_category:
            balanced_reviews.extend(comb_list)
            continue

        # Равномерное обрезание по сентиментам
        excess_reviews = total_reviews - max_reviews_per_category
        total_sentiments = len(positive) + len(negative) + len(neutral) + len(mixed)
        if total_sentiments == 0:
            continue

        # Целевые количества для каждой категории
        pos_target = max(0, len(positive) - (excess_reviews * (len(positive) / total_sentiments)))
        neg_target = max(0, len(negative) - (excess_reviews * (len(negative) / total_sentiments)))
        neu_target = max(0, len(neutral) - (excess_reviews * (len(neutral) / total_sentiments)))
        mix_target = max(0, len(mixed) - (excess_reviews * (len(mixed) / total_sentiments)))

        # Детерминированная выборка
        sampled_positive = positive[:int(pos_target)]
        sampled_negative = negative[:int(neg_target)]
        sampled_neutral = neutral[:int(neu_target)]
        sampled_mixed = mixed[:int(mix_target)]

        # Объединение выборок
        sampled = sampled_positive + sampled_negative + sampled_neutral + sampled_mixed
        balanced_reviews.extend(sampled)

    # Общая обрезка до максимального числа, если нужно
    if len(balanced_reviews) > max_reviews_per_category:
        balanced_reviews = balanced_reviews[:max_reviews_per_category]

    return balanced_reviews

def calculate_statistics(reviews):
    # Подсчет статистики по темам и сентиментам (мультилейбл)
    stats = {
        'total_reviews': len(reviews),
        'topics': defaultdict(lambda: {'count': 0, 'sentiments': defaultdict(int)})
    }

    for review in reviews:
        for topic, sentiment in zip(review['topics'], review['sentiments']):
            stats['topics'][topic]['count'] += 1
            stats['topics'][topic]['sentiments'][sentiment] += 1

    return stats

def save_processed_reviews(reviews, output_file):
    # Создание директории, если она не существует, и запись обработанных отзывов
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        for review in reviews:
            processed_text = preprocess_text(review['text'])
            processed_review = {
                'data': {
                    'id': review['id'],
                    'text': processed_text  # Сохраняем даже пустой текст
                },
                'predictions': {
                    'id': review['id'],
                    'topics': review['topics'],
                    'sentiments': review['sentiments']
                }
            }
            f.write(json.dumps(processed_review, ensure_ascii=False) + '\n')

def save_statistics(stats, output_file):
    # Сохранение файла со статистикой
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def main():
    # Определение путей и параметров
    input_file = 'data/generated/generated_multi_label_reviews.jsonl'
    output_dir = 'data/generated/processed'
    output_file = os.path.join(output_dir, 'processed_multi_label_reviews_50k.jsonl')
    stats_initial_file = os.path.join(output_dir, 'statistics_initial.json')
    stats_final_file = os.path.join(output_dir, 'statistics_final.json')

    # Загрузка, обработка и сохранение отзывов
    reviews = load_reviews(input_file)
    balanced_reviews = balance_by_category(reviews)
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