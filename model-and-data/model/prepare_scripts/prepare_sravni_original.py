import pandas as pd
import numpy as np
from datetime import datetime
import re
import os
import json
from datetime import timezone
import pymorphy3
from html import unescape
import warnings
import gc

# Подавление предупреждения о pkg_resources (если нужно)
warnings.filterwarnings("ignore", category=UserWarning, module="pkg_resources")

# Инициализация лемматизатора
morph = pymorphy3.MorphAnalyzer()

# Шаг 1: Подготовка данных
# Этот скрипт загружает данные из JSON-файлов, фильтрует по дате,
# предобрабатывает текст (с лемматизацией) и сохраняет только необходимые поля.

# 1.1. Определение дерева тегов
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

# Создаем обратный mapping для specificProductName/reviewTag -> top_tag
product_to_tag = {}
for tag, products in tag_tree.items():
    if products:
        for prod in products:
            product_to_tag[prod.lower()] = tag
    product_to_tag[tag.lower()] = tag

# 1.2. Функция для предобработки текста с лемматизацией
def preprocess_text(text):
    if not isinstance(text, str):
        return ''
    text = unescape(text)  # Очистка HTML-entities
    text = text.lower()
    text = re.sub(r'[^a-zа-я0-9\s]', ' ', text)  # Удаление всего кроме букв, цифр, пробелов
    text = re.sub(r'\s+', ' ', text).strip()
    words = text.split()
    lemmatized = [morph.parse(word)[0].normal_form for word in words]
    return ' '.join(lemmatized)

# 1.3. Функция для маппинга сентимента
def map_sentiment(rating):
    if pd.isna(rating) or rating == 0:
        return 'unknown'
    elif rating <= 2:
        return 'отрицательно'
    elif rating == 3:
        return 'нейтрально'
    else:
        return 'положительно'

# Функция для фильтрации DataFrame по параметрам
def filter_data(df, filters=None, start_date=None, end_date=None):
    if filters is None:
        filters = {}
    mask = pd.Series(True, index=df.index)
    for param, values in filters.items():
        allowed = values.get('allowed', [])
        disallowed = values.get('disallowed', [])
        if allowed:
            mask &= df[param].isin(allowed)
        if disallowed:
            mask &= ~df[param].isin(disallowed)
    if start_date:
        mask &= df['date'] >= start_date
    if end_date:
        mask &= df['date'] <= end_date
    return df[mask]

# Функция для подсчета и сохранения статистики
def save_statistics(df, output_path, is_all_banks=False):
    stats = {}
    
    # Статистика по оценкам (rating)
    if 'rating' in df.columns:
        rating_counts = df['rating'].value_counts().to_dict()
        stats['rating_counts'] = rating_counts
    
    # Статистика по тегам (pseudo_topic)
    if 'pseudo_topic' in df.columns:
        topic_counts = df['pseudo_topic'].value_counts().to_dict()
        stats['topic_counts'] = topic_counts
    
    # Если all_banks, статистика по банкам
    if is_all_banks and 'bank_name' in df.columns:
        bank_stats = {}
        for bank in df['bank_name'].unique():
            bank_df = df[df['bank_name'] == bank]
            bank_rating_counts = bank_df['rating'].value_counts().to_dict() if 'rating' in bank_df.columns else {}
            bank_topic_counts = bank_df['pseudo_topic'].value_counts().to_dict() if 'pseudo_topic' in bank_df.columns else {}
            bank_stats[bank] = {
                'rating_counts': bank_rating_counts,
                'topic_counts': bank_topic_counts
            }
        stats['bank_stats'] = bank_stats
    
    # Сохраняем статистику в JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=4)
    
    print(f"Статистика сохранена в {output_path}")

# 1.4. Обработка данных Газпромбанка
def process_gazprom_data(gazprom_file='model/data/reviews/gazprom_reviews.json', filters=None, start_date=None, end_date=None):
    # Загрузка данных Газпромбанка
    with open(gazprom_file, 'r', encoding='utf-8') as f:
        gazprom_data = pd.json_normalize(json.load(f)['items'])
    gazprom_data['date'] = pd.to_datetime(gazprom_data['date'], format='ISO8601', errors='coerce')
    invalid_dates = gazprom_data[gazprom_data['date'].isna()]['date']
    if not invalid_dates.empty:
        print(f"Найдены некорректные даты: {invalid_dates.tolist()}")
    gazprom_data = gazprom_data.dropna(subset=['date'])

    # Заполнение NaN в ключевых столбцах
    gazprom_data['title'] = gazprom_data['title'].fillna('unknown')
    gazprom_data['text'] = gazprom_data['text'].fillna('unknown')
    gazprom_data['rating'] = gazprom_data['rating'].fillna(0)
    gazprom_data['specificProductName'] = gazprom_data['specificProductName'].fillna('unknown')
    gazprom_data['reviewTag'] = gazprom_data['reviewTag'].fillna('unknown')
    gazprom_data['bank_name'] = 'Газпромбанк'
    gazprom_data['isAuthorDeleted'] = gazprom_data['isAuthorDeleted'].fillna(False)
    gazprom_data['isRecommendedByUser'] = gazprom_data['isRecommendedByUser'].fillna(False)
    gazprom_data['userDataStatus'] = gazprom_data['userDataStatus'].fillna('unknown')

    # Обработка данных (лемматизация, сентимент, тема)
    gazprom_data['full_text'] = gazprom_data['title'] + ' ' + gazprom_data['text']
    gazprom_data['preprocessed_text'] = gazprom_data['full_text'].apply(preprocess_text)
    gazprom_data['pseudo_sentiment'] = gazprom_data['rating'].apply(map_sentiment)
    gazprom_data['pseudo_topic'] = gazprom_data['specificProductName'].str.lower().map(product_to_tag).fillna(
        gazprom_data['reviewTag'].str.lower().map(product_to_tag)).fillna('Прочее')
    if 'preprocessed_text' not in gazprom_data.columns:
        print(f"Столбец 'preprocessed_text' отсутствует. Доступные столбцы: {gazprom_data.columns.tolist()}")
        raise KeyError("Отсутствует столбец 'preprocessed_text' после обработки")
    gazprom_data.drop_duplicates(subset=['id', 'preprocessed_text'], inplace=True)
    gazprom_data = gazprom_data[gazprom_data['preprocessed_text'].str.len() > 20]

    # Фильтрация по параметрам (если указаны)
    gazprom_data = filter_data(gazprom_data, filters, start_date, end_date)

    # Фильтрация по дате после обработки (если не указан в filters)
    if start_date is None:
        start_date = datetime(2024, 1, 1).replace(tzinfo=timezone.utc)
    if end_date is None:
        end_date = datetime(2025, 5, 31).replace(tzinfo=timezone.utc)
    df_recent = gazprom_data[(gazprom_data['date'] >= start_date) & (gazprom_data['date'] <= end_date)]
    df_historical = gazprom_data[~gazprom_data.index.isin(df_recent.index)]


    # Новые файлы: with_specificProductName и without_specificProductName
    df_with_specific = gazprom_data[gazprom_data['specificProductName'] != 'unknown']
    df_without_specific = gazprom_data[gazprom_data['specificProductName'] == 'unknown']

    # Создание отдельного файла только с отзывами, где есть оценка (pseudo_sentiment != 'unknown')
    df_with_rating = gazprom_data[gazprom_data['pseudo_sentiment'] != 'unknown']

    # Выбор только необходимых столбцов для основных файлов
    required_columns = ['preprocessed_text', 'pseudo_sentiment', 'pseudo_topic', 'date', 'bank_name']
    # print(f"Столбцы в gazprom_data: {gazprom_data.columns.tolist()}")  # Отладка
    gazprom_data = gazprom_data[required_columns]
    # print(f"Столбцы в df_recent до фильтрации: {df_recent.columns.tolist()}")  # Отладка
    df_recent = df_recent[required_columns]
    df_historical = df_historical[required_columns]
    df_with_specific = df_with_specific[required_columns]
    df_without_specific = df_without_specific[required_columns]
    df_with_rating = df_with_rating[required_columns]  # Для файла с оценками

    # Сохранение результатов
    os.makedirs('model/data/processed', exist_ok=True)
    gazprom_data.to_csv('model/data/processed/processed_gazprom.csv', index=False)
    df_recent.to_csv('model/data/processed/recent_gazprom.csv', index=False)
    df_historical.to_csv('model/data/processed/historical_gazprom.csv', index=False)
    df_with_specific.to_csv('model/data/processed/with_specificProductName_gazprom.csv', index=False)
    df_without_specific.to_csv('model/data/processed/without_specificProductName_gazprom.csv', index=False)
    df_with_rating.to_csv('model/data/processed/processed_gazprom_with_rating.csv', index=False)  # Новый файл

    # Подсчет и сохранение статистики
    save_statistics(gazprom_data, 'model/data/processed/stats_gazprom.json', is_all_banks=False)

    # Очистка памяти
    del gazprom_data, df_recent, df_historical, df_with_specific, df_without_specific, df_with_rating
    gc.collect()


# 1.5. Обработка данных всех банков (с учетом памяти: загрузка чанками, если нужно, но pd.json_normalize обычно справляется)
def process_all_banks_data(all_banks_file='model/data/reviews/all_reviews.json', subsample_all=None, filters=None, start_date=None, end_date=None):
    # Загрузка данных всех банков (для большого файла используем chunks, но json_normalize не поддерживает chunks напрямую,
    # поэтому загружаем весь JSON, но обрабатываем по частям если subsample
    with open(all_banks_file, 'r', encoding='utf-8') as f:
        all_data = json.load(f)['items']
    
    # Если subsample, берем случайную подвыборку
    if subsample_all:
        import random
        random.seed(42)
        all_data = random.sample(all_data, min(subsample_all, len(all_data)))
    
    all_banks_data = pd.json_normalize(all_data)
    del all_data  # Освобождаем память от списка
    gc.collect()
    
    all_banks_data['date'] = pd.to_datetime(all_banks_data['date'], format='ISO8601', errors='coerce')
    invalid_dates_all = all_banks_data[all_banks_data['date'].isna()]['date']
    if not invalid_dates_all.empty:
        print(f"Найдены некорректные даты в all_banks: {invalid_dates_all.tolist()}")
    all_banks_data = all_banks_data.dropna(subset=['date'])

    # Заполнение NaN в all_banks_data
    all_banks_data['title'] = all_banks_data['title'].fillna('unknown')
    all_banks_data['text'] = all_banks_data['text'].fillna('unknown')
    all_banks_data['rating'] = all_banks_data['rating'].fillna(0)
    all_banks_data['specificProductName'] = all_banks_data['specificProductName'].fillna('unknown')
    all_banks_data['reviewTag'] = all_banks_data['reviewTag'].fillna('unknown')
    all_banks_data['bank_name'] = all_banks_data['bank_name'].fillna('Другие банки')
    all_banks_data['isAuthorDeleted'] = all_banks_data['isAuthorDeleted'].fillna(False)
    all_banks_data['isRecommendedByUser'] = all_banks_data['isRecommendedByUser'].fillna(False)
    all_banks_data['userDataStatus'] = all_banks_data['userDataStatus'].fillna('unknown')

    # Обработка данных (чтобы сэкономить память, применяем apply по частям, если нужно, но pandas обычно ок)
    all_banks_data['full_text'] = all_banks_data['title'] + ' ' + all_banks_data['text']
    all_banks_data['preprocessed_text'] = all_banks_data['full_text'].apply(preprocess_text)
    all_banks_data['pseudo_sentiment'] = all_banks_data['rating'].apply(map_sentiment)
    all_banks_data['pseudo_topic'] = all_banks_data['specificProductName'].str.lower().map(product_to_tag).fillna(
        all_banks_data['reviewTag'].str.lower().map(product_to_tag)).fillna('Прочее')
    if 'preprocessed_text' not in all_banks_data.columns:
        print(f"Столбец 'preprocessed_text' отсутствует. Доступные столбцы: {all_banks_data.columns.tolist()}")
        raise KeyError("Отсутствует столбец 'preprocessed_text' после обработки")
    all_banks_data.drop_duplicates(subset=['id', 'preprocessed_text'], inplace=True)
    all_banks_data = all_banks_data[all_banks_data['preprocessed_text'].str.len() > 20]

    # Фильтрация по параметрам (если указаны)
    all_banks_data = filter_data(all_banks_data, filters, start_date, end_date)

    # Создание отдельного файла только с отзывами, где есть оценка (pseudo_sentiment != 'unknown')
    df_with_rating = all_banks_data[all_banks_data['pseudo_sentiment'] != 'unknown']

    # Выбор только необходимых столбцов
    required_columns = ['preprocessed_text', 'pseudo_sentiment', 'pseudo_topic', 'date', 'bank_name']
    all_banks_data = all_banks_data[required_columns]
    df_with_rating = df_with_rating[required_columns]

    # Сохранение результатов
    os.makedirs('model/data/processed', exist_ok=True)
    all_banks_data.to_csv('model/data/processed/aux_all_banks.csv', index=False)
    df_with_rating.to_csv('model/data/processed/aux_all_banks_with_rating.csv', index=False)  # Новый файл

    # Подсчет и сохранение статистики (с groupby, чтобы не дублировать df)
    save_statistics(all_banks_data, 'model/data/processed/stats_all_banks.json', is_all_banks=True)

    # Очистка памяти
    del all_banks_data, df_with_rating
    gc.collect()


# 1.6. Главная функция для запуска
def prepare_data(process_gazprom=True, process_all_banks=True,
                 gazprom_file='model/data/reviews/gazprom_reviews.json',
                 all_banks_file='model/data/reviews/all_reviews.json',
                 subsample_all=None,  # Убрали дефолт 50000, чтобы обработать все, но можно указать для теста
                 filters=None, start_date=None, end_date=None):
    if process_gazprom:
        process_gazprom_data(gazprom_file, filters, start_date, end_date)
    if process_all_banks:
        process_all_banks_data(all_banks_file, subsample_all, filters, start_date, end_date)

if __name__ == "__main__":
    filters = {}  # Пустой, как в оригинале
    prepare_data()