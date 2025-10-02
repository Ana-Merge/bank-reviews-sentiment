# preprocess.py

import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pymorphy3

# Загрузка необходимых данных NLTK
nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)
nltk.download('stopwords', quiet=True)

# Инициализация лемматизатора
morph = pymorphy3.MorphAnalyzer()

# Определение пользовательских стоп-слов для проверки как подстрок
custom_stop_words = {'банка', 'банке', 'банк', 'газпром', 'газпромбанк', 'руб', 'рублей', 'деньги', 'санкт',
                     'счет', 'счета', 'альфа', 'втб', 'сбер', 'тинькоф', 'мтс', 'спб', 'гпб', 'газпром', 'санкт', '\\*'}
to_replace = ['(', ')', '«', '»', '`', '\'', '№']

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