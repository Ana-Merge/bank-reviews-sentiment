# preprocess_single_review.py
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pymorphy3

nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)
nltk.download('stopwords', quiet=True)

morph = pymorphy3.MorphAnalyzer()
custom_stop_words = {'банка', 'банке', 'банк', 'газпром', 'газпромбанк', 'руб', 'рублей', 'деньги', 'санкт',
                     'счет', 'счета', 'альфа', 'втб', 'сбер', 'тинькоф', 'мтс', 'спб', 'гпб', 'газпром', 'санкт', '\\*'}
to_replace = ['(', ')', '«', '»', '`', '\'', '№']
russian_stopwords = set(stopwords.words('russian'))

def preprocess_text(text):
    for char in to_replace:
        text = text.replace(char, ' ')
    
    def replace_censored(match):
        word = match.group(0)
        word_clean = word.replace('\\', '')
        if any(char.isdigit() for char in word_clean):
            return ''
        return '*'
    
    text = re.sub(r'[а-яА-Яa-zA-Z]+(?:\\*\*+)+[а-яА-Яa-zA-Z]*', replace_censored, text, flags=re.IGNORECASE)
    
    text = text.lower()
    tokens = word_tokenize(text)
    lemmatized_tokens = []
    for token in tokens:
        lemma = morph.parse(token)[0].normal_form
        if (lemma not in russian_stopwords and 
            not any(stop in lemma for stop in custom_stop_words) and 
            not lemma.isdigit() and 
            not any(char.isdigit() for char in lemma)):
            lemmatized_tokens.append(lemma)
    processed_text = ' '.join(lemmatized_tokens)
    if not processed_text.strip():
        print(f"Warning: Text became empty after preprocessing for review: original='{text[:50]}...'")
    return processed_text