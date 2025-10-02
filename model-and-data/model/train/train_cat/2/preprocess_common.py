import json
import re
import os
from collections import defaultdict
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pymorphy3
import random
import stanza
import spacy
import nlpaug.augmenter.word as naw

nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)
nltk.download('stopwords', quiet=True)

stanza.download('ru')
nlp_stanza = stanza.Pipeline(lang='ru', processors='tokenize,lemma', verbose=False)

try:
    nlp_spacy = spacy.load('ru_core_news_sm')
except OSError:
    os.system("python -m spacy download ru_core_news_sm")
    nlp_spacy = spacy.load('ru_core_news_sm')
except:
    print("SpaCy failed. Using fallback (NLTK + pymorphy3).")
    nlp_spacy = None

aug = naw.BackTranslationAug(
    from_model_name='Helsinki-NLP/opus-mt-ru-en',
    to_model_name='Helsinki-NLP/opus-mt-en-ru'
)

morph = pymorphy3.MorphAnalyzer()

custom_stop_words = {'руб', 'рублей', 'деньги', '\\*'}
to_replace = ['(', ')', '«', '»', '`', '\'', '№']

russian_stopwords = set(stopwords.words('russian'))

def preprocess_text(text):
    original_text = text
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
    
    if nlp_spacy:
        doc = nlp_spacy(text)
        text = ' '.join([ent.text if ent.label_ not in ['PER', 'ORG'] else '[MASK]' for ent in doc.ents])
    
    doc_stanza = nlp_stanza(text)
    tokens = [word.lemma for sent in doc_stanza.sentences for word in sent.words if word.text.isalpha()]
    
    lemmatized_tokens = []
    for token in tokens:
        if token not in russian_stopwords and token not in custom_stop_words and not re.match(r'\d+', token):
            lemmatized_tokens.append(token)
    
    processed_text = ' '.join(lemmatized_tokens)
    if not processed_text.strip():
        print(f"Warning: Empty after advanced preprocess, using fallback: '{original_text[:50]}...'")
        tokens = word_tokenize(text)
        lemmatized_tokens = []
        for token in tokens:
            lemma = morph.parse(token)[0].normal_form
            if lemma not in russian_stopwords and lemma not in custom_stop_words and not lemma.isdigit() and not any(char.isdigit() for char in lemma):
                lemmatized_tokens.append(lemma)
        processed_text = ' '.join(lemmatized_tokens)
    
    processed_text = re.sub(r'\s+', ' ', processed_text).strip()
    
    if random.random() < 0.05 and len(processed_text.split()) > 5:
        try:
            augmented = aug.augment(processed_text)
            if augmented:
                processed_text = augmented[0]
        except Exception as e:
            print(f"Augmentation failed: {e}")
    
    return processed_text

def load_reviews(file_path):
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
                if len(topics) == len(sentiments) and text:
                    reviews.append({
                        'id': data.get('id', 'unknown'),
                        'text': text,
                        'topics': topics,
                        'sentiments': sentiments
                    })
    return reviews

def balance_by_category(reviews, target_size=50000):
    categories = defaultdict(list)
    for review in reviews:
        cat = review['topics'][0] if review['topics'] else 'other'
        categories[cat].append(review)
    
    balanced = []
    num_cats = len(categories)
    for cat, items in categories.items():
        target_per_cat = target_size // num_cats
        if len(items) > target_per_cat:
            balanced.extend(random.sample(items, target_per_cat))
        else:
            balanced.extend(items)
            while len(items) < target_per_cat:
                aug_review = random.choice(items).copy()
                aug_review['text'] = preprocess_text(aug_review['text'])
                balanced.append(aug_review)
                items.append(aug_review)  # To avoid infinite loop if preprocessing fails
    
    random.shuffle(balanced)
    return balanced[:target_size]

def calculate_statistics(reviews):
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
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        for review in reviews:
            processed_text = preprocess_text(review['text'])
            processed_review = {
                'data': {
                    'id': review.get('id', 'unknown'),
                    'text': processed_text,
                    'date': review.get('date', '')
                },
                'predictions': {
                    'id': review.get('id', 'unknown'),
                    'topics': review['topics'],
                    'sentiments': review['sentiments']
                }
            }
            f.write(json.dumps(processed_review, ensure_ascii=False) + '\n')

def save_statistics(stats, output_file):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def main():
    input_file = '/data/generated/generated_multi_label_reviews.jsonl'
    output_dir = '/data/generated/processed'
    output_file = os.path.join(output_dir, 'processed_multi_label_reviews_50k.jsonl')
    stats_initial_file = os.path.join(output_dir, 'statistics_initial.json')
    stats_final_file = os.path.join(output_dir, 'statistics_final.json')

    reviews = load_reviews(input_file)
    balanced_reviews = balance_by_category(reviews)
    save_processed_reviews(balanced_reviews, output_file)

    initial_stats = calculate_statistics(reviews)
    final_stats = calculate_statistics(balanced_reviews)
    save_statistics(initial_stats, stats_initial_file)
    save_statistics(final_stats, stats_final_file)

    print(f"Обработано {len(balanced_reviews)} отзывов в {output_file}")
    print(f"Gini imbalance: {final_stats.get('imbalance_gini', 0):.3f} (ниже 0.5 — хорошо)")

if __name__ == "__main__":
    main()