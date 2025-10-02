import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scipy.linalg import svd
from collections import Counter
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import pymorphy3
import torch
import os
import json
from sklearn.metrics import silhouette_score
from sklearn.manifold import TSNE
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import gensim
from gensim import corpora
from gensim.models import LdaModel, CoherenceModel
import hdbscan

# Логирование CUDA
print(f"CUDA available: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"GPU device: {torch.cuda.get_device_name(0)}")
    torch.cuda.set_device(0)
else:
    print("Warning: Falling back to CPU")

# Определение директории вывода для TF-IDF
output_dir = '/app/tf-idf/data/clusters'
os.makedirs(output_dir, exist_ok=True)

# Полный список стоп-слов из NLTK + расширенный банковский
russian_stop_words = set(stopwords.words('russian'))
russian_stop_words.update(['банк', 'газпромбанк', 'рубль', 'рублей', 'газпром', 'банковский', 'клиентский', 'это', 'который', 'мой'])

# Инициализация pymorphy3
morph = pymorphy3.MorphAnalyzer()

# Загрузка данных с обработкой JSONL
file_path = '/app/tf-idf/data/prepared/common/gazprom_reviews.jsonl'
if not os.path.exists(file_path):
    raise FileNotFoundError(f"Файл не найден: {file_path}")

data = []
with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:  # Пропускаем пустые строки
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Ошибка парсинга строки: {line[:50]}... (ошибка: {e})")
                continue

if not data:
    raise ValueError("Файл пуст или содержит только некорректные данные.")

df = pd.DataFrame(data)

def map_sentiment(rating):
    if rating is None or rating == 0:
        return 'unknown'
    elif rating <= 2:
        return 'negative'
    elif rating == 3:
        return 'neutral'
    else:
        return 'positive'

df['sentiment'] = df['rating'].apply(map_sentiment)

df_negative = df[df['sentiment'] == 'negative']
df_positive = df[df['sentiment'] == 'positive']

# Функция для обработки подмножества (для positive/negative)
def process_subset(df_subset, subset_name):
    # Стратифицированная выборка по 'topic' для баланса
    if 'topic' in df_subset.columns and len(df_subset) > 10000:
        df_sample = df_subset.groupby('topic', group_keys=False).apply(
            lambda x: x.sample(min(len(x), 1000)), include_groups=False
        ).reset_index(drop=True)
    else:
        df_sample = df_subset.sample(n=min(10000, len(df_subset))) if len(df_subset) > 10000 else df_subset

    # Проверка на пустой df_sample
    if df_sample.empty:
        print(f"Ошибка: Подмножество {subset_name} пустое после выборки. Прерывание.")
        return

    # Предобработка с лемматизацией и metadata
    def preprocess(text, theme='', topic=''):
        if not isinstance(text, str) or len(text.split()) < 10:
            return ''
        # metadata
        enhanced_text = f"{theme} {topic} {text}".lower()
        enhanced_text = re.sub(r'[\d\W_]+', ' ', enhanced_text)
        tokens = []
        for word in enhanced_text.split():
            if word not in russian_stop_words and len(word) > 2:
                p = morph.parse(word)[0]
                if 'NOUN' in p.tag or 'ADJF' in p.tag:  # Только существительные и прилагательные
                    lemma = p.normal_form
                    tokens.append(lemma)
        bigrams = ['_'.join(tokens[i:i+2]) for i in range(len(tokens)-1) if len(tokens[i:i+2]) == 2]
        trigrams = ['_'.join(tokens[i:i+3]) for i in range(len(tokens)-2) if len(tokens[i:i+3]) == 3]
        return ' '.join(tokens + bigrams + trigrams)

    df_sample['processed_text'] = df_sample.apply(lambda row: preprocess(row['review_text'], row.get('review_theme', ''), row.get('topic', '')), axis=1)

    def detect_product(text):
        if not isinstance(text, str) or not text:
            return ['other']
        text_lower = text.lower()
        products = []
        if 'ипотека' in text_lower:
            products.append('hypothec')
        if 'вклад' in text_lower or 'накопительный' in text_lower:
            products.append('deposits')
        if 'кредит' in text_lower:
            products.append('credits')
        if 'автокредит' in text_lower:
            products.append('autocredits')
        if 'перевод' in text_lower or 'сбп' in text_lower:
            products.append('transfers')
        if 'приложение' in text_lower:
            products.append('mobile_app')
        if 'карта' in text_lower:
            products.append('cards')
        return products if products else ['other']

    df_sample['detected_product'] = df_sample['processed_text'].apply(detect_product)

    all_words = ' '.join(df_sample['processed_text'].dropna()).split()
    word_freq = Counter(all_words)
    vocab = [word for word, freq in word_freq.most_common(10000)]
    vocab_size = len(vocab)
    word_to_idx = {word: idx for idx, word in enumerate(vocab)}

    rows, cols, data = [], [], []
    for i, text in enumerate(df_sample['processed_text'].dropna()):
        if text:
            word_counts = Counter(text.split())
            total_words = sum(word_counts.values())
            for word, count in word_counts.items():
                if word in word_to_idx:
                    rows.append(i)
                    cols.append(word_to_idx[word])
                    data.append(count / total_words)

    tf_matrix = csr_matrix((data, (rows, cols)), shape=(len(df_sample['processed_text'].dropna()), vocab_size))
    doc_freq = np.array((tf_matrix > 0).sum(axis=0)).squeeze()
    idf = np.log(len(df_sample['processed_text'].dropna()) / (1 + doc_freq))
    tfidf_matrix = tf_matrix.multiply(idf)

    U, S, Vt = svd(tfidf_matrix.toarray(), full_matrices=False)
    k = 200
    reduced_matrix = U[:, :k] * S[:k]

    texts = [text.split() for text in df_sample['processed_text'].dropna() if text]
    if len(texts) != len(df_sample['processed_text'].dropna()):
        print(f"Предупреждение: Количество текстов после фильтрации ({len(texts)}) не совпадает с ожидаемым ({len(df_sample['processed_text'].dropna())}).")
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    lda_model = LdaModel(corpus, num_topics=15, id2word=dictionary, passes=10)  # 15 тем для разнообразия
    lda_topics = np.array([lda_model.get_document_topics(bow, minimum_probability=0) for bow in corpus])
    
    # Синхронизация размеров
    n_samples = len(df_sample['processed_text'].dropna())
    lda_features = np.zeros((n_samples, lda_model.num_topics))
    for i, topics in enumerate(lda_topics):
        for topic, prob in topics:
            lda_features[i, topic] = prob
    if len(lda_features) != len(reduced_matrix):
        print(f"Ошибка: Несоответствие размеров - reduced_matrix: {len(reduced_matrix)}, lda_features: {len(lda_features)}")
        if len(lda_features) < len(reduced_matrix):
            lda_features = np.vstack((lda_features, np.zeros((len(reduced_matrix) - len(lda_features), lda_model.num_topics))))
        elif len(lda_features) > len(reduced_matrix):
            lda_features = lda_features[:len(reduced_matrix)]

    combined_features = np.hstack((reduced_matrix, lda_features))

    sil_scores = []
    clusterers = []
    min_cluster_sizes = range(10, 50, 5)  # Для HDBSCAN
    for min_size in min_cluster_sizes:
        clusterer = hdbscan.HDBSCAN(min_cluster_size=min_size)
        labels = clusterer.fit_predict(combined_features)
        if len(set(labels)) > 1:  # Избегаем одного кластера
            sil = silhouette_score(combined_features, labels)
            sil_scores.append(sil)
            clusterers.append(clusterer)
        else:
            sil_scores.append(0)

    optimal_idx = np.argmax(sil_scores)
    optimal_clusterer = clusterers[optimal_idx]
    df_sample['cluster'] = optimal_clusterer.labels_

    coherence_model = CoherenceModel(model=lda_model, texts=texts, dictionary=dictionary, coherence='c_v')
    coherence_score = coherence_model.get_coherence()
    print(f"Coherence score for {subset_name}: {coherence_score}")

    themes = {}
    product_classes = ["cards", "credits", "autocredits", "deposits", "hypothec", "remote", "restructing", "transfers", "mobile_app", "individual", "other"]
    keyword_map = {
        "cards": ["карта", "дебет", "кредитный", "дебетовый", "карта_union"],
        "credits": ["кредит", "займ", "платёж", "задолженность"],
        "autocredits": ["автокредит"],
        "deposits": ["вклад", "накопительный", "счёт", "ставка", "процент"],
        "hypothec": ["ипотека", "рефинансирование"],
        "remote": ["онлайн", "приложение", "сайт", "поддержка"],
        "restructing": ["реструктуризация", "пересмотр"],
        "transfers": ["перевод", "сбп", "комиссия"],
        "mobile_app": ["приложение", "мобильный", "интерфейс"],
        "individual": ["зарплатный", "клиент", "премиальный"]
    }

    vectorizer = TfidfVectorizer()
    product_vectors = vectorizer.fit_transform([' '.join(keywords) for keywords in keyword_map.values()])
    class_mapping = {}

    for cluster in set(df_sample['cluster']):
        if cluster == -1:  # Шум в HDBSCAN
            continue
        cluster_texts = df_sample[df_sample['cluster'] == cluster]['processed_text']
        if not cluster_texts.empty:
            all_cluster_words = ' '.join(cluster_texts.dropna()).split()
            top_words = Counter(all_cluster_words).most_common(20)
            themes[cluster] = top_words
            top_str = ' '.join([w[0] for w in top_words])
            cluster_vector = vectorizer.transform([top_str])
            similarities = cosine_similarity(cluster_vector, product_vectors)[0]
            best_idx = np.argmax(similarities)
            best_class = list(keyword_map.keys())[best_idx]
            if similarities[best_idx] > 0.2:
                class_mapping[cluster] = best_class
            else:
                fallback = df_sample[df_sample['cluster'] == cluster]['detected_product'].explode().mode()[0]
                class_mapping[cluster] = fallback

    print(f"Выявленные классы для {subset_name}:")
    for cluster, cls in class_mapping.items():
        print(f"Кластер {cluster}: {cls} ({', '.join([w[0] for w in themes[cluster]])})")

    # Визуализация: t-SNE
    tsne = TSNE(n_components=2, random_state=42)
    tsne_results = tsne.fit_transform(combined_features)
    plt.figure(figsize=(10, 5))
    plt.scatter(tsne_results[:, 0], tsne_results[:, 1], c=df_sample['cluster'], cmap='viridis')
    plt.title(f't-SNE visualization for {subset_name}')
    plt.savefig(os.path.join(output_dir, f'tsne_{subset_name}.png'))

    # Сохранение результатов
    df_sample.to_csv(os.path.join(output_dir, f'clustered_reviews_{subset_name}.csv'), index=False)
    product_distribution = df_sample.groupby('cluster')['detected_product'].apply(lambda x: x.explode().value_counts()).unstack(fill_value=0)
    product_distribution.to_csv(os.path.join(output_dir, f'product_distribution_{subset_name}.csv'))

    if themes:
        wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(dict(themes[0]))
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.savefig(os.path.join(output_dir, f'theme_wordcloud_{subset_name}.png'))
        plt.show()
# Обработка подмножеств
process_subset(df_negative, 'negative')
process_subset(df_positive, 'positive')