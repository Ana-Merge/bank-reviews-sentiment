# Импорт NLTK для стоп-слов
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

# Логирование CUDA
print(f"CUDA available: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"GPU device: {torch.cuda.get_device_name(0)}")
    torch.cuda.set_device(0)  # Явно выбираем первое устройство
else:
    print("Warning: Falling back to CPU")

# Полный список стоп-слов из NLTK + расширенный (без "карта" и "карты")
russian_stop_words = set(stopwords.words('russian'))
russian_stop_words.update(['банк', 'газпромбанк', 'рубль', 'рублей'])

# Инициализация pymorphy3
morph = pymorphy3.MorphAnalyzer()

# Загрузка данных
file_path = '/app/data/processed/common/gazprom_reviews.jsonl'
df = pd.read_json(file_path, lines=True)
df_sample = df.sample(n=5000)  # Уменьшен для теста

# Предобработка с лемматизацией
def preprocess(text):
    if not isinstance(text, str) or len(text.split()) < 10:
        return ''
    text = re.sub(r'[\d\W_]+', ' ', text.lower())
    tokens = []
    for word in text.split():
        if word not in russian_stop_words and len(word) > 2:
            lemma = morph.parse(word)[0].normal_form
            tokens.append(lemma)
    bigrams = ['_'.join(tokens[i:i+2]) for i in range(len(tokens)-1) if len(tokens[i:i+2]) == 2]
    return ' '.join(tokens + bigrams)

df_sample['processed_text'] = df_sample['review_text'].apply(preprocess)

# Определение продукта
def detect_product(text):
    if not text:
        return 'unknown'
    text_lower = text.lower()
    if 'кредит' in text_lower or 'займ' in text_lower:
        return 'credits'
    elif 'вклад' in text_lower or 'сбережение' in text_lower:
        return 'deposits'
    elif 'карта' in text_lower and ('дебет' in text_lower or 'привилегия' in text_lower):
        return 'debitcards'
    elif 'карта' in text_lower and 'кредит' in text_lower:
        return 'creditcards'
    elif 'ипотека' in text_lower:
        return 'hypothec'
    elif 'автокредит' in text_lower:
        return 'autocredits'
    elif 'мобильный' in text_lower or 'приложение' in text_lower:
        return 'mobile_app'
    return 'other'

df_sample['detected_product'] = df_sample['processed_text'].apply(detect_product)

# TF-IDF с sparse матрицей
all_words = ' '.join(df_sample['processed_text']).split()
word_freq = Counter(all_words)
vocab = [word for word, freq in word_freq.most_common(2500)]  # Уменьшен для теста
vocab_size = len(vocab)
word_to_idx = {word: idx for idx, word in enumerate(vocab)}

rows, cols, data = [], [], []
for i, text in enumerate(df_sample['processed_text']):
    if text:
        word_counts = Counter(text.split())
        total_words = sum(word_counts.values())
        for word, count in word_counts.items():
            if word in word_to_idx:
                rows.append(i)
                cols.append(word_to_idx[word])
                data.append(count / total_words)

tf_matrix = csr_matrix((data, (rows, cols)), shape=(len(df_sample), vocab_size))
doc_freq = np.array((tf_matrix > 0).sum(axis=0)).squeeze()
idf = np.log(len(df_sample) / (1 + doc_freq))
tfidf_matrix = tf_matrix.multiply(idf)

# SVD на CPU
U, S, Vt = svd(tfidf_matrix.toarray(), full_matrices=False)
k = 50  # Уменьшен для теста
reduced_matrix = U[:, :k] * S[:k]

# Перенос на GPU
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")
reduced_tensor = torch.tensor(reduced_matrix, dtype=torch.float32).to(device)

# K-Means на GPU с PyTorch
def kmeans_torch(data, num_clusters, max_iter=100, tol=1e-4):
    data = data.to(device)
    centroids = data[torch.randperm(data.size(0))[:num_clusters]]
    for i in range(max_iter):
        distances = torch.cdist(data, centroids)
        labels = torch.argmin(distances, dim=1)
        new_centroids = torch.stack([data[labels == i].mean(dim=0) for i in range(num_clusters)])
        if torch.all(torch.abs(centroids - new_centroids) < tol):
            break
        centroids = new_centroids
    return labels.cpu().numpy(), centroids.cpu().numpy()

num_clusters = 20
cluster_labels, centroids = kmeans_torch(reduced_tensor, num_clusters)
df_sample['cluster'] = cluster_labels

# Темы
themes = {}
for cluster in range(num_clusters):
    cluster_texts = df_sample[df_sample['cluster'] == cluster]['processed_text']
    if not cluster_texts.empty:
        all_cluster_words = ' '.join(cluster_texts.dropna()).split()
        top_words = Counter(all_cluster_words).most_common(20)
        themes[cluster] = top_words

print("Выявленные тематики (кластеры):")
for cluster, words in themes.items():
    print(f"Кластер {cluster}: {', '.join([w[0] for w in words])}")

# Сохранение результатов
output_dir = '/app/data/clusters'
os.makedirs(output_dir, exist_ok=True)
df_sample.to_csv(os.path.join(output_dir, 'clustered_reviews_improved.csv'), index=False)
product_distribution = df_sample.groupby('cluster')['detected_product'].value_counts().unstack(fill_value=0)
product_distribution.to_csv(os.path.join(output_dir, 'product_distribution.csv'))

if themes:
    wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(dict(themes[0]))
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.savefig(os.path.join(output_dir, 'theme_wordcloud_improved.png'))
    plt.show()