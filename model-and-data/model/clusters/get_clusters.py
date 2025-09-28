import pandas as pd
import numpy as np
from scipy.cluster.vq import kmeans, vq
from collections import Counter
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import os

# Загрузка и семплирование
file_path = 'data/processed/common/gazprom_reviews.jsonl'
df = pd.read_json(file_path, lines=True)
df_sample = df.sample(n=5000, random_state=42)

# Расширенный список стоп-слов
stop_words = set(['и', 'в', 'на', 'не', 'с', 'по', 'для', 'от', 'у', 'а', 'как', 'что', 'это', 
                  'банк', 'газпромбанка', 'мне', 'меня', 'была', 'было', 'все', 'карта', 'карты',
                  'руб', 'рублей', 'января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 
                  'августа', 'сентября', 'октября', 'ноября', 'декабря'])

# Улучшенная предобработка
def preprocess(text):
    if not isinstance(text, str) or len(text.split()) < 10:  # Фильтр коротких текстов
        return ''
    # Удаление чисел и дат
    text = re.sub(r'\d+', '', text.lower())
    text = re.sub(r'[а-я]+[а-я]\s+[а-я]+', '', text)  # Удаление возможных дат
    # Токенизация и очистка
    tokens = [word for word in text.split() if word not in stop_words and len(word) > 2]
    # Биграммы
    bigrams = ['_'.join(tokens[i:i+2]) for i in range(len(tokens)-1)]
    return ' '.join(tokens + bigrams)

df_sample['processed_text'] = df_sample['review_text'].apply(preprocess)

# TF-IDF
all_words = ' '.join(df_sample['processed_text']).split()
vocab = list(set(all_words))
vocab_size = len(vocab)
word_to_idx = {word: idx for idx, word in enumerate(vocab)}

tf_matrix = np.zeros((len(df_sample), vocab_size))
for i, text in enumerate(df_sample['processed_text']):
    if text:
        word_counts = Counter(text.split())
        for word, count in word_counts.items():
            if word in word_to_idx:
                tf_matrix[i, word_to_idx[word]] = count / len(text.split())

doc_freq = np.sum(tf_matrix > 0, axis=0)
idf = np.log(len(df_sample) / (1 + doc_freq))
tfidf_matrix = tf_matrix * idf

# Кластеризация
num_clusters = 10
centroids, distortion = kmeans(tfidf_matrix, num_clusters)
cluster_labels, _ = vq(tfidf_matrix, centroids)
df_sample['cluster'] = cluster_labels

# Темы
themes = {}
for cluster in range(num_clusters):
    cluster_texts = df_sample[df_sample['cluster'] == cluster]['processed_text']
    if not cluster_texts.empty:
        all_cluster_words = ' '.join(cluster_texts.dropna()).split()
        top_words = Counter(all_cluster_words).most_common(10)
        themes[cluster] = top_words

print("Выявленные тематики (кластеры):")
for cluster, words in themes.items():
    print(f"Кластер {cluster}: {', '.join([w[0] for w in words])}")

# Сохранение и визуализация в data/clusters
output_dir = 'data/clusters'
os.makedirs(output_dir, exist_ok=True)
df_sample.to_csv(os.path.join(output_dir, 'clustered_reviews_improved.csv'), index=False)
if themes:
    wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(dict(themes[0]))
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.savefig(os.path.join(output_dir, 'theme_wordcloud_improved.png'))
    plt.show()