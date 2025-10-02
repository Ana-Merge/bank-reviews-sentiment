from transformers import AutoTokenizer, AutoModel
import torch
import numpy as np
import pandas as pd
import os
import io
import re
from bertopic import BERTopic
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt
from collections import Counter
import pymorphy3
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')

# Логирование CUDA
print(f"CUDA available: {torch.cuda.is_available()}")
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

# Расширенные стоп-слова
russian_stop_words = set(stopwords.words('russian'))
russian_stop_words.update([
    'банк', 'альфа-банк', 'втб', 'тинькоф', 'сбер', 'гбп', 
    'рубль', 'рубли', 'рублей', 'газпром', 'банковский', 'клиентский', 'это', 'который', 'мой', 'свой', 'добрый',
    'день', 'месяц', 'год', 'сотрудник', 'клиент', 'время', 'вопрос', 'офис', 'отделение', 'линия', 'горячий',
    'акция', 'условие', 'сумма', 'друг', 'работа', 'привилегия', 'договор', 'заявка', 'менеджер', 'ответ',
    'поддержка', 'обращение', 'номер', 'получение', 'перевод', 'реализация', 'последний', 'неделя', 'удобный',
    'быстрый', 'обслуживание', 'мобильный', 'оформление', 'указанный', 'жизнь', 'течение', 'первый', 'другой',
    'один', 'ваш', 'сентябрь', 'руб', 'премиальный', 'чат', 'ставка', 'процент', 'накопительный', 'документ',
    'очередь'
])

morph = pymorphy3.MorphAnalyzer()

# Предобработка текста
def preprocess(text):
    if not isinstance(text, str) or len(text.split()) < 10:
        return None
    text = re.sub(r'[\d\W_]+', ' ', text.lower())
    text = re.sub(r'\b\w*банк\w*\b', '', text)
    tokens = []
    for word in text.split():
        if word not in russian_stop_words and len(word) > 2:
            p = morph.parse(word)[0]
            if 'NOUN' in p.tag or 'ADJF' in p.tag:
                lemma = p.normal_form
                tokens.append(lemma)
    prepared = ' '.join(tokens)
    return prepared if len(prepared.split()) >= 10 else None

# Загрузка данных
file_path = '/app/bert/data/prepared/common/all_reviews.jsonl'
output_dir = '/app/bert/data/clusters'
os.makedirs(output_dir, exist_ok=True)

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        if not content.strip():
            raise ValueError("Файл пустой.")
        df = pd.read_json(io.StringIO(content), lines=True, orient='records')
except Exception as e:
    print(f"Ошибка чтения файла: {e}")
    raise

if df.empty:
    raise ValueError("DataFrame пустой после чтения.")

# Фильтр на Газпромбанк (раскомментируйте, если нужно только его отзывы)
# df = df[df['bank_name'] == 'Газпромбанк']

# Стратифицированная выборка с сохранением исходных индексо
if 'topic' in df.columns and len(df) > 1000:
    df_sample = df.groupby('topic', group_keys=False).apply(lambda x: x.sample(min(len(x), 300)), include_groups=False).reset_index()
else:
    df_sample = df.sample(n=min(1500, len(df))).reset_index()
print(f"Размер выборки: {len(df_sample)}")
print(f"Столбцы в df_sample: {df_sample.columns.tolist()}")

# Предобработка и фильтрация
df_sample['processed_text'] = df_sample['review_text'].apply(preprocess)
valid_df = df_sample[df_sample['processed_text'].notnull()].reset_index(drop=True)
print(f"Валидных текстов после предобработки: {len(valid_df)}")

if len(valid_df) == 0:
    raise ValueError("Нет валидных текстов после предобработки.")

# Получение эмбеддингов
tokenizer = AutoTokenizer.from_pretrained("bert-base-multilingual-cased")
model = AutoModel.from_pretrained("bert-base-multilingual-cased").to(device)

def get_bert_embeddings(texts, batch_size=16):
    embeddings = []
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i+batch_size]
        inputs = tokenizer(batch_texts, return_tensors="pt", padding=True, truncation=True, max_length=256).to(device)
        with torch.no_grad():
            outputs = model(**inputs)
            batch_embeddings = outputs.last_hidden_state.mean(dim=1).cpu().numpy()
            embeddings.extend(batch_embeddings)
    embeddings = np.vstack(embeddings)
    print(f"Размер эмбеддингов: {embeddings.shape}")
    return embeddings

embeddings = get_bert_embeddings(valid_df['processed_text'].tolist())

# Проверка размерностей
if len(embeddings) != len(valid_df):
    raise ValueError(f"Несоответствие размеров: embeddings ({len(embeddings)}), валидных документов ({len(valid_df)}).")

# Создание BERTopic
topic_model = BERTopic(
    nr_topics=20,
    min_topic_size=10,
    language="russian",
    calculate_probabilities=True,
    verbose=True
)

# Фит и трансформация
topics, probs = topic_model.fit_transform(valid_df['processed_text'].tolist(), embeddings)

# Добавление меток тем в DataFrame
valid_df['topic'] = topics

# Вывод тем
topics_info = topic_model.get_topic_info()
print("Темы и ключевые слова:")
print(topics_info)

# Сохранение тем и ключевых слов
topics_info.to_csv(os.path.join(output_dir, 'topic_keywords.csv'), index=False)

# Сохранение кластеров с полным содержимым
cluster_df = valid_df[['topic', 'processed_text', 'review_text']].dropna()
cluster_df.to_csv(os.path.join(output_dir, 'clustered_reviews_full.csv'), index=False)
print(f"Сохранено {len(cluster_df)} отзывов с кластерами в 'clustered_reviews_full.csv'.")

# Визуализация с обработкой ошибок
try:
    fig = topic_model.visualize_topics()
    fig.write_html(os.path.join(output_dir, 'topics_visualization.html'))
except Exception as e:
    print(f"Ошибка в visualize_topics: {e}. Пропускаем.")

try:
    fig = topic_model.visualize_documents(valid_df['processed_text'].tolist(), topics=topics)
    fig.write_html(os.path.join(output_dir, 'documents_visualization.html'))
except Exception as e:
    print(f"Ошибка в visualize_documents: {e}. Пропускаем.")

tsne = TSNE(n_components=2, random_state=42)
tsne_results = tsne.fit_transform(embeddings)
plt.figure(figsize=(10, 5))
plt.scatter(tsne_results[:, 0], tsne_results[:, 1], c=topics, cmap='viridis')
plt.title('t-SNE visualization with BERTopic')
plt.savefig(os.path.join(output_dir, 'tsne_bertopic.png'))

valid_df.to_csv(os.path.join(output_dir, 'clustered_reviews_bertopic.csv'), index=False)
print("Скрипт завершён успешно. Результаты сохранены в data/clusters.")