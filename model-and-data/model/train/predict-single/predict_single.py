import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import logging
import os
from preprocess import preprocess_text

os.makedirs('/app/results/logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/results/logs/predict_single_log.txt'),
        logging.StreamHandler()
    ]
)

model_name = 'cointegrated/rubert-tiny2'
tokenizer = AutoTokenizer.from_pretrained(model_name)

topic_model_path = '/app/results/topic_model'
if not os.path.exists(topic_model_path):
    raise FileNotFoundError(f"Директория модели тем не найдена: {topic_model_path}")
topic_model = AutoModelForSequenceClassification.from_pretrained(topic_model_path, local_files_only=True)
topic_model.eval()

sent_model_path = '/app/results/sentiment_model'
if not os.path.exists(sent_model_path):
    raise FileNotFoundError(f"Директория модели сентиментов не найдена: {sent_model_path}")
sent_model = AutoModelForSequenceClassification.from_pretrained(sent_model_path, local_files_only=True)
sent_model.eval()

topics_list = []
sentiments_list = ['негативная', 'нейтральная', 'положительная']

def load_lists_from_logs(log_path='/app/results/logs/training_log.txt'):
    global topics_list, sentiments_list
    if os.path.exists(log_path):
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                if "Уникальные темы:" in line:
                    topics_str = line.split("Уникальные темы: ")[1].strip()
                    topics_list = eval(topics_str)
                if "Уникальные сентименты:" in line:
                    sents_str = line.split("Уникальные сентименты: ")[1].strip()
                    sentiments_list = eval(sents_str)
    else:
        topics_list = ['debitcards', 'hypothec', 'mobile_app', 'other']
        logging.warning("Логи не найдены, fallback темы")
    print(f"Загруженные темы: {topics_list}")
    print(f"Загруженные сентименты: {sentiments_list}")

load_lists_from_logs()

raw_review_text = "Очень доволен оформлением ипотеки — всё прошло быстро, менеджер вежливый и компетентный. Но мобильное приложение постоянно вылетает при попытке загрузить сканы документов."

def predict_topics_single(text):
    processed_text = preprocess_text(text)
    print(f"Обработанный текст: '{processed_text}'")
    if not processed_text.strip():
        print("Warning: Текст пустой после preprocess!")
        return []
    inputs = tokenizer([processed_text], return_tensors="pt", truncation=True, padding=True, max_length=128)
    inputs = {k: v.to('cpu') for k, v in inputs.items()}
    with torch.no_grad():
        outputs = topic_model(**inputs)
    probs = torch.sigmoid(outputs.logits)
    print(f"Вероятности по темам ({[t[:10] for t in topics_list]}): {probs[0].tolist()}")
    threshold = 0.2
    predicted_topics = [topics_list[j] for j in range(len(topics_list)) if probs[0][j] > threshold]
    if not predicted_topics:
        top_indices = torch.topk(probs[0], min(2, len(topics_list))).indices.tolist()
        print(f"Top темы (probs < {threshold}): {[(topics_list[idx], probs[0][idx].item()) for idx in top_indices]}")
    return predicted_topics

def predict_sentiments_single(text, topics):
    processed_text = preprocess_text(text)
    if not topics:
        return []
    text_topic_pairs = [(processed_text, topic) for topic in topics]
    prompts = [f"Текст: {t} [SEP] Тема: {topic}" for t, topic in text_topic_pairs]
    inputs = tokenizer(prompts, return_tensors="pt", truncation=True, padding=True, max_length=128)
    inputs = {k: v.to('cpu') for k, v in inputs.items()}
    with torch.no_grad():
        outputs = sent_model(**inputs)
    pred_ids = torch.argmax(outputs.logits, dim=1).tolist()
    predicted_sentiments = [sentiments_list[pred_id] for pred_id in pred_ids]
    return predicted_sentiments

if __name__ == "__main__":
    try:
        logging.info("Запуск предсказания...")
        topics = predict_topics_single(raw_review_text)
        sentiments = predict_sentiments_single(raw_review_text, topics)
        
        print("\n" + "=" * 60)
        print("ОРИГИНАЛЬНЫЙ ОТЗЫВ:")
        print(raw_review_text)
        print("\nПРЕДСКАЗАННЫЕ ТЕМЫ: " + (", ".join(topics) if topics else "Нет тем (проверьте probs выше)"))
        print("\nСЕНТИМЕНТЫ ПО ТЕМАМ:")
        for i, (topic, sentiment) in enumerate(zip(topics, sentiments), 1):
            print(f"  {i}. {topic}: {sentiment}")
        print("=" * 60)
        logging.info("Предсказание завершено.")
    except Exception as e:
        error_msg = f"Ошибка: {str(e)}"
        print(error_msg)
        logging.error(error_msg)