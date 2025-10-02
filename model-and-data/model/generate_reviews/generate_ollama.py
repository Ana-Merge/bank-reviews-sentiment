import json
import random
import statistics
from collections import defaultdict
import requests
import time
from datetime import datetime
import subprocess
import re

# Определение тем
themes = [
    "карты",  # кредитные и дебетовые
    "кредиты",  # вместе с автокредитами и реструктуризацией
    "депозиты",
    "индивидуальное обслуживание",
    "удаленное обслуживание",
    "ипотека",
    "другое"  # мобильное приложение, другие продукты
]

# Маппинг тональностей
sentiments = ["положительная", "нейтральная", "отрицательная"]

def load_reviews(file_path):
    reviews_by_theme_sentiment = defaultdict(lambda: defaultdict(list))
    review_lengths = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            review = json.loads(line.strip())
            text = review.get('review_text', '').strip()
            topic = review.get('topic', 'other')
            rating = review.get('rating', 0)
            if rating <= 2:
                sentiment = "отрицательная"
            elif rating == 3:
                sentiment = "нейтральная"
            elif rating >= 4:
                sentiment = "положительная"
            else:
                sentiment = "нейтральная"  # по умолчанию
            # Маппинг тем на ваш список
            if topic in ["creditcards", "debitcards", "cards"]:
                mapped_theme = "карты"
            elif topic in ["credits", "restructing", "hypothec"]:
                mapped_theme = "займы"
            elif topic == "deposits":
                mapped_theme = "депозиты"
            elif topic == "service":
                mapped_theme = "обслуживание"
            elif topic == "mobile_app":
                mapped_theme = "мобильное приложение"
            else:
                mapped_theme = "другое"
            reviews_by_theme_sentiment[mapped_theme][sentiment].append(text)
            review_lengths.append(len(text.split()))  # Длина в словах
    avg_length = int(statistics.mean(review_lengths)) if review_lengths else 150
    return reviews_by_theme_sentiment, avg_length

def start_ollama_server():
    # Проверяем, запущен ли сервер
    try:
        response = requests.get("http://localhost:11434/api/tags")
        if response.status_code == 200:
            print("Ollama сервер уже запущен.")
            return
    except requests.ConnectionError:
        pass
    # Запускаем сервер
    subprocess.Popen(["ollama", "serve"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(5)  # Ждем запуска
    print("Ollama сервер запущен.")

def generate_review(ollama_url, model_name, reviews_by_theme_sentiment, avg_length):
    # Выбор 2-3 тем
    num_themes = random.randint(1, 4)
    selected_themes = random.sample(themes, num_themes)
    # Для каждой темы выбираем отдельную тональность
    theme_sentiments = {}
    prompt_parts = []
    for theme in selected_themes:
        sentiment = random.choice(sentiments)
        theme_sentiments[theme] = sentiment
        examples = reviews_by_theme_sentiment[theme].get(sentiment, [])
        num_examples = min(random.randint(3, 5), len(examples))
        selected_examples = random.sample(examples, num_examples) if examples else []
        prompt_parts.append(f"Тема '{theme}' с {sentiment} тональностью. Примеры: {', '.join(selected_examples)}.")
    # Формирование промпта для генерации полного отзыва
    system_prompt = "Ты генерируешь только чистый JSON без лишнего текста. Формат: {'text': 'отзыв', 'topics': ['theme1', 'theme2'], 'sentiments': ['sent1', 'sent2']}. Текст отзыва на русском, coherent и реалистичный."
    prompt = f"Генерируй реалистичный отзыв о Газпромбанке, затрагивающий темы: {', '.join(selected_themes)}. Для каждой темы используй указанную тональность и стиль из примеров: {' '.join(prompt_parts)} Объедини в coherent текст длиной около {avg_length} слов."
    payload = {
        "model": model_name,
        "prompt": prompt,
        "system": system_prompt,
        "stream": False
    }
    response = requests.post(ollama_url, json=payload)
    if response.status_code == 200:
        result = response.json()
        generated_text = result.get("response", "")
        # Парсинг JSON из ответа модели
        try:
            generated_review = json.loads(generated_text)
            # Корректировка структуры под пример
            generated_review["topics"] = selected_themes
            generated_review["sentiments"] = [theme_sentiments[theme] for theme in selected_themes]
            return generated_review
        except json.JSONDecodeError:
            # Если не JSON, пытаемся извлечь текст
            match = re.search(r'\{.*\}', generated_text, re.DOTALL)
            if match:
                try:
                    generated_review = json.loads(match.group(0))
                    generated_review["topics"] = selected_themes
                    generated_review["sentiments"] = [theme_sentiments[theme] for theme in selected_themes]
                    return generated_review
                except json.JSONDecodeError:
                    pass
            return {"text": generated_text, "topics": selected_themes, "sentiments": [theme_sentiments[theme] for theme in selected_themes], "bank_name": "Газпромбанк"}
    else:
        return {"text": "Ошибка генерации", "topics": selected_themes, "sentiments": [theme_sentiments[theme] for theme in selected_themes], "bank_name": "Газпромбанк"}

def main(input_file, output_file, num_reviews=1, ollama_url="http://localhost:11434/api/generate", model_name="lakomoor/vikhr-llama-3.2-1b-instruct:1b"):
    start_time = time.time()
    start_ollama_server()  # Убедимся, что сервер запущен
    reviews_by_theme_sentiment, avg_length = load_reviews(input_file)
    generated_reviews = []
    progress_step = 1
    last_progress = 0

    for i in range(num_reviews):
        review = generate_review(ollama_url, model_name, reviews_by_theme_sentiment, avg_length)
        review["bank_name"] = "Газпромбанк"
        generated_reviews.append(review)
        current_progress = (i + 1) // progress_step
        if current_progress > last_progress:
            elapsed_time = time.time() - start_time
            print(f"Прогресс: {current_progress}% (от {num_reviews}), Время: {elapsed_time:.2f} сек, {datetime.now().strftime('%H:%M:%S')}")
            last_progress = current_progress

    with open(output_file, 'w', encoding='utf-8') as f:
        for review in generated_reviews:
            f.write(json.dumps(review, ensure_ascii=False) + '\n')
    total_time = time.time() - start_time
    print(f"Сгенерировано {num_reviews} отзывов и сохранено в {output_file}, Общее время: {total_time:.2f} сек")

if __name__ == "__main__":
    input_file = '../data/prepared/common/gazprom_reviews.jsonl'
    output_file = 'generated_multi_label_reviews.jsonl'
    main(input_file, output_file)