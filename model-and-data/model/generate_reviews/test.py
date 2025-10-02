import requests
import time
import json

# Конфигурация
OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "wavecut/vikhr:7b-instruct_0.4-Q4_1"
TEST_PROMPTS = [
    "Оцените отзыв: 'Отличный сервис в Альфа-Банке, быстрое открытие счета.'",
    "Проанализируйте: 'Проблемы с процентами по вкладу в Газпромбанке.'",
    "Расскажите о преимуществах вклада в Сбербанке."
] * 10  # 30 запросов для теста

# Функция для отправки запроса и измерения времени
def test_model_performance(prompt):
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False  # Отключаем поток для точного измерения времени
    }
    start_time = time.time()
    response = requests.post(OLLAMA_API_URL, json=payload)
    end_time = time.time()
    
    if response.status_code == 200:
        result = response.json()
        response_text = result.get("response", "")
        total_time = end_time - start_time
        tokens_processed = len(response_text.split())  # Простая оценка токенов
        return total_time, tokens_processed, response_text
    else:
        return None, None, f"Ошибка: {response.status_code}"

# Тест производительности
def run_performance_test():
    total_time = 0
    total_tokens = 0
    results = []

    for i, prompt in enumerate(TEST_PROMPTS):
        print(f"Тест {i+1}/{len(TEST_PROMPTS)}: {prompt[:50]}...")
        time_taken, tokens, response = test_model_performance(prompt)
        if time_taken is not None:
            total_time += time_taken
            total_tokens += tokens
            results.append({
                "prompt": prompt,
                "time_taken": time_taken,
                "tokens": tokens,
                "response": response
            })
            print(f"Время: {time_taken:.2f} сек, Токены: {tokens}")
        else:
            print(f"Пропущен из-за ошибки.")

    avg_time = total_time / len(TEST_PROMPTS) if TEST_PROMPTS else 0
    tokens_per_sec = total_tokens / total_time if total_time > 0 else 0

    print(f"\nИтоги теста (на {len(TEST_PROMPTS)} запросов):")
    print(f"Среднее время на запрос: {avg_time:.2f} сек")
    print(f"Токенов в секунду: {tokens_per_sec:.2f}")
    print(f"Общее время: {total_time:.2f} сек")
    print(f"Общее количество токенов: {total_tokens}")

    # Сохранение результатов (опционально)
    with open("performance_test_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    run_performance_test()