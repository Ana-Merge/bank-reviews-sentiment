# Dockerfile для обучения модели (PyTorch 2.6+ для исправления CVE)
FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-devel

# Установка зависимостей, необходимых для обучения
RUN pip install --no-cache-dir \
    transformers>=4.45.0 \
    accelerate>=0.26.0 \
    scikit-learn \
    pandas \
    numpy \
    safetensors

# Установка рабочей директории
WORKDIR /app

# Копирование только необходимых файлов для обучения
COPY train_cat.py /app/
COPY processed_multi_label_reviews_10k.jsonl /app/

# Создание директорий для результатов и логов
RUN mkdir -p /app/results/logs /app/results/topic_model /app/results/sentiment_model

# Команда по умолчанию (переопределяется в docker-compose)
CMD ["python", "train_cat.py"]