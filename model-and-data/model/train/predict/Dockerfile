# Dockerfile для инференса модели (PyTorch 2.6.0, тот же образ, что для обучения)
FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-devel

# Установка дополнительных зависимостей для инференса
RUN pip install --no-cache-dir \
    transformers>=4.45.0 \
    accelerate>=0.26.0 \
    scikit-learn \
    pandas \
    numpy \
    safetensors \
    nltk \
    pymorphy3 \
    fastapi \
    uvicorn

# Установка рабочей директории
WORKDIR /app

# Копирование только необходимых файлов для инференса
COPY predict.py /app/
COPY preprocess.py /app/

# Создание директорий для моделей и логов
RUN mkdir -p /app/results/topic_model /app/results/sentiment_model /app/results/logs

# Открытие порта для FastAPI
EXPOSE 8002

# Команда по умолчанию
CMD ["python", "-m", "uvicorn", "predict:app", "--host", "0.0.0.0", "--port", "8002"]