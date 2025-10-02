# Dockerfile для one-shot скрипта предсказания (PyTorch 2.6.0)
FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-devel

# Установка зависимостей (без fastapi/uvicorn)
RUN pip install --no-cache-dir \
    transformers>=4.45.0 \
    accelerate>=0.26.0 \
    scikit-learn \
    pandas \
    numpy \
    safetensors \
    nltk \
    pymorphy3

# Рабочая директория
WORKDIR /app

# Копирование файлов
COPY predict_single.py /app/
COPY preprocess.py /app/

# Директории для моделей
RUN mkdir -p /app/results/topic_model /app/results/sentiment_model /app/results/logs

# Запуск скрипта (вывод в stdout)
CMD ["python", "predict_single.py"]