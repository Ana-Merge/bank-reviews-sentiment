# Используем базовый образ с CUDA 11.8 и Ubuntu 22.04
FROM nvidia/cuda:11.8.0-base-ubuntu22.04

# Установка зависимостей
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Проверка версии Python
RUN python3 --version

# Установка Python-пакетов
RUN pip3 install --upgrade pip
RUN pip3 install torch torchvision --index-url https://download.pytorch.org/whl/cu118
RUN pip3 install transformers==4.40.0 pandas numpy scikit-learn pymorphy3 tqdm

# Копирование кода и данных
WORKDIR /app
COPY train/train1.py .
COPY data/ data/

# Создание директории для моделей
RUN mkdir -p models

# Запуск скрипта
CMD ["python3", "train1.py"]