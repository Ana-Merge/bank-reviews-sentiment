# Базовый образ с PyTorch и CUDA
FROM pytorch/pytorch:2.4.1-cuda12.4-cudnn9-runtime

# Установка системных зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    wget \
    curl \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Установка Python-пакетов
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    scipy \
    matplotlib \
    wordcloud \
    pymorphy3 \
    nltk \
    scikit-learn \
    transformers \
    torch \
    bertopic \
    umap-learn \
    hdbscan \
    plotly

# Скачивание NLTK данных
RUN python -m nltk.downloader stopwords

# Копирование скрипта
WORKDIR /app/bert
COPY cluster_script.py /app/bert/cluster_script.py

# Проверка CUDA при запуске
CMD ["python", "-c", "import torch; print(f'CUDA available: {torch.cuda.is_available()}'); exec(open('cluster_script.py').read())"]