# Dockerfile for model training and prediction (PyTorch 2.6+ for CVE fix)
FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-devel

# Install additional dependencies
RUN pip install --no-cache-dir transformers[torch]>=4.45.0 accelerate peft scikit-learn pandas numpy safetensors bertopic plotly nltk pymorphy3

# Set working directory
WORKDIR /app

# Copy all necessary scripts (train_cat.py, predict.py, preprocess.py, etc.)
COPY . /app

# No default CMD, as it will be overridden in docker-compose