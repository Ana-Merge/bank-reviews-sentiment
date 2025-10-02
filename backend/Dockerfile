
FROM python:3.13-slim AS base

RUN echo "deb http://deb.debian.org/debian trixie contrib non-free" >> /etc/apt/sources.list && \
    apt-get update && apt-get install -y \
    ca-certificates \
    fonts-liberation \
    fonts-unifont \
    fonts-dejavu \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libwayland-client0 \
    libwayland-egl1 \
    libwayland-server0 \
    libx11-6 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxtst6 \
    wget \
    xdg-utils \
    build-essential \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY scripts/entrypoint.sh ./scripts/entrypoint.sh

# Установка прав на entrypoint.sh и исправление символов конца строки
RUN sed -i 's/\r$//' ./scripts/entrypoint.sh && \
    chmod +x ./scripts/entrypoint.sh

RUN pip install playwright==1.49.1 && \
    playwright install chromium

FROM base AS dev
ENV PYTHONPATH=/app:/app/app
EXPOSE 8005
ENTRYPOINT ["./scripts/entrypoint.sh"]

FROM base AS run
ENV PYTHONPATH=/app:/app/app
EXPOSE 8005
ENTRYPOINT ["./scripts/entrypoint.sh"]