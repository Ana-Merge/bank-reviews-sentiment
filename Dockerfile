FROM python:3.13-slim AS base

# Установка системных зависимостей для backend и Node.js
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
    curl \
    nginx \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Установка Node.js
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs

WORKDIR /app

# Устанавливаем PYTHONPATH
ENV PYTHONPATH=/app:/app/app

# Копируем и устанавливаем зависимости для backend
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем backend приложение
COPY backend/app ./app
COPY backend/scripts/entrypoint.sh ./scripts/entrypoint.sh

# Установка прав на entrypoint.sh
RUN sed -i 's/\r$//' ./scripts/entrypoint.sh && \
    chmod +x ./scripts/entrypoint.sh

# Установка Playwright
RUN pip install playwright==1.49.1 && \
    playwright install chromium

# Создаем директорию для frontend
WORKDIR /app/frontend

# Копируем и устанавливаем зависимости для frontend
COPY frontend/package*.json ./
RUN npm ci

# Копируем исходный код frontend и собираем
COPY frontend/. .
RUN npm run build

# Создаем директорию для nginx и копируем собранный фронтенд
RUN mkdir -p /var/www/html && \
    cp -r dist/* /var/www/html/

# Возвращаемся в корневую директорию
WORKDIR /app

# Копируем конфигурацию nginx
COPY frontend/nginx.conf /etc/nginx/nginx.conf

# Создаем конфигурацию supervisord
RUN echo '[supervisord]' > /etc/supervisor/conf.d/supervisord.conf && \
    echo 'nodaemon=true' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'user=root' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo '' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo '[program:backend]' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'command=/app/scripts/entrypoint.sh' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'directory=/app' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'autostart=true' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'autorestart=true' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'environment=PYTHONPATH=/app:/app/app' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'stdout_logfile=/dev/stdout' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'stdout_logfile_maxbytes=0' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'stderr_logfile=/dev/stderr' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'stderr_logfile_maxbytes=0' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo '' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo '[program:nginx]' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'command=nginx -g "daemon off;"' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'autostart=true' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'autorestart=true' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'stdout_logfile=/dev/stdout' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'stdout_logfile_maxbytes=0' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'stderr_logfile=/dev/stderr' >> /etc/supervisor/conf.d/supervisord.conf && \
    echo 'stderr_logfile_maxbytes=0' >> /etc/supervisor/conf.d/supervisord.conf

EXPOSE 80

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]