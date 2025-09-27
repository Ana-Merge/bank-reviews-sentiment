# Base stage
FROM python:3.13-slim AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY scripts/entrypoint.sh ./scripts/entrypoint.sh

# Установка прав на entrypoint.sh и исправление символов конца строки
RUN sed -i 's/\r$//' ./scripts/entrypoint.sh && \
    chmod +x ./scripts/entrypoint.sh

# Dev stage (hot-reload)
FROM base AS dev
ENV PYTHONPATH=/app:/app/app
EXPOSE 8000
ENTRYPOINT ["./scripts/entrypoint.sh"]

# Run stage
FROM base AS run
ENV PYTHONPATH=/app:/app/app
EXPOSE 8000
ENTRYPOINT ["./scripts/entrypoint.sh"]