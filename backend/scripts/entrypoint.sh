#!/bin/bash
# scripts/entrypoint.sh
set -e

# Установка PYTHONPATH
export PYTHONPATH=/app:/app/app

echo "Current directory: $(pwd)"
echo "PYTHONPATH: $PYTHONPATH"

# Выполнение seed из корневой директории /app
echo "Running database migrations and seeding..."
python app/scripts/seed.py

# Запуск Uvicorn из корневой директории /app
echo "Starting FastAPI server..."
exec uvicorn app.core.setup:create_app --host 0.0.0.0 --port ${SERVER_PORT:-8000} --reload