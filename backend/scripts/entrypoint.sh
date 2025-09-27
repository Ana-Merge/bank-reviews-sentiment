#!/bin/bash
# scripts/entrypoint.sh
set -e
cd app
# Выполнение seed
python scripts/seed.py
# Запуск Uvicorn
exec uvicorn app.core.setup:create_app --host 0.0.0.0 --port ${SERVER_PORT:-8000}