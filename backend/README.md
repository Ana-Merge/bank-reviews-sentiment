
## Установка и запуск
1. Убедитесь, что у вас установлен [Docker](https://www.docker.com/)
2. Склонируйте данный репозиторий:
    ```shell
    git clone https://github.com/Cat-n-Code/clients_tonality_backend.git && cd clients_tonality_backend
    ```
3. Создайте .env файл со следующими полями. Рабочий набор полей:
   ```ini
    SERVER_PORT=8080
    AUTH_TOKEN_SECRET_KEY=I5DrSPTUuY8ytohMTC3nwnRLqsbLZHNYn9zufr49mG0=
    INITIAL_USER=null
    DB_URL=postgresql+psycopg://user:qwerty12@database:5432/clients_tonality
    
    POSTGRES_DATABASE=clients_tonality
    POSTGRES_USER=user
    POSTGRES_PASSWORD=qwerty12
    
    REGION=ru-central1
    AWS_ACCESS_KEY_ID=YCAJEeNDwcaQCXm3HgKm1diik
    AWS_SECRET_ACCESS_KEY=YCNFbMGRR_do7WuF32jdJAvHhnMb1Z9PcOctT8qT
    BUCKET_NAME=bucketcharm
    AWS_ENDPOINT=https://storage.yandexcloud.net/
    AWS_ACCESS_DOMAIN_NAME=https://storage.yandexcloud.net/bucketcharm/
   ```
4. Запустите сервис:
    ```shell
    docker compose -f docker-compose.yaml -f docker-compose.dev.yaml up -d
    ```
5. После запуска, сервис будет доступен по адресу http://localhost:8080. Также
    по адресу http://localhost:8080/docs будет доступна OpenAPI документация.
6. Остановка сервиса:
    ```shell
    docker compose -f docker-compose.yaml -f docker-compose.dev.yaml down
    ```
## Использование
1.	Выберите сервер в левом верхнем углу экрана /docs servers (localhost)
2.  При использовании можно пройти аутентификацию, но сущестующий функционал доступен любому, в том числе неаутентифицированному пользователю
3.  С помощью соответствующих запрсов можно загрузить файл в облачное хранилище и получить ссылку на загруженный ранее файл по его имени (выданному сервисом после выгрузки в хранилище)

## Демонстрация работы backend
[Видео запуска] (https://disk.yandex.ru/i/KTE6M3g9FQqduw)
