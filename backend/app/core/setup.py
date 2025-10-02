import logging
import sys
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.db_manager import DatabaseManager
from app.services.auth_services import (
    AuthService,
    PasswordService,
    TokenService,
)
from app.services.stats_service import StatsService
from app.services.parser_service import ParserService
from app.services.notification_service import NotificationService
from app.services.data_initializer import DataInitializer
from app.scripts.jsonl_loader import JSONLLoader

from app.repositories.user_repositories import UserRepository
from app.repositories.repositories import (
    ProductRepository, ReviewRepository, MonthlyStatsRepository,
    ClusterRepository, ReviewClusterRepository, ClusterStatsRepository,
    NotificationRepository, AuditLogRepository, NotificationConfigRepository, ReviewsForModelRepository
)
from app.core.exceptions import (
    AppException,
    handle_app_exception,
    handle_validation_exception,
)
from app.core.settings import AppSettings

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_app(settings: AppSettings | None = None) -> FastAPI:
    """Создание и настройка приложения FastAPI"""
    logger.info("Запуск создания приложения")
    if settings is None:
        logger.info("Загрузка настроек приложения")
        settings = AppSettings()

    app = FastAPI(
        title="Review Analytics Auth",
        lifespan=_app_lifespan,
        servers=[
            {"url": "http://localhost:8000", "description": "Локальный сервер"},
        ],
        responses={
            400: {"description": "Неверный формат входных данных"},
        },
    )
    
    # Настройка CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:8000", "http://localhost:3000", "http://localhost:5147", "http://localhost"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    logger.info("Настройка глобальных зависимостей")
    _setup_app_dependencies(app, settings)

    logger.info("Подключение маршрутов")
    from app.api.auth import auth_router
    from app.api.dashboards import dashboards_router
    from app.api.user_dashboards import user_dashboard_router
    from app.api.notifications_router import notifications_router
    from app.api.notification_configs_router import configs_router
    from app.api.parser_router import parsers_router
    
    app.include_router(auth_router)
    app.include_router(dashboards_router)
    app.include_router(user_dashboard_router)
    app.include_router(notifications_router)
    app.include_router(configs_router)
    app.include_router(parsers_router)

    logger.info("Настройка обработчиков исключений")
    app.add_exception_handler(AppException, handle_app_exception)
    app.add_exception_handler(RequestValidationError, handle_validation_exception)

    logger.info("Создание приложения завершено")
    return app

def _setup_app_dependencies(app: FastAPI, settings: AppSettings):
    """Настройка зависимостей приложения"""
    logger.info("Инициализация менеджера базы данных")
    app.state.settings = settings
    app.state.database_manager = DatabaseManager(settings.db_url)

    logger.info("Инициализация репозиториев")
    user_repository = UserRepository()
    product_repository = ProductRepository()
    review_repository = ReviewRepository()
    monthly_stats_repository = MonthlyStatsRepository()
    cluster_repository = ClusterRepository()
    review_cluster_repository = ReviewClusterRepository()
    cluster_stats_repository = ClusterStatsRepository()
    notification_repository = NotificationRepository()
    audit_log_repository = AuditLogRepository()
    notification_config_repository = NotificationConfigRepository()
    reviews_for_model_repository = ReviewsForModelRepository()

    logger.info("Инициализация сервисов")
    password_service = PasswordService()
    token_service = TokenService(
        settings.auth_token_secret_key, settings.auth_token_lifetime
    )
    auth_service = AuthService(password_service, token_service, user_repository)
    app.state.auth_service = auth_service

    stats_service = StatsService(
    product_repo=product_repository,
    review_repo=review_repository,
    monthly_stats_repo=monthly_stats_repository,
    cluster_stats_repo=cluster_stats_repository,
    cluster_repo=cluster_repository,
    review_cluster_repo=review_cluster_repository,
    reviews_for_model_repo=reviews_for_model_repository,
    )
    app.state.stats_service = stats_service

    notification_service = NotificationService(
        notification_repo=notification_repository,
        audit_log_repo=audit_log_repository,
        config_repo=notification_config_repository,
        product_repo=product_repository,
        review_repo=review_repository,
        monthly_stats_repo=monthly_stats_repository,
    )
    app.state.notification_service = notification_service
    parser_service = ParserService(reviews_for_model_repository)
    app.state.parser_service = parser_service
    
    jsonl_loader = JSONLLoader(reviews_for_model_repository)
    data_initializer = DataInitializer()
    app.state.jsonl_loader = jsonl_loader
    app.state.data_initializer = data_initializer

@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    logger.info("Инициализация подключения к базе данных")
    db: DatabaseManager = app.state.database_manager
    await db.initialize()
    logger.info("База данных инициализирована")

    # ЗАГРУЗКА ДАННЫХ ИЗ JSONL ПРИ СТАРТЕ
    if os.getenv('SKIP_JSONL_LOAD', 'false').lower() != 'true':
        logger.info("Запуск инициализации данных из JSONL файлов")
        try:
            async with app.state.database_manager.async_session() as session:
                initializer = app.state.data_initializer
                results = await initializer.initialize_data(session)
                logger.info(f"Инициализация данных завершена: {results}")
        except Exception as e:
            logger.error(f"Ошибка при инициализации данных: {str(e)}", exc_info=True)
    else:
        logger.info("Пропуск загрузки JSONL данных (SKIP_JSONL_LOAD=true)")

    # Запуск планировщика задач
    scheduler = AsyncIOScheduler()
    
    async def run_checks():
        """Задача для проверки и генерации уведомлений"""
        async with app.state.database_manager.async_session() as session:
            service = app.state.notification_service
            logger.info("Запуск запланированной проверки уведомлений")
            try:
                await service.check_and_generate_notifications(session)
            except Exception as e:
                logger.error(f"Не удалось проверить уведомления: {str(e)}", exc_info=True)

    # Запуск проверки каждые 10 минут
    scheduler.add_job(run_checks, 'cron', minute='*/10')
    scheduler.start()
    logger.info("Планировщик запущен")

    try:
        yield
    finally:
        logger.info("Остановка планировщика")
        scheduler.shutdown()
        logger.info("Закрытие подключения к базе данных")
        await db.dispose()

app = create_app()