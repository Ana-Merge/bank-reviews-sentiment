import logging
import sys
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
from app.services.notification_service import NotificationService
from app.repositories.user_repositories import UserRepository
from app.repositories.repositories import (
    ProductRepository, ReviewRepository, MonthlyStatsRepository,
    ClusterRepository, ReviewClusterRepository, ClusterStatsRepository,
    NotificationRepository, AuditLogRepository, NotificationConfigRepository
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
    logger.info("Starting create_app")
    if settings is None:
        logger.info("Loading AppSettings")
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
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:8000", "http://localhost:3000", "http://localhost:5147"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("Setting up global dependencies")
    _setup_app_dependencies(app, settings)

    logger.info("Including routers")
    from app.api.auth import auth_router
    from app.api.dashboards import dashboards_router
    from app.api.user_dashboards import user_dashboard_router
    from app.api.notifications_router import notifications_router
    from app.api.notification_configs_router import configs_router
    app.include_router(auth_router)
    app.include_router(dashboards_router)
    app.include_router(user_dashboard_router)
    app.include_router(notifications_router)
    app.include_router(configs_router)

    logger.info("Setting up exception handlers")
    app.add_exception_handler(AppException, handle_app_exception)
    app.add_exception_handler(RequestValidationError, handle_validation_exception)

    logger.info("create_app completed")
    return app

def _setup_app_dependencies(app: FastAPI, settings: AppSettings):
    logger.info("Initializing DatabaseManager")
    app.state.settings = settings
    app.state.database_manager = DatabaseManager(settings.db_url)

    logger.info("Initializing repositories")
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

    logger.info("Initializing services")
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

@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    logger.info("Initializing database connection")
    db: DatabaseManager = app.state.database_manager
    await db.initialize()
    logger.info("Database initialized")

    # Запуск APScheduler
    scheduler = AsyncIOScheduler()
    
    async def run_checks():
        async with app.state.database_manager.async_session() as session:
            service = app.state.notification_service
            logger.info("Running scheduled notification checks")
            try:
                await service.check_and_generate_notifications(session)
            except Exception as e:
                logger.error(f"Failed to check notifications: {str(e)}", exc_info=True)

    scheduler.add_job(run_checks, 'cron', minute='*/10')  # Изменено: каждые 10 минут
    scheduler.start()
    logger.info("Scheduler started")

    try:
        yield
    finally:
        logger.info("Shutting down scheduler")
        scheduler.shutdown()
        logger.info("Disposing database connection")
        await db.dispose()

app = create_app()