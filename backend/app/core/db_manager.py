# flake8: noqa F401
import logging
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(AsyncAttrs, DeclarativeBase):
    pass


class DatabaseManager:
    def __init__(self, db_url: str):
        self._db_url = db_url
        self._engine = None
        self._sessionmaker = None

    async def initialize(self):
        try:
            self._engine = create_async_engine(self._db_url, echo=True)  # echo=True для отладки
            self._sessionmaker = async_sessionmaker(
                self._engine, expire_on_commit=False, autoflush=False
            )
            logging.info(f"Successfully connected to database: {self._db_url}")
        except Exception as ex:
            logging.error(
                f"Exception occurred during connection to database {self._db_url}", 
                exc_info=ex
            )
            raise

        # Импорт всех используемых моделей
        from app.models.user_models import User
        from app.models.models import (
            Product, Review, Cluster, ReviewCluster, MonthlyStats, ClusterStats,
            Notification, AuditLog, NotificationConfig  # Добавлено NotificationConfig и другие
        )

        async with self._engine.begin() as connection:
            try:
                # Проверка, существуют ли таблицы
                existing_tables = await connection.run_sync(
                    lambda conn: conn.dialect.get_table_names(conn)
                )
                if not existing_tables:
                    await connection.run_sync(Base.metadata.create_all)
                    logging.info("Database tables were successfully created")
                else:
                    logging.info("Database tables already exist, skipping creation")
            except Exception as ex:
                logging.error(
                    f"Exception occurred during database tables creation for {self._db_url}",
                    exc_info=ex
                )
                raise

    async def dispose(self):
        if self._engine:
            await self._engine.dispose()
            logging.info(f"Closed connection with database: {self._db_url}")
        else:
            logging.warning("No engine to dispose, database was not initialized")

    @asynccontextmanager
    async def create_session(self):
        if not self._sessionmaker:
            raise RuntimeError("DatabaseManager not initialized, call initialize() first")
        async with self._sessionmaker() as session:
            try:
                yield session
            except Exception as ex:
                logging.error(
                    "Exception was thrown during database session. Rollback",
                    exc_info=ex,
                )
                await session.rollback()
                raise ex

    @property
    def async_session(self):
        """Экспортируем sessionmaker для использования в других модулях (например, setup.py)"""
        if not self._sessionmaker:
            raise RuntimeError("DatabaseManager not initialized, call initialize() first")
        return self._sessionmaker