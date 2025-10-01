import logging
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(AsyncAttrs, DeclarativeBase):
    """Базовый класс для всех моделей SQLAlchemy"""
    pass


class DatabaseManager:
    """Менеджер для работы с базой данных"""
    
    def __init__(self, db_url: str):
        """
        Инициализация менеджера базы данных
        
        Args:
            db_url: URL подключения к базе данных
        """
        self._db_url = db_url
        self._engine = None
        self._sessionmaker = None

    async def initialize(self):
        """Инициализация подключения к базе данных и создание таблиц"""
        try:
            self._engine = create_async_engine(self._db_url, echo=True)
            self._sessionmaker = async_sessionmaker(
                self._engine, expire_on_commit=False, autoflush=False
            )
            logging.info(f"Успешное подключение к базе данных: {self._db_url}")
        except Exception as ex:
            logging.error(
                f"Произошла ошибка при подключении к базе данных {self._db_url}", 
                exc_info=ex
            )
            raise

        from app.models.user_models import User
        from app.models.models import (
            Product, Review, Cluster, ReviewCluster, MonthlyStats, ClusterStats,
            Notification, AuditLog, NotificationConfig 
        )

        async with self._engine.begin() as connection:
            try:
                existing_tables = await connection.run_sync(
                    lambda conn: conn.dialect.get_table_names(conn)
                )
                if not existing_tables:
                    await connection.run_sync(Base.metadata.create_all)
                    logging.info("Таблицы базы данных успешно созданы")
                else:
                    logging.info("Таблицы базы данных уже существуют, пропускаем создание")
            except Exception as ex:
                logging.error(
                    f"Произошла ошибка при создании таблиц базы данных для {self._db_url}",
                    exc_info=ex
                )
                raise

    async def dispose(self):
        """Закрытие подключения к базе данных"""
        if self._engine:
            await self._engine.dispose()
            logging.info(f"Закрыто подключение к базе данных: {self._db_url}")
        else:
            logging.warning("Нет engine для закрытия, база данных не была инициализирована")

    @asynccontextmanager
    async def create_session(self):
        """
        Асинхронный контекстный менеджер для сессии базы данных
        
        Yields:
            AsyncSession: Асинхронная сессия базы данных
        
        Raises:
            RuntimeError: Если менеджер не инициализирован
        """
        if not self._sessionmaker:
            raise RuntimeError("DatabaseManager не инициализирован, сначала вызовите initialize()")
        async with self._sessionmaker() as session:
            try:
                yield session
            except Exception as ex:
                logging.error(
                    "Произошла ошибка во время сессии базы данных. Откат изменений",
                    exc_info=ex,
                )
                await session.rollback()
                raise ex

    @property
    def async_session(self):
        """
        Sessionmaker для использования в других модулях
        
        Returns:
            async_sessionmaker: Асинхронный sessionmaker
        
        Raises:
            RuntimeError: Если менеджер не инициализирован
        """
        if not self._sessionmaker:
            raise RuntimeError("DatabaseManager не инициализирован, сначала вызовите initialize()")
        return self._sessionmaker