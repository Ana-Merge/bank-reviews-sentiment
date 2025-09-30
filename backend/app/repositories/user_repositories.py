# user_repositories.py
from sqlalchemy import exists, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Dict, Any, List
from app.core.exceptions import (
    EntityAlreadyExistsException,
    EntityNotFoundException,
)
from app.models.user_models import User
from app.schemas.auth_schema import DashboardConfig, PageConfig

class UserRepository:
    """Репозиторий для работы с пользователями"""
    
    async def get_by_id(self, session: AsyncSession, user_id: int) -> User | None:
        """
        Получить пользователя по ID
        
        Args:
            session: Асинхронная сессия базы данных
            user_id: ID пользователя
            
        Returns:
            User | None: Объект пользователя или None если не найден
        """
        statement = select(User).where(User.id == user_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_by_username(self, session: AsyncSession, username: str) -> User | None:
        """
        Получить пользователя по имени пользователя
        
        Args:
            session: Асинхронная сессия базы данных
            username: Имя пользователя
            
        Returns:
            User | None: Объект пользователя или None если не найден
        """
        statement = select(User).where(User.username == username)
        result = await session.execute(statement)
        return result.scalar_one_or_none()
    
    async def get_dashboard_config(self, session: AsyncSession, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Получить конфигурацию дашборда пользователя
        
        Args:
            session: Асинхронная сессия базы данных
            user_id: ID пользователя
            
        Returns:
            Optional[Dict[str, Any]]: Конфигурация дашборда или None если пользователь не найден
        """
        user = await self.get_by_id(session, user_id)
        if not user:
            return None
        config = user.dashboard_config or {"pages": []}
        return config
    
    async def update_dashboard_config(self, session: AsyncSession, user_id: int, config: Dict[str, Any]) -> bool:
        """
        Обновить конфигурацию дашборда пользователя
        
        Args:
            session: Асинхронная сессия базы данных
            user_id: ID пользователя
            config: Новая конфигурация дашборда
            
        Returns:
            bool: True если успешно обновлено, False если пользователь не найден
        """
        user = await self.get_by_id(session, user_id)
        if not user:
            return False
        statement = update(User).where(User.id == user_id).values(dashboard_config=config)
        await session.execute(statement)
        await session.commit()
        return True
    
    async def add_page_to_config(self, session: AsyncSession, user_id: int, page: PageConfig) -> bool:
        """
        Добавить страницу в конфигурацию дашборда
        
        Args:
            session: Асинхронная сессия базы данных
            user_id: ID пользователя
            page: Конфигурация страницы
            
        Returns:
            bool: True если успешно добавлено, False если пользователь не найден
            
        Raises:
            EntityAlreadyExistsException: Если страница с таким ID уже существует
        """
        user = await self.get_by_id(session, user_id)
        if not user:
            return False
        config = user.dashboard_config or {"pages": []}
        if any(p["id"] == page.id for p in config["pages"]):
            raise EntityAlreadyExistsException(f"Страница с ID {page.id} уже существует")
        config["pages"].append(page.model_dump(mode='json'))
        statement = update(User).where(User.id == user_id).values(dashboard_config=config)
        await session.execute(statement)
        await session.commit()
        return True
    
    async def delete_page_from_config(self, session: AsyncSession, user_id: int, page_id: str) -> bool:
        """
        Удалить страницу из конфигурации дашборда
        
        Args:
            session: Асинхронная сессия базы данных
            user_id: ID пользователя
            page_id: ID страницы для удаления
            
        Returns:
            bool: True если успешно удалено, False если пользователь или страница не найдены
        """
        user = await self.get_by_id(session, user_id)
        if not user:
            return False
        config = user.dashboard_config or {"pages": []}
        config["pages"] = [p for p in config["pages"] if p["id"] != page_id]
        statement = update(User).where(User.id == user_id).values(dashboard_config=config)
        await session.execute(statement)
        await session.commit()
        return True
    
    async def clear_dashboard_config(self, session: AsyncSession, user_id: int) -> bool:
        """
        Очистить конфигурацию дашборда пользователя
        
        Args:
            session: Асинхронная сессия базы данных
            user_id: ID пользователя
            
        Returns:
            bool: True если успешно очищено, False если пользователь не найден
        """
        user = await self.get_by_id(session, user_id)
        if not user:
            return False
        statement = update(User).where(User.id == user_id).values(dashboard_config={"pages": []})
        await session.execute(statement)
        await session.commit()
        return True
    
    async def exists_by_username(self, session: AsyncSession, username: str) -> bool:
        """
        Проверить существование пользователя по имени пользователя
        
        Args:
            session: Асинхронная сессия базы данных
            username: Имя пользователя
            
        Returns:
            bool: True если пользователь существует, False если нет
        """
        statement = select(exists().where(User.username == username))
        result = await session.execute(statement)
        return result.scalar_one()

    async def get_all(
        self, session: AsyncSession, page: int = 0, size: int = 10
    ) -> list[User]:
        """
        Получить список всех пользователей с пагинацией
        
        Args:
            session: Асинхронная сессия базы данных
            page: Номер страницы (начинается с 0)
            size: Размер страницы
            
        Returns:
            list[User]: Список пользователей
        """
        statement = select(User).order_by(User.id).offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()

    async def count_all(self, session: AsyncSession) -> int:
        """
        Получить общее количество пользователей
        
        Args:
            session: Асинхронная сессия базы данных
            
        Returns:
            int: Количество пользователей
        """
        statement = select(func.count()).select_from(User)
        result = await session.execute(statement)
        return result.scalar_one()

    async def save(self, session: AsyncSession, user: User) -> User:
        """
        Сохранить пользователя в базе данных
        
        Args:
            session: Асинхронная сессия базы данных
            user: Объект пользователя для сохранения
            
        Returns:
            User: Сохраненный пользователь
        """
        session.add(user)
        await session.flush()
        await session.commit()
        await session.refresh(user)
        return user