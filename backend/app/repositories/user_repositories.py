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
    async def get_by_id(self, session: AsyncSession, user_id: int) -> User | None:
        statement = select(User).where(User.id == user_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_by_username(self, session: AsyncSession, username: str) -> User | None:
        statement = select(User).where(User.username == username)
        result = await session.execute(statement)
        return result.scalar_one_or_none()
    
    async def get_dashboard_config(self, session: AsyncSession, user_id: int) -> Optional[Dict[str, Any]]:
        user = await self.get_by_id(session, user_id)
        if not user:
            return None
        config = user.dashboard_config or {"pages": []}
        return config
    
    async def update_dashboard_config(self, session: AsyncSession, user_id: int, config: Dict[str, Any]) -> bool:
        user = await self.get_by_id(session, user_id)
        if not user:
            return False
        statement = update(User).where(User.id == user_id).values(dashboard_config=config)
        await session.execute(statement)
        await session.commit()
        return True
    
    async def add_page_to_config(self, session: AsyncSession, user_id: int, page: PageConfig) -> bool:
        user = await self.get_by_id(session, user_id)
        if not user:
            return False
        config = user.dashboard_config or {"pages": []}
        if any(p["id"] == page.id for p in config["pages"]):
            raise EntityAlreadyExistsException(f"Page with id {page.id} already exists")
        config["pages"].append(page.model_dump(mode='json'))
        statement = update(User).where(User.id == user_id).values(dashboard_config=config)
        await session.execute(statement)
        await session.commit()
        return True
    
    async def delete_page_from_config(self, session: AsyncSession, user_id: int, page_id: str) -> bool:
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
        user = await self.get_by_id(session, user_id)
        if not user:
            return False
        statement = update(User).where(User.id == user_id).values(dashboard_config={"pages": []})
        await session.execute(statement)
        await session.commit()
        return True
    
    async def exists_by_username(self, session: AsyncSession, username: str) -> bool:
        statement = select(exists().where(User.username == username))
        result = await session.execute(statement)
        return result.scalar_one()

    async def get_all(
        self, session: AsyncSession, page: int = 0, size: int = 10
    ) -> list[User]:
        statement = select(User).order_by(User.id).offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()

    async def count_all(self, session: AsyncSession) -> int:
        statement = select(func.count()).select_from(User)
        result = await session.execute(statement)
        return result.scalar_one()

    async def save(self, session: AsyncSession, user: User) -> User:
        session.add(user)
        await session.flush()
        await session.commit()
        await session.refresh(user)
        return user