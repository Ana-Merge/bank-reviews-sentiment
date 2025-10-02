import asyncio
import os
import random
import logging
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from datetime import date, datetime, timedelta
from app.core.db_manager import DatabaseManager, Base
from app.models.user_models import User
from app.models.models import (
    Product, Review, Cluster, ReviewCluster, MonthlyStats, ClusterStats, Notification, AuditLog, NotificationConfig, ReviewProduct,
    ProductType, ClientType, Sentiment, NotificationType
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def seed_db():
    try:
        db_url = os.getenv("DB_URL")
        if not db_url:
            try:
                with open("/run/secrets/db_url", "r") as f:
                    db_url = f.read().strip()
            except FileNotFoundError:
                raise ValueError("DB_URL не задан или не найден")
        
        if not db_url:
            raise ValueError("DB_URL не задан")
        
        db_manager = DatabaseManager(db_url)
        await db_manager.initialize()
        async_session = db_manager.async_session

        async with async_session() as session:
            async with session.begin():
                user_exists = await session.execute(select(User).where(User.username == "admin"))
                if user_exists.scalar_one_or_none():
                    logger.info("Database already seeded, skipping...")
                    return

                admin_hash = pwd_context.hash("admin")
                manager_hash = pwd_context.hash("manager")
                manager_hash2 = pwd_context.hash("manager2")
                manager_hash3 = pwd_context.hash("manager3")
                users = [
                    User(username="admin", password_hash=admin_hash, role="admin"),
                    User(username="manager", password_hash=manager_hash, role="manager"),
                    User(username="manager2", password_hash=manager_hash2, role="manager"),
                    User(username="manager3", password_hash=manager_hash3, role="manager"),
                ]
                session.add_all(users)
                await session.flush()
                user_ids = {u.username: u.id for u in users}

                audit_logs = [
                    AuditLog(user_id=user_ids["admin"], action="User login", timestamp=datetime.now()),
                    AuditLog(user_id=user_ids["manager"], action="Product stats viewed", timestamp=datetime.now() - timedelta(hours=1)),
                    AuditLog(user_id=user_ids["manager2"], action="Notification read", timestamp=datetime.now() - timedelta(hours=2)),
                    AuditLog(user_id=user_ids["manager3"], action="Notification settings updated", timestamp=datetime.now() - timedelta(hours=3)),
                    AuditLog(user_id=user_ids["admin"], action="System notification check completed", timestamp=datetime.now() - timedelta(hours=4)),
                ]
                session.add_all(audit_logs)
                await session.flush()
                logger.info("Audit logs seeded")

                await session.commit()
                logger.info("Database seeded successfully!")
        
        await db_manager.dispose()
    except Exception as e:
        logger.error(f"Seed failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(seed_db())