from enum import Enum
from sqlalchemy import CheckConstraint, String, JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db_manager import (
    Base,
)  # Предполагаем, что Base = declarative_base() в db_manager.py
# from app.schemas.auth_schema import (
#     Role
# )  # Импорт Enum из схем (см. ниже)


class UserRole(str, Enum):
    MANAGER = "manager"
    ADMIN = "admin"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(
        String(20), default=UserRole.MANAGER, nullable=False
    )
    dashboard_config = mapped_column(JSON, nullable=True, default={})

    __table_args__ = (CheckConstraint("role IN ('manager', 'admin')"),)
