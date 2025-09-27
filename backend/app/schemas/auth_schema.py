from pydantic import BaseModel, field_validator
from typing import Optional, Literal, List
from enum import StrEnum
from datetime import date

# Предполагаем, что NonEmptyStr — кастомный тип из utils (str с min_length=1)
from app.utils.utils import (
    NonEmptyStr,
)  # Если нет — замени на str и добавь validator
from app.models.user_models import (
    UserRole,
)  # Или из schemas/role.py, если отдельно


class Role(StrEnum):
    user = "USER"
    admin = "ADMIN"

class ChartAttributes(BaseModel):
    date_start_1: date
    date_end_1: date
    date_start_2: Optional[date] = None
    date_end_2: Optional[date] = None
    product_id: str | int

    @field_validator("product_id")
    def validate_product_id(cls, v):
        if isinstance(v, str) and v != "all":
            raise ValueError("product_id must be an integer or 'all'")
        return v


class ChartConfig(BaseModel):
    id: str
    name: str
    type: Literal["product_stats", "monthly-review-count", "monthly-pie-chart", "small-bar-charts", "monthly-stacked-bars", "regional-bar-chart", "line-and-bar-pie-chart", "change-chart"]
    attributes: ChartAttributes

class PageConfig(BaseModel):
    id: str
    name: str
    charts: List[ChartConfig]

class DashboardConfig(BaseModel):
    pages: List[PageConfig]

class Config:
    json_encoders = {
        date: lambda v: v.isoformat()  # Для безопасности, на случай если даты где-то ещё
    }

class LoginCredentials(BaseModel):
    username: NonEmptyStr
    password: NonEmptyStr

    @field_validator("username")
    @classmethod
    def validate_username(cls, v):
        if len(v) > 50:
            raise ValueError("Username too long")
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v):
        if len(v) < 6:
            raise ValueError("Password too short")
        return v


class RegisterCredentials(BaseModel):
    username: NonEmptyStr
    password: NonEmptyStr
    role: Optional[UserRole] = (
        UserRole.MANAGER
    )  # Default manager, admin только для privileged

    # Validators как выше


class AuthTokenSchema(BaseModel):
    access_token: str  # Изменено с token на access_token
    token_type: str = "bearer"


class AuthTokenPayload(BaseModel):
    user_id: int
    role: Optional[UserRole] = None  # Опционально для ролей в payload, если нужно


class UserResponse(BaseModel):
    id: int
    username: str
    role: UserRole
