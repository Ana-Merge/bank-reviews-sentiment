from pydantic import BaseModel, field_validator
from typing import Optional, Literal, List
from enum import StrEnum
from datetime import date

from app.utils.utils import (
    NonEmptyStr,
)
from app.models.user_models import (
    UserRole,
)


class Role(StrEnum):
    user = "USER"
    admin = "ADMIN"

class ChartAttributes(BaseModel):
    date_start_1: date
    date_end_1: date
    date_start_2: Optional[date] = None
    date_end_2: Optional[date] = None
    product_id: str | int
    source: str
    aggregation_type: str

    @field_validator("product_id")
    def validate_product_id(cls, v):
        if isinstance(v, str) and v != "all":
            raise ValueError("product_id должен быть int 'all'")
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
        date: lambda v: v.isoformat()
    }

class UserConfigResponse(BaseModel):
    id: int
    username: str
    role: UserRole
    dashboard_config: Optional[DashboardConfig] = None

class UsersListResponse(BaseModel):
    users: List[UserConfigResponse]
    
    class Config:
        from_attributes = True


class LoginCredentials(BaseModel):
    username: NonEmptyStr
    password: NonEmptyStr

    @field_validator("username")
    @classmethod
    def validate_username(cls, v):
        if len(v) > 50:
            raise ValueError("Логин слишком большой")
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v):
        if len(v) < 5:
            raise ValueError("Пароль слишком короткий")
        return v

class RegisterCredentials(BaseModel):
    username: NonEmptyStr
    password: NonEmptyStr
    role: Optional[UserRole] = (
        UserRole.MANAGER
    )

class AuthTokenSchema(BaseModel):
    access_token: str
    token_type: str = "bearer"

class AuthTokenPayload(BaseModel):
    user_id: int
    role: Optional[UserRole] = None

class UserResponse(BaseModel):
    id: int
    username: str
    role: UserRole