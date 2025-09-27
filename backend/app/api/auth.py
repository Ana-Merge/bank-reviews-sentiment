from fastapi import APIRouter, Depends, status, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.dependencies import DbSession, AuthServiceDep
from app.schemas.auth_schema import AuthTokenSchema, LoginCredentials, RegisterCredentials, UserResponse
from app.models.user_models import UserRole

auth_router = APIRouter(prefix="/api/v1/auth", tags=["Аутентификация"])

@auth_router.post(
    "/login",
    summary="Метод аутентификации пользователя (для Swagger UI)",
    response_model=AuthTokenSchema,
    response_description=(
        "`token` - уникальный токен, действующий 30 минут. "
        "Используется для авторизации в других методах."
    ),
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "description": "Указанные `username` или `password` неверные"
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Неверный формат входных данных"
        },
    },
)
async def login(
    session: DbSession,
    auth_service: AuthServiceDep,
    form_data: OAuth2PasswordRequestForm = Depends(),
):
    try:
        credentials = LoginCredentials(username=form_data.username, password=form_data.password)
        return await auth_service.login_user(session, credentials)
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid credentials")

@auth_router.post(
    "/login-json",
    summary="Метод аутентификации пользователя (JSON для фронтенда)",
    response_model=AuthTokenSchema,
    response_description=(
        "`token` - уникальный токен, действующий 30 минут. "
        "Используется для авторизации в других методах."
    ),
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "description": "Указанные `username` или `password` неверные"
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Неверный формат входных данных"
        },
    },
)
async def login_json(
    credentials: LoginCredentials,
    session: DbSession,
    auth_service: AuthServiceDep,
):
    try:
        return await auth_service.login_user(session, credentials)
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid credentials")

@auth_router.post(
    "/register",
    summary="Регистрация нового пользователя",
    response_model=UserResponse,
    response_description="Новый пользователь создан (роль по умолчанию manager)",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Username уже существует"},
        status.HTTP_403_FORBIDDEN: {
            "description": "Регистрация админа ограничена (только для admin)"
        },
    },
)
async def register(
    session: DbSession,
    auth_service: AuthServiceDep,
    schema: RegisterCredentials
):
    if schema.role == UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Registration restricted to admins")
    try:
        user = await auth_service.register_user(session, schema)
        return UserResponse(id=user.id, username=user.username, role=user.role)
    except Exception:
        raise HTTPException(status_code=400, detail="Username already exists")