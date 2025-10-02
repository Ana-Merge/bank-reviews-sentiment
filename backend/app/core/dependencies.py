from typing import Annotated, AsyncGenerator
from fastapi import Depends, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db_manager import DatabaseManager
from app.services.auth_services import AuthService, TokenService, PasswordService
from app.services.stats_service import StatsService
from fastapi.security import OAuth2PasswordBearer
from app.models.user_models import User

# Схема OAuth2 для аутентификации по токену
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

async def get_db(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """
    Зависимость для получения сессии базы данных
    
    Args:
        request: Запрос FastAPI
    
    Yields:
        AsyncSession: Асинхронная сессия базы данных
    """
    db: DatabaseManager = request.app.state.database_manager
    async with db.create_session() as session:
        yield session

def get_auth_service(request: Request) -> AuthService:
    """Получение сервиса аутентификации из состояния приложения"""
    if not hasattr(request.app.state, 'auth_service'):
        raise HTTPException(status_code=500, detail="Сервис аутентификации не инициализирован")
    return request.app.state.auth_service

def get_stats_service(request: Request) -> StatsService:
    """Получение сервиса статистики из состояния приложения"""
    if not hasattr(request.app.state, 'stats_service'):
        raise HTTPException(status_code=500, detail="Сервис статистики не инициализирован")
    return request.app.state.stats_service

def get_password_service(request: Request) -> PasswordService:
    """Получение сервиса работы с паролями из состояния приложения"""
    if not hasattr(request.app.state, 'password_service'):
        raise HTTPException(status_code=500, detail="Сервис паролей не инициализирован")
    return request.app.state.password_service

def get_token_service(request: Request) -> TokenService:
    """Получение сервиса работы с токенами из состояния приложения"""
    if not hasattr(request.app.state, 'token_service'):
        raise HTTPException(status_code=500, detail="Сервис токенов не инициализирован")
    return request.app.state.token_service

async def get_current_user(
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    session: Annotated[AsyncSession, Depends(get_db)],
    token: str = Depends(oauth2_scheme)
) -> User:
    """
    Зависимость для получения текущего пользователя из токена
    
    Args:
        auth_service: Сервис аутентификации
        session: Сессия базы данных
        token: JWT токен
    
    Returns:
        User: Объект текущего пользователя
    
    Raises:
        HTTPException: Если токен невалиден или пользователь не найден
    """
    return await auth_service.get_current_user(token, session)

# Аннотированные типы для зависимостей
DbSession = Annotated[AsyncSession, Depends(get_db)]
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]
PasswordServiceDep = Annotated[PasswordService, Depends(get_password_service)]
TokenServiceDep = Annotated[TokenService, Depends(get_token_service)]
StatsServiceDep = Annotated[StatsService, Depends(get_stats_service)]