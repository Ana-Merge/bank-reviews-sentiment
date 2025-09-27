from typing import Annotated, AsyncGenerator
from fastapi import Depends, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db_manager import DatabaseManager
from app.services.auth_services import AuthService, TokenService, PasswordService
from app.services.stats_service import StatsService
from fastapi.security import OAuth2PasswordBearer
from app.models.user_models import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

async def get_db(request: Request) -> AsyncGenerator[AsyncSession, None]:
    db: DatabaseManager = request.app.state.database_manager
    async with db.create_session() as session:
        yield session

def get_auth_service(request: Request) -> AuthService:
    if not hasattr(request.app.state, 'auth_service'):
        raise HTTPException(status_code=500, detail="AuthService not initialized")
    return request.app.state.auth_service

def get_stats_service(request: Request) -> StatsService:
    if not hasattr(request.app.state, 'stats_service'):
        raise HTTPException(status_code=500, detail="StatsService not initialized")
    return request.app.state.stats_service

def get_password_service(request: Request) -> PasswordService:
    if not hasattr(request.app.state, 'password_service'):
        raise HTTPException(status_code=500, detail="PasswordService not initialized")
    return request.app.state.password_service

def get_token_service(request: Request) -> TokenService:
    if not hasattr(request.app.state, 'token_service'):
        raise HTTPException(status_code=500, detail="TokenService not initialized")
    return request.app.state.token_service

async def get_current_user(
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    session: Annotated[AsyncSession, Depends(get_db)],
    token: str = Depends(oauth2_scheme)
) -> User:
    return await auth_service.get_current_user(token, session)

DbSession = Annotated[AsyncSession, Depends(get_db)]
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]
PasswordServiceDep = Annotated[PasswordService, Depends(get_password_service)]
TokenServiceDep = Annotated[TokenService, Depends(get_token_service)]
StatsServiceDep = Annotated[StatsService, Depends(get_stats_service)]