from typing import Annotated
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from datetime import UTC, datetime, timedelta
from jose import jwt, JWTError
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.auth_schema import (
    AuthTokenPayload,
    AuthTokenSchema,
    LoginCredentials,
    RegisterCredentials,
)
from app.core.exceptions import (
    UnauthorizedException,
    EntityAlreadyExistsException,
)
from app.repositories.user_repositories import UserRepository
from app.models.user_models import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

class PasswordService:
    def __init__(self):
        self._crypto_context = CryptContext(schemes=["bcrypt"])

    def get_password_hash(self, raw_password: str) -> str:
        return self._crypto_context.hash(raw_password)

    def compare_passwords(self, raw_password: str, hashed_password: str) -> bool:
        return self._crypto_context.verify(raw_password, hashed_password)

class TokenService:
    def __init__(self, secret_key: str, token_lifetime: int = 1800):
        self._secret_key = secret_key
        self._token_lifetime = token_lifetime

    def create_auth_token(self, payload: AuthTokenPayload) -> str:
        claims = {
            "exp": datetime.now(UTC) + timedelta(seconds=self._token_lifetime),
            **payload.model_dump(mode="json"),
        }
        return jwt.encode(claims, self._secret_key, algorithm="HS256")

    def verify_auth_token(self, token: str) -> AuthTokenPayload | None:
        try:
            payload = jwt.decode(
                token,
                self._secret_key,
                algorithms=["HS256"],
                options={"verify_exp": True},
            )
            return AuthTokenPayload.model_validate(payload)
        except JWTError:
            return None

class AuthService:
    def __init__(
        self,
        password_service: PasswordService,
        token_service: TokenService,
        user_repository: UserRepository,
    ):
        self._password_service = password_service
        self._token_service = token_service
        self._user_repository = user_repository

    async def login_user(
        self, session: AsyncSession, credentials: LoginCredentials
    ) -> AuthTokenSchema:
        user = await self._user_repository.get_by_username(
            session, credentials.username
        )
        if user is None:
            raise UnauthorizedException("Invalid credentials")

        if not self._password_service.compare_passwords(
            credentials.password, user.password_hash
        ):
            raise UnauthorizedException("Invalid credentials")

        payload = AuthTokenPayload(user_id=user.id)
        token = self._token_service.create_auth_token(payload)

        return AuthTokenSchema(access_token=token)

    async def register_user(
        self, session: AsyncSession, credentials: RegisterCredentials
    ) -> User:
        if await self._user_repository.exists_by_username(
            session, credentials.username
        ):
            raise EntityAlreadyExistsException("Username already exists")

        hashed_password = self._password_service.get_password_hash(credentials.password)
        user = User(
            username=credentials.username,
            password_hash=hashed_password,
            role=credentials.role,
        )
        saved_user = await self._user_repository.save(session, user)
        return saved_user

    async def get_current_user(
        self, token: str, session: AsyncSession
    ) -> User:
        payload = self._token_service.verify_auth_token(token)
        if payload is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user = await self._user_repository.get_by_id(session, payload.user_id)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return user