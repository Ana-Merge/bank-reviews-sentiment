from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.dependencies import get_db, get_current_user
from app.models.user_models import User
from app.schemas.auth_schema import DashboardConfig, PageConfig
from app.repositories.user_repositories import UserRepository
from app.core.exceptions import EntityAlreadyExistsException
from typing import Dict, Any

user_dashboard_router = APIRouter(prefix="/api/v1/user_dashboards", tags=["user_dashboards"])

@user_dashboard_router.get(
    "/config",
    response_model=DashboardConfig,
    summary="Получить конфиг пользователя",
    description="Получение конфига авторизованного пользователя, для отображения персональных страниц и графиков.",
    response_description="Конфиг в формате JSON."
)
async def get_dashboard_config(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    user_repo: UserRepository = Depends(lambda: UserRepository())
):
    """
    Получить конфигурацию дашборда текущего пользователя.
    """
    config = await user_repo.get_dashboard_config(db, current_user.id)
    if config is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    try:
        dashboard_config = DashboardConfig(**config)
        return dashboard_config
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Неправильный формат конфига: {str(e)}")

@user_dashboard_router.post(
    "/config",
    response_model=Dict[str, str],
    summary="Обновить кофиг пользователя",
    description="Обновление конфига пользователя, для авторизованного пользователя.",
    response_description="success или error",
    responses={
        200: {
            "description": "Configuration updated successfully",
            "content": {
                "application/json": {
                    "example": {"status": "success"}
                }
            }
        },
        404: {"description": "User not found"},
        400: {"description": "Invalid configuration format"}
    }
)
async def update_dashboard_config(
    config: DashboardConfig,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    user_repo: UserRepository = Depends(lambda: UserRepository())
):
    """
    Обновить конфигурацию дашборда текущего пользователя.
    """
    success = await user_repo.update_dashboard_config(db, current_user.id, config.model_dump(mode='json'))
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return {"status": "success"}

@user_dashboard_router.post(
    "/pages",
    response_model=Dict[str, str],
    summary="Добавить новую страницу в конфиг пользователя",
    description="Добавление новой страницы в конфиг авторизованного пользователя.",
    response_description="success  или error",
    responses={
        200: {
            "description": "Page added successfully",
            "content": {
                "application/json": {
                    "example": {"status": "success"}
                }
            }
        },
        404: {"description": "User not found"},
        400: {"description": "Invalid page format or page ID already exists"}
    }
)
async def add_dashboard_page(
    page: PageConfig,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    user_repo: UserRepository = Depends(lambda: UserRepository())
):
    """
    Добавить новую страницу в конфигурацию дашборда.
    """
    try:
        success = await user_repo.add_page_to_config(db, current_user.id, page)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
        return {"status": "success"}
    except EntityAlreadyExistsException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@user_dashboard_router.delete(
    "/pages/{page_id}",
    response_model=Dict[str, str],
    summary="Удаление страницы в конфиге пользователя",
    description="Удаление страницы в конфиге авторизованного пользователя по ID.",
    response_description="Delete successful или error",
    responses={
        200: {
            "description": "Page deleted successfully",
            "content": {
                "application/json": {
                    "example": {"status": "success"}
                }
            }
        },
        404: {"description": "User or page not found"}
    }
)
async def delete_dashboard_page(
    page_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    user_repo: UserRepository = Depends(lambda: UserRepository())
):
    """
    Удалить страницу из конфигурации дашборда по ID.
    """
    success = await user_repo.delete_page_from_config(db, current_user.id, page_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User or page not found")
    return {"status": "success"}

@user_dashboard_router.delete(
    "/config",
    response_model=Dict[str, str],
    summary="Очистка конфига пользователя",
    description="Полная очистка конфига авторизованного пользователя.",
    response_description="success или error",
    responses={
        200: {
            "description": "Configuration cleared successfully",
            "content": {
                "application/json": {
                    "example": {"status": "success"}
                }
            }
        },
        404: {"description": "User not found"}
    }
)
async def clear_dashboard_config(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    user_repo: UserRepository = Depends(lambda: UserRepository())
):
    """
    Очистить конфигурацию дашборда текущего пользователя.
    """
    success = await user_repo.clear_dashboard_config(db, current_user.id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return {"status": "success"}