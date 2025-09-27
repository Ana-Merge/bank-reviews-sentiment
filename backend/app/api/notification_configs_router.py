from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any
from app.services.notification_service import NotificationService
from app.core.dependencies import get_current_user, get_db
from app.models.user_models import User
from app.schemas.schemas import NotificationConfigCreate, NotificationConfigResponse
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Request

configs_router = APIRouter(prefix="/api/v1/notification_configs", tags=["notification_configs"])

def get_notification_service(request: Request) -> NotificationService:
    """Retrieve the NotificationService from app.state."""
    return request.app.state.notification_service

@configs_router.post("/", response_model=NotificationConfigResponse)
async def create_config(
    config_data: NotificationConfigCreate,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
    service: NotificationService = Depends(get_notification_service),
):
    return await service.create_config(session, current_user.id, config_data)

@configs_router.get("/", response_model=List[NotificationConfigResponse])
async def get_configs(
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
    service: NotificationService = Depends(get_notification_service),
):
    return await service.get_user_configs(session, current_user.id)

@configs_router.patch("/{config_id}", response_model=NotificationConfigResponse)
async def update_config(
    config_id: int,
    update_data: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
    service: NotificationService = Depends(get_notification_service),
):
    updated = await service.update_config(session, config_id, current_user.id, update_data)
    if not updated:
        raise HTTPException(404, "Config not found")
    return updated

@configs_router.delete("/{config_id}", response_model=Dict[str, str])
async def delete_config(
    config_id: int,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
    service: NotificationService = Depends(get_notification_service),
):
    deleted = await service.delete_config(session, config_id, current_user.id)
    if not deleted:
        raise HTTPException(404, "Config not found")
    return {"detail": "Config deleted"}