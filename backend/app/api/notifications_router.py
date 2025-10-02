from fastapi import APIRouter, Depends, Query, HTTPException
from typing import List, Dict, Any
from datetime import date
from fastapi import Request
from app.services.notification_service import NotificationService
from app.core.dependencies import get_current_user, get_db
from app.models.user_models import User
from app.schemas.schemas import NotificationResponse
from sqlalchemy.ext.asyncio import AsyncSession

notifications_router = APIRouter(prefix="/api/v1/notifications", tags=["notifications"])

def get_notification_service(request: Request) -> NotificationService:
    """Получить NotificationService из app.state."""
    return request.app.state.notification_service

@notifications_router.get("/", response_model=List[NotificationResponse])
async def get_notifications(
    is_read: bool = Query(False, description="Фильтр по прочитанным уведомлениям"),
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
    notification_service: NotificationService = Depends(get_notification_service),
):
    """
    Получить уведомления пользователя.
    
    **Параметры**:
    - `is_read`: Фильтр по прочитанным уведомлениям (опционально)
    
    **Возвращает**:
    - Список уведомлений пользователя
    """
    notifications = await notification_service.get_user_notifications(session, current_user.id, is_read)
    return notifications

@notifications_router.patch("/{notification_id}", response_model=Dict[str, str])
async def mark_notification_as_read(
    notification_id: int,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
    notification_service: NotificationService = Depends(get_notification_service),
):
    """
    Пометить уведомление как прочитанное.
    
    **Параметры**:
    - `notification_id`: ID уведомления
    
    **Возвращает**:
    - Подтверждение успешного обновления
    """
    if await notification_service.mark_as_read(session, notification_id, current_user.id):
        return {"detail": "Уведомление помечено как прочитанное"}
    raise HTTPException(status_code=404, detail="Уведомление не найдено")

@notifications_router.delete("/{notification_id}", response_model=Dict[str, str])
async def delete_notification(
    notification_id: int,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
    notification_service: NotificationService = Depends(get_notification_service),
):
    """
    Удалить уведомление.
    
    **Параметры**:
    - `notification_id`: ID уведомления
    
    **Возвращает**:
    - Подтверждение успешного удаления
    """
    if await notification_service.delete_notification(session, notification_id, current_user.id):
        return {"detail": "Уведомление удалено"}
    raise HTTPException(status_code=404, detail="Уведомление не найдено")

@notifications_router.post("/check", response_model=Dict[str, str])
async def manual_check_notifications(
    session: AsyncSession = Depends(get_db),
    notification_service: NotificationService = Depends(get_notification_service),
):
    """
    Запустить ручную проверку и генерацию уведомлений.
    
    **Возвращает**:
    - Подтверждение запуска проверки
    """
    await notification_service.check_and_generate_notifications(session)
    return {"detail": "Ручная проверка уведомлений запущена"}