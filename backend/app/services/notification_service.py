from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, timedelta
from app.repositories.repositories import NotificationConfigRepository, ProductRepository, ReviewRepository, MonthlyStatsRepository, NotificationRepository, AuditLogRepository
from app.models.user_models import User
from app.models.models import Notification, NotificationConfig
from app.schemas.schemas import NotificationConfigCreate, NotificationType
from app.core.exceptions import EntityNotFoundException
from typing import List, Dict, Any
from dateutil.relativedelta import relativedelta

class NotificationService:
    def __init__(
        self,
        notification_repo: NotificationRepository,
        audit_log_repo: AuditLogRepository,
        config_repo: NotificationConfigRepository,
        product_repo: ProductRepository,
        review_repo: ReviewRepository,
        monthly_stats_repo: MonthlyStatsRepository,
    ):
        self._notification_repo = notification_repo
        self._audit_log_repo = audit_log_repo
        self._config_repo = config_repo
        self._product_repo = product_repo
        self._review_repo = review_repo
        self._monthly_stats_repo = monthly_stats_repo

    async def create_config(self, session: AsyncSession, user_id: int, config_data: NotificationConfigCreate) -> NotificationConfig:
        config = NotificationConfig(user_id=user_id, **config_data.model_dump())
        saved = await self._config_repo.save(session, config)
        await self._audit_log_repo.save(session, user_id, f"Created notification config: {config.notification_type}")
        return saved

    async def get_user_configs(self, session: AsyncSession, user_id: int) -> List[NotificationConfig]:
        return await self._config_repo.get_all_by_user(session, user_id)

    async def update_config(self, session: AsyncSession, config_id: int, user_id: int, update_data: Dict[str, Any]) -> NotificationConfig:
        config = await self._config_repo.get_by_id(session, config_id, user_id)
        if not config:
            raise EntityNotFoundException("Notification config not found")
        for key, value in update_data.items():
            setattr(config, key, value)
        updated = await self._config_repo.update(session, config)
        await self._audit_log_repo.save(session, user_id, f"Updated notification config ID: {config_id}")
        return updated

    async def delete_config(self, session: AsyncSession, config_id: int, user_id: int) -> bool:
        deleted = await self._config_repo.delete(session, config_id, user_id)
        if deleted:
            await self._audit_log_repo.save(session, user_id, f"Deleted notification config ID: {config_id}")
        return deleted

    async def check_and_generate_notifications(self, session: AsyncSession):
        configs = await self._config_repo.get_active_configs(session)
        for config in configs:
            product = await self._product_repo.get_by_id(session, config.product_id)
            if not product:
                continue

            descendants = await self._product_repo.get_all_descendants(session, config.product_id)
            product_ids = [config.product_id] + [d.id for d in descendants]

            today = date.today()
            current_month = date(today.year, today.month, 1)
            prev_month = (current_month - relativedelta(months=1)).replace(day=1)

            if config.period != "monthly":
                continue


            current_end = current_month + relativedelta(months=1) - timedelta(days=1)
            prev_end = prev_month + relativedelta(months=1) - timedelta(days=1)

            current_stats = await self._monthly_stats_repo.get_by_product_and_month(session, config.product_id, current_month)
            prev_stats = await self._monthly_stats_repo.get_by_product_and_month(session, config.product_id, prev_month)

            if not current_stats or not prev_stats:
                continue

            message = None
            if config.notification_type == NotificationType.REVIEW_SPIKE:
                change = ((current_stats.review_count - prev_stats.review_count) / prev_stats.review_count) * 100 if prev_stats.review_count > 0 else 0
                if abs(change) > config.threshold:
                    message = f"Review spike for product {product.name}: {change:.2f}% change"
            elif config.notification_type == NotificationType.SENTIMENT_DECLINE:
                change = current_stats.sentiment_trend - prev_stats.sentiment_trend if prev_stats.sentiment_trend else 0
                if change < -config.threshold:
                    message = f"Sentiment decline for product {product.name}: {change:.2f}"
            elif config.notification_type == NotificationType.RATING_DROP:
                change = current_stats.avg_rating - prev_stats.avg_rating if prev_stats.avg_rating else 0
                if change < -config.threshold:
                    message = f"Rating drop for product {product.name}: {change:.2f}"
            elif config.notification_type == NotificationType.NEGATIVE_SPIKE:

                current_tonality = await self._review_repo.get_tonality_counts_by_product_and_period(
                    session, product_ids, current_month, current_end
                )
                prev_tonality = await self._review_repo.get_tonality_counts_by_product_and_period(
                    session, product_ids, prev_month, prev_end
                )
                negative_change = ((current_tonality['negative'] - prev_tonality['negative']) / prev_tonality['negative']) * 100 if prev_tonality['negative'] > 0 else 0
                if negative_change > config.threshold:
                    message = f"Negative reviews spike for product {product.name}: {negative_change:.2f}%"


            if message:
                await self.generate_notification(session, config.user_id, message, config.notification_type.value)
                await self._audit_log_repo.save(session, config.user_id, f"Generated auto-notification: {config.notification_type}")

    async def generate_notification(
        self, session: AsyncSession, user_id: int, message: str, type: str
    ) -> Notification:
        notification = Notification(user_id=user_id, message=message, type=type)
        saved = await self._notification_repo.save(session, notification)
        # Лог в audit_logs
        await self._audit_log_repo.save(session, user_id, f"Generated notification: {type}")
        return saved

    async def get_user_notifications(self, session: AsyncSession, user_id: int, is_read: bool = False) -> List[Notification]:
        return await self._notification_repo.get_by_user_id(session, user_id, is_read)

    async def mark_as_read(self, session: AsyncSession, notification_id: int, user_id: int) -> bool:
        return await self._notification_repo.update_read_status(session, notification_id, user_id)

    async def delete_notification(self, session: AsyncSession, notification_id: int, user_id: int) -> bool:
        return await self._notification_repo.delete(session, notification_id, user_id)