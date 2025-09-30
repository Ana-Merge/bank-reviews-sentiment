from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, timedelta
from app.repositories.repositories import NotificationConfigRepository, ProductRepository, ReviewRepository, MonthlyStatsRepository, NotificationRepository, AuditLogRepository
from app.models.user_models import User
from app.models.models import Notification, NotificationConfig, NotificationType
from app.schemas.schemas import NotificationConfigCreate
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

            current_review_count = await self._review_repo.count_by_product_and_period(
                session, product_ids, current_month, current_end
            )
            prev_review_count = await self._review_repo.count_by_product_and_period(
                session, product_ids, prev_month, prev_end
            )

            current_tonality = await self._review_repo.get_tonality_counts_by_product_and_period(
                session, product_ids, current_month, current_end
            )
            prev_tonality = await self._review_repo.get_tonality_counts_by_product_and_period(
                session, product_ids, prev_month, prev_end
            )

            current_avg_rating = await self._review_repo.get_avg_rating_by_products(
                session, product_ids
            )
            prev_avg_rating = await self._review_repo.get_avg_rating_by_products(
                session, product_ids
            )

            message = None
            if config.notification_type == NotificationType.REVIEW_SPIKE:
                if prev_review_count > 0:
                    change = ((current_review_count - prev_review_count) / prev_review_count) * 100
                    if abs(change) > config.threshold:
                        message = f"Review spike for product {product.name}: {change:.2f}% change ({prev_review_count} → {current_review_count})"
                elif current_review_count > 0:
                    message = f"Review spike for product {product.name}: new reviews appeared ({current_review_count})"

            elif config.notification_type == NotificationType.SENTIMENT_DECLINE:
                current_positive = current_tonality.get('positive', 0)
                prev_positive = prev_tonality.get('positive', 0)
                current_total = sum(current_tonality.values())
                prev_total = sum(prev_tonality.values())
                
                if prev_total > 0 and current_total > 0:
                    current_positive_percent = (current_positive / current_total) * 100
                    prev_positive_percent = (prev_positive / prev_total) * 100
                    change = current_positive_percent - prev_positive_percent
                    
                    if change < -config.threshold:
                        message = f"Sentiment decline for product {product.name}: positive reviews decreased by {abs(change):.2f}%"

            elif config.notification_type == NotificationType.RATING_DROP:
                if prev_avg_rating > 0 and current_avg_rating > 0:
                    change = current_avg_rating - prev_avg_rating
                    if change < -config.threshold:
                        message = f"Rating drop for product {product.name}: {prev_avg_rating:.2f} → {current_avg_rating:.2f} (change: {change:.2f})"

            elif config.notification_type == NotificationType.NEGATIVE_SPIKE:
                current_negative = current_tonality.get('negative', 0)
                prev_negative = prev_tonality.get('negative', 0)
                
                if prev_negative > 0:
                    negative_change = ((current_negative - prev_negative) / prev_negative) * 100
                    if negative_change > config.threshold:
                        message = f"Negative reviews spike for product {product.name}: {negative_change:.2f}% increase ({prev_negative} → {current_negative})"
                elif current_negative > 0:
                    message = f"Negative reviews appeared for product {product.name}: {current_negative} negative reviews"

            if message:
                await self.generate_notification(session, config.user_id, message, config.notification_type)
                await self._audit_log_repo.save(session, config.user_id, f"Generated auto-notification: {config.notification_type}")

    async def generate_notification(
        self, session: AsyncSession, user_id: int, message: str, notification_type: str
    ) -> Notification:
        notification = Notification(
            user_id=user_id, 
            message=message, 
            type=notification_type
        )
        saved = await self._notification_repo.save(session, notification)
        await self._audit_log_repo.save(session, user_id, f"Generated notification: {notification_type}")
        return saved

    async def get_user_notifications(self, session: AsyncSession, user_id: int, is_read: bool = False) -> List[Notification]:
        return await self._notification_repo.get_by_user_id(session, user_id, is_read)

    async def mark_as_read(self, session: AsyncSession, notification_id: int, user_id: int) -> bool:
        return await self._notification_repo.update_read_status(session, notification_id, user_id)

    async def delete_notification(self, session: AsyncSession, notification_id: int, user_id: int) -> bool:
        return await self._notification_repo.delete(session, notification_id, user_id)