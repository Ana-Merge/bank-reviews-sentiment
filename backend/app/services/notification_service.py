from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, timedelta, datetime
from app.repositories.repositories import NotificationConfigRepository, ProductRepository, ReviewRepository, MonthlyStatsRepository, NotificationRepository, AuditLogRepository
from app.models.user_models import User
from app.models.models import Notification, NotificationConfig, NotificationType
from app.schemas.schemas import NotificationConfigCreate
from app.core.exceptions import EntityNotFoundException
from typing import List, Dict, Any, Tuple, Optional
from dateutil.relativedelta import relativedelta
import logging

logger = logging.getLogger(__name__)

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

    def get_comparison_periods(self, period: str) -> Tuple[Optional[date], Optional[date], Optional[date], Optional[date]]:
        """
        Возвращает кортеж (current_start, current_end, prev_start, prev_end) для сравнения.
        Всегда сравнивает два завершенных периода.
        """
        today = date.today()
        
        if period == "monthly":
            last_month = today.replace(day=1) - timedelta(days=1)
            current_start = last_month.replace(day=1)
            current_end = last_month
            
            prev_month_end = current_start - timedelta(days=1) 
            prev_start = prev_month_end.replace(day=1)
            prev_end = prev_month_end
            
            logger.info(f"Monthly periods - Current: {current_start} to {current_end}, Previous: {prev_start} to {prev_end}")
            return current_start, current_end, prev_start, prev_end
            
        elif period == "weekly":
            last_sunday = today - timedelta(days=today.weekday() + 1)
            current_start = last_sunday - timedelta(days=6)
            current_end = last_sunday
            
            prev_start = current_start - timedelta(days=7)
            prev_end = current_end - timedelta(days=7)
            
            logger.info(f"Weekly periods - Current: {current_start} to {current_end}, Previous: {prev_start} to {prev_end}")
            return current_start, current_end, prev_start, prev_end
            
        elif period == "daily":
            yesterday = today - timedelta(days=1)
            current_start = yesterday
            current_end = yesterday
            
            prev_start = yesterday - timedelta(days=7)
            prev_end = prev_start
            
            logger.info(f"Daily periods - Current: {current_start}, Previous: {prev_start}")
            return current_start, current_end, prev_start, prev_end
        
        logger.warning(f"Unknown period type: {period}")
        return None, None, None, None

    async def get_period_data(self, session: AsyncSession, product_ids: List[int], start_date: date, end_date: date) -> Dict[str, Any]:
        """Получает все данные для указанного периода"""
        review_count = await self._review_repo.count_by_product_and_period(
            session, product_ids, start_date, end_date
        )
        
        tonality_counts = await self._review_repo.get_tonality_counts_by_product_and_period(
            session, product_ids, start_date, end_date
        )
        
        avg_rating = await self._review_repo.get_avg_rating_by_products(
            session, product_ids, start_date, end_date
        )
        
        return {
            'review_count': review_count,
            'tonality_counts': tonality_counts,
            'avg_rating': avg_rating,
            'total_reviews': sum(tonality_counts.values())
        }

    async def check_and_generate_notifications(self, session: AsyncSession):
        """Основной метод проверки и генерации уведомлений"""
        configs = await self._config_repo.get_active_configs(session)
        logger.info(f"Checking {len(configs)} active notification configs")
        
        notifications_generated = 0
        
        for config in configs:
            try:
                product = await self._product_repo.get_by_id(session, config.product_id)
                if not product:
                    logger.warning(f"Product {config.product_id} not found for config {config.id}")
                    continue

                descendants = await self._product_repo.get_all_descendants(session, config.product_id)
                product_ids = [config.product_id] + [d.id for d in descendants]
                
                logger.info(f"Checking config {config.id} for product {product.name} (IDs: {product_ids})")

                current_start, current_end, prev_start, prev_end = self.get_comparison_periods(config.period)
                
                if not all([current_start, current_end, prev_start, prev_end]):
                    logger.warning(f"Could not determine periods for config {config.id} with period {config.period}")
                    continue

                current_data = await self.get_period_data(session, product_ids, current_start, current_end)
                prev_data = await self.get_period_data(session, product_ids, prev_start, prev_end)
                
                logger.info(f"Config {config.id} - Current: {current_data}, Previous: {prev_data}")

                notification_generated = await self.check_config_thresholds(
                    session, config, product, current_data, prev_data, current_start, current_end
                )
                
                if notification_generated:
                    notifications_generated += 1
                    
            except Exception as e:
                logger.error(f"Error processing config {config.id}: {str(e)}", exc_info=True)
                continue
        
        logger.info(f"Notification check completed. Generated {notifications_generated} notifications")

    async def check_config_thresholds(
        self, 
        session: AsyncSession, 
        config: NotificationConfig, 
        product: Any,
        current_data: Dict[str, Any], 
        prev_data: Dict[str, Any],
        current_start: date,
        current_end: date
    ) -> bool:
        """Проверяет конкретную конфигурацию на превышение порогов"""
        
        message = None
        period_label = self.get_period_label(config.period, current_start, current_end)

        if config.notification_type == NotificationType.REVIEW_SPIKE:
            message = await self.check_review_spike(config, product, current_data, prev_data, period_label)
            
        elif config.notification_type == NotificationType.SENTIMENT_DECLINE:
            message = await self.check_sentiment_decline(config, product, current_data, prev_data, period_label)
            
        elif config.notification_type == NotificationType.RATING_DROP:
            message = await self.check_rating_drop(config, product, current_data, prev_data, period_label)
            
        elif config.notification_type == NotificationType.NEGATIVE_SPIKE:
            message = await self.check_negative_spike(config, product, current_data, prev_data, period_label)

        if message:
            await self.generate_notification(session, config.user_id, message, config.notification_type)
            await self._audit_log_repo.save(session, config.user_id, 
                                          f"Generated {config.notification_type} notification for {product.name}")
            return True
        
        return False

    async def check_review_spike(self, config: NotificationConfig, product: Any, 
                               current_data: Dict, prev_data: Dict, period_label: str) -> Optional[str]:
        """Проверка резкого роста количества отзывов"""
        current_count = current_data['review_count']
        prev_count = prev_data['review_count']
        
        if prev_count > 0:
            change_percent = ((current_count - prev_count) / prev_count) * 100
            if change_percent > config.threshold:
                return (f"📈 Резкий рост отзывов по продукту '{product.name}' "
                       f"за {period_label}: +{change_percent:.1f}% "
                       f"({prev_count} → {current_count} отзывов)")
        
        return None

    async def check_sentiment_decline(self, config: NotificationConfig, product: Any,
                                    current_data: Dict, prev_data: Dict, period_label: str) -> Optional[str]:
        """Проверка ухудшения тональности"""
        current_positive = current_data['tonality_counts'].get('positive', 0)
        prev_positive = prev_data['tonality_counts'].get('positive', 0)
        current_total = current_data['total_reviews']
        prev_total = prev_data['total_reviews']
        
        if prev_total > 0 and current_total > 0:
            current_positive_percent = (current_positive / current_total) * 100
            prev_positive_percent = (prev_positive / prev_total) * 100
            change = current_positive_percent - prev_positive_percent
            
            if change < -config.threshold:
                return (f"📉 Ухудшение тональности по продукту '{product.name}' "
                       f"за {period_label}: доля позитивных отзывов снизилась на {abs(change):.1f}% "
                       f"({prev_positive_percent:.1f}% → {current_positive_percent:.1f}%)")
        
        return None

    async def check_rating_drop(self, config: NotificationConfig, product: Any,
                              current_data: Dict, prev_data: Dict, period_label: str) -> Optional[str]:
        """Проверка падения рейтинга"""
        current_rating = current_data['avg_rating'] or 0
        prev_rating = prev_data['avg_rating'] or 0
        
        if prev_rating > 0 and current_rating > 0:
            change = current_rating - prev_rating
            if change < -config.threshold:
                return (f"⭐ Падение рейтинга продукта '{product.name}' "
                       f"за {period_label}: {prev_rating:.2f} → {current_rating:.2f} "
                       f"(снижение на {abs(change):.2f} баллов)")
        
        return None

    async def check_negative_spike(self, config: NotificationConfig, product: Any,
                                 current_data: Dict, prev_data: Dict, period_label: str) -> Optional[str]:
        """Проверка роста негативных отзывов"""
        current_negative = current_data['tonality_counts'].get('negative', 0)
        prev_negative = prev_data['tonality_counts'].get('negative', 0)
        
        if prev_negative > 0:
            change_percent = ((current_negative - prev_negative) / prev_negative) * 100
            if change_percent > config.threshold:
                return (f"🔴 Рост негативных отзывов по продукту '{product.name}' "
                       f"за {period_label}: +{change_percent:.1f}% "
                       f"({prev_negative} → {current_negative} негативных отзывов)")
        elif current_negative > 0 and prev_negative == 0:
            return (f"🔴 Появились негативные отзывы по продукту '{product.name}' "
                   f"за {period_label}: {current_negative} негативных отзывов")
        
        return None

    def get_period_label(self, period: str, start_date: date, end_date: str) -> str:
        """Генерирует читаемую метку периода"""
        if period == "daily":
            return start_date.strftime("%d.%m.%Y")
        elif period == "weekly":
            return f"неделю {start_date.strftime('%d.%m')}-{end_date.strftime('%d.%m.%Y')}"
        elif period == "monthly":
            return start_date.strftime("%B %Y").lower()
        return f"период {start_date} - {end_date}"

    async def generate_notification(
        self, session: AsyncSession, user_id: int, message: str, notification_type: str
    ) -> Notification:
        notification = Notification(
            user_id=user_id, 
            message=message, 
            type=notification_type
        )
        saved = await self._notification_repo.save(session, notification)
        logger.info(f"Generated notification for user {user_id}: {message}")
        return saved

    async def get_user_notifications(self, session: AsyncSession, user_id: int, is_read: bool = False) -> List[Notification]:
        return await self._notification_repo.get_by_user_id(session, user_id, is_read)

    async def mark_as_read(self, session: AsyncSession, notification_id: int, user_id: int) -> bool:
        return await self._notification_repo.update_read_status(session, notification_id, user_id)

    async def delete_notification(self, session: AsyncSession, notification_id: int, user_id: int) -> bool:
        return await self._notification_repo.delete(session, notification_id, user_id)