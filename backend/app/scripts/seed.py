import asyncio
import os
import random
import logging
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from datetime import date, datetime, timedelta
from app.core.db_manager import DatabaseManager, Base
from app.models.user_models import User
from app.models.models import (
    Product, Review, Cluster, ReviewCluster, MonthlyStats, ClusterStats, Notification, AuditLog, NotificationConfig,
    ProductType, ClientType, Sentiment, NotificationType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def seed_db():
    try:
        # Чтение DB_URL
        db_url = os.getenv("DB_URL")
        if not db_url:
            try:
                with open("/run/secrets/db_url", "r") as f:
                    db_url = f.read().strip()
            except FileNotFoundError:
                raise ValueError("DB_URL not set or not found")
        
        if not db_url:
            raise ValueError("DB_URL is not set")
        
        # Используем DatabaseManager
        db_manager = DatabaseManager(db_url)
        await db_manager.initialize()
        async_session = db_manager.async_session

        async with async_session() as session:
            async with session.begin():
                # Проверка на наличие данных
                user_exists = await session.execute(select(User).where(User.username == "admin"))
                if user_exists.scalar_one_or_none():
                    logger.info("Database already seeded, skipping...")
                    return

                # Пользователи
                admin_hash = pwd_context.hash("admin")
                manager_hash = pwd_context.hash("manager")
                manager_hash2 = pwd_context.hash("manager2")
                manager_hash3 = pwd_context.hash("manager3")
                users = [
                    User(username="admin", password_hash=admin_hash, role="admin"),
                    User(username="manager", password_hash=manager_hash, role="manager"),
                    User(username="manager2", password_hash=manager_hash2, role="manager"),
                    User(username="manager3", password_hash=manager_hash3, role="manager"),
                ]
                session.add_all(users)
                await session.flush()
                # Сохраняем IDs пользователей
                user_ids = {u.username: u.id for u in users}

                # Продукты
                category = Product(name="Карты", type=ProductType.CATEGORY, level=0, client_type=ClientType.BOTH, description="Общие карты")
                session.add(category)
                await session.flush()
                category_id = category.id  # ID 1

                subcategory = Product(name="Кредитные карты", parent_id=category_id, level=1, type=ProductType.SUBCATEGORY, client_type=ClientType.BOTH, description="Общие кредитные карты")
                session.add(subcategory)
                await session.flush()
                subcategory_id = subcategory.id  # ID 2

                products = [
                    Product(name="карта \"Мир\"", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                    Product(name="Mir Supreme", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                    Product(name="Для школьников", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                    Product(name="карта \"Мир2\"", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                    Product(name="Mir Supreme2", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                    Product(name="Для школьников2", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                    Product(name="Золотая карта", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                    Product(name="Платиновая карта", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                ]
                session.add_all(products)
                await session.flush()
                product_ids = {p.name: p.id for p in products}

                subcategory_debit = Product(name="Дебетовые карты", parent_id=category_id, level=1, type=ProductType.SUBCATEGORY, client_type=ClientType.BOTH, description="Общие дебетовые карты")
                session.add(subcategory_debit)
                await session.flush()
                subcategory_debit_id = subcategory_debit.id

                products_debit = [
                    Product(name="карта gazpromDEB", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                    Product(name="DEB Supreme", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                    Product(name="Для dep школьников", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                    Product(name="карта gazpromDEBNEW", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                    Product(name="Golden Deb", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                    Product(name="Deb New Brilliant", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                    Product(name="Золотая golden Deb", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                ]
                session.add_all(products_debit)
                await session.flush()
                product_ids.update({p.name: p.id for p in products_debit})

                category_accounts = Product(name="Вклады и счета", type=ProductType.CATEGORY, level=0, client_type=ClientType.BOTH, description="Общие вклады и счета")
                session.add(category_accounts)
                await session.flush()
                category_accounts_id = category_accounts.id

                subcategory_savings = Product(name="Накопительные вклады", parent_id=category_accounts_id, level=1, type=ProductType.SUBCATEGORY, client_type=ClientType.BOTH, description="Общие накопительные вклады и счета")
                session.add(subcategory_savings)
                await session.flush()
                subcategory_savings_id = subcategory_savings.id

                products_savings = [
                    Product(name="Вклад лучшие проценты", parent_id=subcategory_savings_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                    Product(name="Вклад Накопилка", parent_id=subcategory_savings_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                    Product(name="Накопление для школьников", parent_id=subcategory_savings_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                    Product(name="Винстон черчиль", parent_id=subcategory_savings_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                ]
                session.add_all(products_savings)
                await session.flush()
                product_ids.update({p.name: p.id for p in products_savings})

                # Кластеры
                clusters = [
                    Cluster(name="Скорость и качество клиентской поддержки", keywords=["поддержка"], description="Работа поддержки"),
                    Cluster(name="Акции, бонусы, кэшбэк", keywords=["акция"], description="Промо"),
                    Cluster(name="Безопасность, надежность", keywords=["безопасность"], description="Защита данных"),
                    Cluster(name="Условия кредитования", keywords=["кредит"], description="Условия кредитов"),
                    Cluster(name="Комиссии", keywords=["комиссия"], description="Плата за услуги"),
                    Cluster(name="Мобильное приложение", keywords=["приложение"], description="Работа приложения"),
                    Cluster(name="Доброжелательность", keywords=["френдли"], description="Доброжелательность работников"),
                    Cluster(name="Отзывчивость", keywords=["отзывчивость"], description="Отзывчивость сотрудников"),
                ]
                session.add_all(clusters)
                await session.flush()

                # Отзывы
                product_id_list = [product_ids[name] for name in product_ids]
                reviews = []
                for product_id in product_id_list:
                    for month in range(1, 10):  # Январь-сентябрь 2025
                        for i in range(5):
                            sentiment = random.choice([Sentiment.POSITIVE, Sentiment.NEUTRAL, Sentiment.NEGATIVE])
                            sentiment_score = random.uniform(-1.0, 1.0) if sentiment == Sentiment.NEGATIVE else random.uniform(0.0, 1.0)
                            rating = random.randint(1, 5)
                            text = f"Отзыв {month}-{i} для продукта {product_id}: "
                            if sentiment == Sentiment.POSITIVE:
                                text += "Отличный сервис!"
                            elif sentiment == Sentiment.NEUTRAL:
                                text += "Нормально."
                            else:
                                text += "Плохо работает."
                            review = Review(
                                text=text,
                                date=date(2025, month, random.randint(1, 28)),
                                product_id=product_id,
                                rating=rating,
                                sentiment=sentiment,
                                sentiment_score=sentiment_score,
                                source=random.choice(["Banki.ru", "App Store", "Google Play"]),
                                created_at=datetime.now()
                            )
                            reviews.append(review)
                session.add_all(reviews)
                await session.flush()

                # Связи отзывов с кластерами
                review_clusters = []
                for review in reviews:
                    for cluster_id in range(1, len(clusters) + 1):
                        if random.random() < 0.3:
                            review_cluster = ReviewCluster(
                                review_id=review.id,
                                cluster_id=cluster_id,
                                topic_weight=random.uniform(0.5, 1.0)
                            )
                            review_clusters.append(review_cluster)
                session.add_all(review_clusters)
                await session.flush()
                logger.info("Review clusters seeded")

                # MonthlyStats
                monthly_stats = []
                for product_id in product_id_list:
                    for month in range(1, 10):
                        month_start = date(2025, month, 1)
                        month_end = month_start + timedelta(days=28)
                        review_count = len([r for r in reviews if r.product_id == product_id and month_start <= r.date <= month_end])
                        if review_count == 0:
                            continue
                        avg_rating = sum(r.rating for r in reviews if r.product_id == product_id and month_start <= r.date <= month_end) / review_count
                        positive_count = len([r for r in reviews if r.product_id == product_id and month_start <= r.date <= month_end and r.sentiment == Sentiment.POSITIVE])
                        neutral_count = len([r for r in reviews if r.product_id == product_id and month_start <= r.date <= month_end and r.sentiment == Sentiment.NEUTRAL])
                        negative_count = len([r for r in reviews if r.product_id == product_id and month_start <= r.date <= month_end and r.sentiment == Sentiment.NEGATIVE])
                        sentiment_trend = (positive_count - negative_count) / review_count if review_count > 0 else 0
                        prev_month_start = month_start - timedelta(days=28)
                        prev_review_count = len([r for r in reviews if r.product_id == product_id and prev_month_start <= r.date < month_start])
                        count_change_percent = ((review_count - prev_review_count) / prev_review_count * 100) if prev_review_count > 0 else 0

                        stats = MonthlyStats(
                            product_id=product_id,
                            month=month_start,
                            review_count=review_count,
                            count_change_percent=count_change_percent,
                            avg_rating=avg_rating,
                            positive_count=positive_count,
                            neutral_count=neutral_count,
                            negative_count=negative_count,
                            sentiment_trend=sentiment_trend
                        )
                        monthly_stats.append(stats)
                session.add_all(monthly_stats)
                await session.flush()
                logger.info("Monthly stats seeded")

                # ClusterStats
                cluster_stats = []
                for cluster in clusters:
                    for product_id in product_id_list:
                        for month in range(1, 10):
                            month_start = date(2025, month, 1)
                            month_end = month_start + timedelta(days=28)
                            relevant_reviews = [rc for rc in review_clusters if rc.cluster_id == cluster.id and any(r.product_id == product_id and month_start <= r.date <= month_end for r in reviews if r.id == rc.review_id)]
                            weighted_review_count = sum(rc.topic_weight for rc in relevant_reviews) if relevant_reviews else 0
                            if weighted_review_count == 0:
                                continue
                            positive_percent = (len([rc for rc in relevant_reviews if next((r.sentiment for r in reviews if r.id == rc.review_id), None) == Sentiment.POSITIVE]) / len(relevant_reviews)) * 100
                            neutral_percent = (len([rc for rc in relevant_reviews if next((r.sentiment for r in reviews if r.id == rc.review_id), None) == Sentiment.NEUTRAL]) / len(relevant_reviews)) * 100
                            negative_percent = (len([rc for rc in relevant_reviews if next((r.sentiment for r in reviews if r.id == rc.review_id), None) == Sentiment.NEGATIVE]) / len(relevant_reviews)) * 100
                            avg_rating = sum(next((r.rating for r in reviews if r.id == rc.review_id), 0) for rc in relevant_reviews) / len(relevant_reviews)

                            stats = ClusterStats(
                                cluster_id=cluster.id,
                                product_id=product_id,
                                month=month_start,
                                weighted_review_count=weighted_review_count,
                                positive_percent=positive_percent,
                                neutral_percent=neutral_percent,
                                negative_percent=negative_percent,
                                avg_rating=avg_rating
                            )
                            cluster_stats.append(stats)
                session.add_all(cluster_stats)
                await session.flush()
                logger.info("Cluster stats seeded")

                # Уведомления
                notifications = [
                    Notification(user_id=user_ids["manager"], message="Резкий скачок отзывов по карте Мир (+25%)!", type=NotificationType.REVIEW_SPIKE, is_read=False),
                    Notification(user_id=user_ids["manager"], message="Ухудшение тональности по комиссиям (-18%)", type=NotificationType.SENTIMENT_DECLINE, is_read=False),
                    Notification(user_id=user_ids["manager2"], message="Снижение рейтинга продукта Mir Supreme (-10%)", type=NotificationType.RATING_DROP, is_read=False),
                    Notification(user_id=user_ids["manager3"], message="Рост негативных отзывов (+30%)", type=NotificationType.NEGATIVE_SPIKE, is_read=False),
                ]
                session.add_all(notifications)
                await session.flush()
                logger.info("Notifications seeded")

                # NotificationConfig
                configs = [
                    NotificationConfig(
                        user_id=user_ids["manager"],
                        product_id=product_ids["карта \"Мир\""],
                        notification_type=NotificationType.REVIEW_SPIKE,
                        threshold=20.0,
                        period="monthly",
                        active=True,
                    ),
                    NotificationConfig(
                        user_id=user_ids["manager"],
                        product_id=product_ids["карта \"Мир\""],
                        notification_type=NotificationType.NEGATIVE_SPIKE,
                        threshold=30.0,
                        period="monthly",
                        active=True,
                    ),
                    NotificationConfig(
                        user_id=user_ids["manager2"],
                        product_id=product_ids["Mir Supreme"],
                        notification_type=NotificationType.RATING_DROP,
                        threshold=0.5,
                        period="monthly",
                        active=True,
                    ),
                ]
                session.add_all(configs)
                await session.flush()
                logger.info("Notification configs seeded")

                # Логи аудита
                audit_logs = [
                    AuditLog(user_id=user_ids["admin"], action="User login", timestamp=datetime.now()),
                    AuditLog(user_id=user_ids["manager"], action="Product stats viewed", timestamp=datetime.now() - timedelta(hours=1)),
                    AuditLog(user_id=user_ids["manager2"], action="Notification read", timestamp=datetime.now() - timedelta(hours=2)),
                ]
                session.add_all(audit_logs)
                await session.flush()
                logger.info("Audit logs seeded")

                await session.commit()
                logger.info("Database seeded successfully!")
        
        await db_manager.dispose()
    except Exception as e:
        logger.error(f"Seed failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(seed_db())