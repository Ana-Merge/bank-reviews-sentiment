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
    Product, Review, Cluster, ReviewCluster, MonthlyStats, ClusterStats, Notification, AuditLog, NotificationConfig, ReviewProduct,
    ProductType, ClientType, Sentiment, NotificationType
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def seed_db():
    try:
        db_url = os.getenv("DB_URL")
        if not db_url:
            try:
                with open("/run/secrets/db_url", "r") as f:
                    db_url = f.read().strip()
            except FileNotFoundError:
                raise ValueError("DB_URL не задан или не найден")
        
        if not db_url:
            raise ValueError("DB_URL не задан")
        
        db_manager = DatabaseManager(db_url)
        await db_manager.initialize()
        async_session = db_manager.async_session

        async with async_session() as session:
            async with session.begin():
                user_exists = await session.execute(select(User).where(User.username == "admin"))
                if user_exists.scalar_one_or_none():
                    logger.info("Database already seeded, skipping...")
                    return

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
                user_ids = {u.username: u.id for u in users}

                # category = Product(name="Карты", type=ProductType.CATEGORY, level=0, client_type=ClientType.BOTH, description="Общие карты")
                # session.add(category)
                # await session.flush()
                # category_id = category.id 

                # subcategory = Product(name="Кредитные карты", parent_id=category_id, level=1, type=ProductType.SUBCATEGORY, client_type=ClientType.BOTH, description="Общие кредитные карты")
                # session.add(subcategory)
                # await session.flush()
                # subcategory_id = subcategory.id

                # products = [
                #     Product(name="карта \"Мир\"", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                #     Product(name="Mir Supreme", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                #     Product(name="Для школьников", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                #     Product(name="карта \"Мир2\"", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                #     Product(name="Mir Supreme2", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                #     Product(name="Для школьников2", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                #     Product(name="Золотая карта", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                #     Product(name="Платиновая карта", parent_id=subcategory_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                # ]
                # session.add_all(products)
                # await session.flush()
                # product_ids = {p.name: p.id for p in products}

                # subcategory_debit = Product(name="Дебетовые карты", parent_id=category_id, level=1, type=ProductType.SUBCATEGORY, client_type=ClientType.BOTH, description="Общие дебетовые карты")
                # session.add(subcategory_debit)
                # await session.flush()
                # subcategory_debit_id = subcategory_debit.id

                # products_debit = [
                #     Product(name="карта gazpromDEB", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                #     Product(name="DEB Supreme", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                #     Product(name="Для dep школьников", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                #     Product(name="карта gazpromDEBNEW", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                #     Product(name="Golden Deb", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                #     Product(name="Deb New Brilliant", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                #     Product(name="Золотая golden Deb", parent_id=subcategory_debit_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                # ]
                # session.add_all(products_debit)
                # await session.flush()
                # product_ids.update({p.name: p.id for p in products_debit})

                # category_accounts = Product(name="Вклады и счета", type=ProductType.CATEGORY, level=0, client_type=ClientType.BOTH, description="Общие вклады и счета")
                # session.add(category_accounts)
                # await session.flush()
                # category_accounts_id = category_accounts.id

                # subcategory_savings = Product(name="Накопительные вклады", parent_id=category_accounts_id, level=1, type=ProductType.SUBCATEGORY, client_type=ClientType.BOTH, description="Общие накопительные вклады и счета")
                # session.add(subcategory_savings)
                # await session.flush()
                # subcategory_savings_id = subcategory_savings.id

                # products_savings = [
                #     Product(name="Вклад лучшие проценты", parent_id=subcategory_savings_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                #     Product(name="Вклад Накопилка", parent_id=subcategory_savings_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BUSINESS),
                #     Product(name="Накопление для школьников", parent_id=subcategory_savings_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.INDIVIDUAL),
                #     Product(name="Винстон черчиль", parent_id=subcategory_savings_id, level=2, type=ProductType.PRODUCT, client_type=ClientType.BOTH),
                # ]
                # session.add_all(products_savings)
                # await session.flush()
                # product_ids.update({p.name: p.id for p in products_savings})

                # clusters = [
                #     Cluster(name="Скорость и удобство оформления"),
                #     Cluster(name="Комиссии и тарифы"),
                #     Cluster(name="Лимиты и ограничения"),
                #     Cluster(name="Кешбек и бонусы"),
                #     Cluster(name="Техническая поддержка"),
                #     Cluster(name="Безопасность"),
                #     Cluster(name="Доступность банкоматов"),
                #     Cluster(name="Дизайн карты"),
                #     Cluster(name="Интеграция с другими сервисами"),
                #     Cluster(name="Дополнительные услуги"),
                # ]
                # session.add_all(clusters)
                # await session.flush()
                # logger.info("Clusters seeded")

                # reviews_list = []
                # product_mappings = {}
                # product_id_list = list(product_ids.values())
                # sources = ['Banki.ru', 'App Store', 'Google Play']
                # sentiments = list(Sentiment)

                # for _ in range(2000):
                #     num_products = random.randint(1, 3)
                #     selected_product_ids = random.sample(product_id_list, k=num_products)
                #     date_random = date(2025, random.randint(1, 9), random.randint(1, 28))
                #     rating = random.randint(1, 5)
                #     source = random.choice(sources)
                #     text = f"Review text mentioning products {', '.join(map(str, selected_product_ids))} on {date_random}"
                    
                #     # Создаем отзыв без тональности (будет в review_products)
                #     review = Review(text=text, date=date_random, rating=rating, source=source, created_at=datetime.now())
                #     product_mappings[id(review)] = selected_product_ids
                #     reviews_list.append(review)

                # session.add_all(reviews_list)
                # await session.flush()
                # logger.info("Reviews seeded")

                # # Создаем связи в review_products с тональностями
                # for review in reviews_list:
                #     for pid in product_mappings[id(review)]:
                #         # Для каждого продукта назначаем случайную тональность
                #         sentiment = random.choice(sentiments)
                #         sentiment_score = round(random.uniform(-1.0, 1.0), 2)
                        
                #         rp = ReviewProduct(
                #             review_id=review.id, 
                #             product_id=pid,
                #             sentiment=sentiment,
                #             sentiment_score=sentiment_score
                #         )
                #         session.add(rp)
                # await session.flush()
                # logger.info("ReviewProduct associations seeded")


                # review_clusters = []
                # for review in reviews_list:
                #     cluster = random.choice(clusters)
                #     review_cluster = ReviewCluster(
                #         review_id=review.id,
                #         cluster_id=cluster.id,
                #         topic_weight=random.uniform(0.5, 1.0),
                #         sentiment_contribution=random.choice(list(Sentiment)),
                #         created_at=datetime.now()
                #     )
                #     review_clusters.append(review_cluster)
                # session.add_all(review_clusters)
                # await session.flush()
                # logger.info("Review clusters seeded")

                # monthly_stats = []
                # for product_id in product_id_list:
                #     for month in range(1, 10):
                #         month_start = date(2025, month, 1)
                #         month_end = month_start + timedelta(days=28)
                #         relevant_reviews = [r for r in reviews_list if product_id in product_mappings[id(r)] and month_start <= r.date <= month_end]
                #         review_count = len(relevant_reviews)
                #         if review_count == 0:
                #             continue
                #         avg_rating = sum(r.rating for r in relevant_reviews) / review_count
                #         positive_count = 0
                #         neutral_count = 0
                #         negative_count = 0
                #         for review in relevant_reviews:
                #             # Получаем тональности из review_products для этого отзыва и продукта
                #             sentiment_stmt = select(ReviewProduct.sentiment).where(
                #                 ReviewProduct.review_id == review.id,
                #                 ReviewProduct.product_id == product_id
                #             )
                #             sentiment_result = await session.execute(sentiment_stmt)
                #             sentiment = sentiment_result.scalar()
                #             if sentiment == Sentiment.POSITIVE:
                #                 positive_count += 1
                #             if sentiment == Sentiment.NEUTRAL:
                #                 neutral_count += 1
                #             if sentiment == Sentiment.NEGATIVE:
                #                 negative_count += 1
                #         sentiment_trend = (positive_count - negative_count) / review_count if review_count > 0 else 0
                #         prev_month_start = month_start - timedelta(days=28)
                #         prev_review_count = len([r for r in reviews_list if product_id in product_mappings[id(r)] and prev_month_start <= r.date < month_start])
                #         count_change_percent = ((review_count - prev_review_count) / prev_review_count * 100) if prev_review_count > 0 else 0

                #         stats = MonthlyStats(
                #             product_id=product_id,
                #             month=month_start,
                #             review_count=review_count,
                #             count_change_percent=count_change_percent,
                #             avg_rating=avg_rating,
                #             positive_count=positive_count,
                #             neutral_count=neutral_count,
                #             negative_count=negative_count,
                #             sentiment_trend=sentiment_trend
                #         )
                #         monthly_stats.append(stats)
                # session.add_all(monthly_stats)
                # await session.flush()
                # logger.info("Monthly stats seeded")

                # cluster_stats = []
                # for cluster in clusters:
                #     for product_id in product_id_list:
                #         for month in range(1, 10):
                #             month_start = date(2025, month, 1)
                #             month_end = month_start + timedelta(days=28)
                #             relevant_review_clusters = [rc for rc in review_clusters if rc.cluster_id == cluster.id]
                #             relevant_rc = [rc for rc in relevant_review_clusters if any(r.id == rc.review_id and product_id in product_mappings[id(r)] and month_start <= r.date <= month_end for r in reviews_list)]
                #             weighted_review_count = sum(rc.topic_weight for rc in relevant_rc) if relevant_rc else 0
                #             if weighted_review_count == 0:
                #                 continue
                #             sentiments = [next((r.sentiment for r in reviews_list if r.id == rc.review_id), None) for rc in relevant_rc]
                #             positive_count = sentiments.count(Sentiment.POSITIVE)
                #             neutral_count = sentiments.count(Sentiment.NEUTRAL)
                #             negative_count = sentiments.count(Sentiment.NEGATIVE)
                #             total = len(relevant_rc)
                #             positive_percent = (positive_count / total) * 100 if total > 0 else 0
                #             neutral_percent = (neutral_count / total) * 100 if total > 0 else 0
                #             negative_percent = (negative_count / total) * 100 if total > 0 else 0
                #             avg_rating = sum(next((r.rating for r in reviews_list if r.id == rc.review_id), 0) for rc in relevant_rc) / total if total > 0 else 0

                #             stats = ClusterStats(
                #                 cluster_id=cluster.id,
                #                 product_id=product_id,
                #                 month=month_start,
                #                 weighted_review_count=weighted_review_count,
                #                 positive_percent=positive_percent,
                #                 neutral_percent=neutral_percent,
                #                 negative_percent=negative_percent,
                #                 avg_rating=avg_rating
                #             )
                #             cluster_stats.append(stats)
                # session.add_all(cluster_stats)
                # await session.flush()
                # logger.info("Cluster stats seeded")

                # # ========== ОБНОВЛЕННЫЙ БЛОК УВЕДОМЛЕНИЙ ==========
                
                # # Создаем реалистичные даты для уведомлений
                # now = datetime.now()
                # yesterday = now - timedelta(days=1)
                # two_days_ago = now - timedelta(days=2)
                # week_ago = now - timedelta(days=7)
                # two_weeks_ago = now - timedelta(days=14)

                # notifications = [
                #     # Уведомления для manager (прочитанные и непрочитанные)
                #     Notification(user_id=user_ids["manager"], message="📈 Резкий рост отзывов по карте 'Мир' за вчера: +150% (2 → 5 отзывов)", type=NotificationType.REVIEW_SPIKE, is_read=False, created_at=now),
                #     Notification(user_id=user_ids["manager"], message="📉 Ухудшение тональности по карте 'Мир' за неделю 06.10-12.10: доля позитивных отзывов снизилась на 25%", type=NotificationType.SENTIMENT_DECLINE, is_read=True, created_at=yesterday),
                #     Notification(user_id=user_ids["manager"], message="⭐ Падение рейтинга продукта 'Золотая карта' за сентябрь 2025: 4.8 → 4.5 (снижение на 0.3 баллов)", type=NotificationType.RATING_DROP, is_read=False, created_at=two_days_ago),
                #     Notification(user_id=user_ids["manager"], message="🔴 Рост негативных отзывов по 'Mir Supreme' за 14.10.2025: +80% (5 → 9 негативных отзывов)", type=NotificationType.NEGATIVE_SPIKE, is_read=True, created_at=week_ago),
                    
                #     # Уведомления для manager2
                #     Notification(user_id=user_ids["manager2"], message="📈 Высокий спрос на 'Вклад лучшие проценты': рост отзывов на 120% за неделю", type=NotificationType.REVIEW_SPIKE, is_read=False, created_at=now),
                #     Notification(user_id=user_ids["manager2"], message="📉 Пользователи жалуются на комиссии по 'DEB Supreme': негатив вырос на 45%", type=NotificationType.SENTIMENT_DECLINE, is_read=False, created_at=yesterday),
                #     Notification(user_id=user_ids["manager2"], message="⭐ Рейтинг 'Для школьников' упал с 4.2 до 3.8 за месяц", type=NotificationType.RATING_DROP, is_read=True, created_at=two_days_ago),
                #     Notification(user_id=user_ids["manager2"], message="🔴 Критический рост негатива по 'Платиновая карта': +200% за две недели", type=NotificationType.NEGATIVE_SPIKE, is_read=False, created_at=week_ago),
                #     Notification(user_id=user_ids["manager2"], message="📈 Новые отзывы по 'Карта gazpromDEB' появились после обновления условий", type=NotificationType.REVIEW_SPIKE, is_read=True, created_at=two_weeks_ago),
                    
                #     # Уведомления для manager3  
                #     Notification(user_id=user_ids["manager3"], message="📉 Снижение удовлетворенности по 'Технической поддержке' на 35%", type=NotificationType.SENTIMENT_DECLINE, is_read=False, created_at=now),
                #     Notification(user_id=user_ids["manager3"], message="⭐ 'Вклад Накопилка' получил низкие оценки за октябрь: 3.5/5", type=NotificationType.RATING_DROP, is_read=False, created_at=yesterday),
                #     Notification(user_id=user_ids["manager3"], message="🔴 Много жалоб на 'Лимиты и ограничения' по дебетовым картам", type=NotificationType.NEGATIVE_SPIKE, is_read=True, created_at=two_days_ago),
                #     Notification(user_id=user_ids["manager3"], message="📈 Активное обсуждение 'Кешбек и бонусы' в социальных сетях", type=NotificationType.REVIEW_SPIKE, is_read=False, created_at=week_ago),
                    
                #     # Уведомления для admin
                #     Notification(user_id=user_ids["admin"], message="📊 Еженедельный отчет: 45 новых уведомлений сгенерировано системой", type=NotificationType.REVIEW_SPIKE, is_read=False, created_at=now),
                #     Notification(user_id=user_ids["admin"], message="⚠️ Внимание! Высокий уровень негатива по продуктам категории 'Карты'", type=NotificationType.NEGATIVE_SPIKE, is_read=False, created_at=yesterday),
                #     Notification(user_id=user_ids["admin"], message="📈 Топ продукт недели: 'Винстон черчиль' - рост отзывов на 85%", type=NotificationType.REVIEW_SPIKE, is_read=True, created_at=two_days_ago),
                #     Notification(user_id=user_ids["admin"], message="📉 Требуется внимание: падение рейтинга по 3 продуктам одновременно", type=NotificationType.RATING_DROP, is_read=False, created_at=week_ago),
                # ]
                # session.add_all(notifications)
                # await session.flush()
                # logger.info("Enhanced notifications seeded")

                # # ========== ОБНОВЛЕННЫЙ БЛОК КОНФИГУРАЦИЙ УВЕДОМЛЕНИЙ ==========
                
                # configs = [
                #     # Конфигурации для manager (все периоды)
                #     NotificationConfig(
                #         user_id=user_ids["manager"],
                #         product_id=product_ids["карта \"Мир\""],
                #         notification_type=NotificationType.REVIEW_SPIKE,
                #         threshold=20.0,
                #         period="monthly",
                #         active=True,
                #         created_at=now - timedelta(days=30)
                #     ),
                #     NotificationConfig(
                #         user_id=user_ids["manager"],
                #         product_id=product_ids["карта \"Мир\""],
                #         notification_type=NotificationType.NEGATIVE_SPIKE,
                #         threshold=30.0,
                #         period="weekly",
                #         active=True,
                #         created_at=now - timedelta(days=15)
                #     ),
                #     NotificationConfig(
                #         user_id=user_ids["manager"],
                #         product_id=product_ids["Золотая карта"],
                #         notification_type=NotificationType.RATING_DROP,
                #         threshold=0.3,
                #         period="daily",
                #         active=True,
                #         created_at=now - timedelta(days=7)
                #     ),
                    
                #     # Конфигурации для manager2
                #     NotificationConfig(
                #         user_id=user_ids["manager2"],
                #         product_id=product_ids["Mir Supreme"],
                #         notification_type=NotificationType.RATING_DROP,
                #         threshold=0.5,
                #         period="monthly",
                #         active=True,
                #         created_at=now - timedelta(days=25)
                #     ),
                #     NotificationConfig(
                #         user_id=user_ids["manager2"],
                #         product_id=product_ids["Вклад лучшие проценты"],
                #         notification_type=NotificationType.REVIEW_SPIKE,
                #         threshold=50.0,
                #         period="weekly",
                #         active=True,
                #         created_at=now - timedelta(days=10)
                #     ),
                #     NotificationConfig(
                #         user_id=user_ids["manager2"],
                #         product_id=product_ids["Платиновая карта"],
                #         notification_type=NotificationType.NEGATIVE_SPIKE,
                #         threshold=100.0,
                #         period="daily",
                #         active=False,  # Неактивная конфигурация
                #         created_at=now - timedelta(days=5)
                #     ),
                    
                #     # Конфигурации для manager3
                #     NotificationConfig(
                #         user_id=user_ids["manager3"],
                #         product_id=product_ids["Для школьников"],
                #         notification_type=NotificationType.SENTIMENT_DECLINE,
                #         threshold=25.0,
                #         period="monthly",
                #         active=True,
                #         created_at=now - timedelta(days=20)
                #     ),
                #     NotificationConfig(
                #         user_id=user_ids["manager3"],
                #         product_id=product_ids["DEB Supreme"],
                #         notification_type=NotificationType.NEGATIVE_SPIKE,
                #         threshold=40.0,
                #         period="weekly",
                #         active=True,
                #         created_at=now - timedelta(days=12)
                #     ),
                    
                #     # Конфигурации для admin
                #     NotificationConfig(
                #         user_id=user_ids["admin"],
                #         product_id=product_ids["карта gazpromDEB"],
                #         notification_type=NotificationType.REVIEW_SPIKE,
                #         threshold=10.0,
                #         period="daily",
                #         active=True,
                #         created_at=now - timedelta(days=3)
                #     ),
                #     NotificationConfig(
                #         user_id=user_ids["admin"],
                #         product_id=product_ids["Вклад Накопилка"],
                #         notification_type=NotificationType.RATING_DROP,
                #         threshold=0.2,
                #         period="monthly",
                #         active=True,
                #         created_at=now - timedelta(days=28)
                #     ),
                # ]
                # session.add_all(configs)
                # await session.flush()
                # logger.info("Enhanced notification configs seeded")

                audit_logs = [
                    AuditLog(user_id=user_ids["admin"], action="User login", timestamp=datetime.now()),
                    AuditLog(user_id=user_ids["manager"], action="Product stats viewed", timestamp=datetime.now() - timedelta(hours=1)),
                    AuditLog(user_id=user_ids["manager2"], action="Notification read", timestamp=datetime.now() - timedelta(hours=2)),
                    AuditLog(user_id=user_ids["manager3"], action="Notification settings updated", timestamp=datetime.now() - timedelta(hours=3)),
                    AuditLog(user_id=user_ids["admin"], action="System notification check completed", timestamp=datetime.now() - timedelta(hours=4)),
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