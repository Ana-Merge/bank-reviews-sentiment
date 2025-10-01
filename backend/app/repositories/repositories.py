from sqlalchemy import exists, func, select, and_, case, cast, Float, literal, Any, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func as sql_func
from typing import List, Optional, Dict
from datetime import date, datetime
from app.schemas.schemas import ProductTreeNode
from app.models.models import NotificationConfig, ReviewProduct

from app.models.models import (
    Product, Review, Cluster, ReviewCluster, MonthlyStats, ClusterStats, Notification, AuditLog, ReviewsForModel
)

class ProductRepository:
    async def get_by_id(self, session: AsyncSession, product_id: int) -> Product | None:
        statement = select(Product).where(Product.id == product_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_by_name(self, session: AsyncSession, name: str) -> Product | None:
        statement = select(Product).where(func.lower(Product.name) == func.lower(name))
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def exists_by_name(self, session: AsyncSession, name: str) -> bool:
        statement = select(exists().where(func.lower(Product.name) == func.lower(name)))
        result = await session.execute(statement)
        return result.scalar()

    async def get_all(
        self, session: AsyncSession, page: int = 0, size: int = 100, client_type: Optional[str] = None
    ) -> List[Product]:
        statement = select(Product).order_by(Product.name)
        if client_type:
            statement = statement.where(Product.client_type == client_type)
        statement = statement.offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()

    async def count_all(self, session: AsyncSession) -> int:
        statement = select(sql_func.count()).select_from(Product)
        result = await session.execute(statement)
        return result.scalar_one()

    async def save(self, session: AsyncSession, product: Product) -> Product:
        session.add(product)
        await session.flush()
        await session.commit()
        await session.refresh(product)
        return product

    async def update(self, session: AsyncSession, product: Product) -> Product:
        await session.merge(product)
        await session.commit()
        await session.refresh(product)
        return product

    async def delete(self, session: AsyncSession, product_id: int) -> bool:
        statement = select(Product).where(Product.id == product_id)
        result = await session.execute(statement)
        product = result.scalar_one_or_none()
        if product:
            await session.delete(product)
            await session.commit()
            return True
        return False
    
    async def get_all_descendants(self, session: AsyncSession, product_id: int) -> List[Product]:
        product = await self.get_by_id(session, product_id)
        if not product:
            return []

        base_query = select(
            Product.id.label('id'),
            Product.name.label('name'),
            Product.type.label('type'),
            Product.client_type.label('client_type'),
            Product.parent_id.label('parent_id'),
            Product.level.label('level')
        ).where(Product.id == product_id)

        recursive_cte = base_query.cte("recursive_products", recursive=True)
        recursive_part = select(
            Product.id,
            Product.name,
            Product.type,
            Product.client_type,
            Product.parent_id,
            Product.level
        ).join(recursive_cte, Product.parent_id == recursive_cte.c.id)
        recursive_cte = recursive_cte.union_all(recursive_part)

        statement = select(Product).join(recursive_cte, Product.id == recursive_cte.c.id).where(recursive_cte.c.id != product_id)
        result = await session.execute(statement)
        descendants = result.scalars().all()
        return descendants
    
    async def get_product_tree(self, session: AsyncSession, client_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получить все продукты в виде дерева (иерархия).
        """
        base_query = select(
            Product.id.label('id'),
            Product.name.label('name'),
            Product.type.label('type'),
            Product.client_type.label('client_type'),
            Product.parent_id.label('parent_id'),
            Product.level.label('level')
        ).where(Product.parent_id.is_(None))

        if client_type:
            base_query = base_query.where(Product.client_type == client_type)

        recursive_cte = base_query.cte("product_tree", recursive=True)

        recursive_part = select(
            Product.id,
            Product.name,
            Product.type,
            Product.client_type,
            Product.parent_id,
            Product.level 
        ).join(recursive_cte, Product.parent_id == recursive_cte.c.id)

        full_cte = recursive_cte.union_all(recursive_part)

        statement = select(
            full_cte.c.id,
            full_cte.c.name,
            full_cte.c.type,
            full_cte.c.client_type,
            full_cte.c.parent_id,
            full_cte.c.level
        ).order_by(full_cte.c.level, full_cte.c.name)

        result = await session.execute(statement)
        rows = result.all()

        tree = []
        node_dict = {row.id: dict(row._mapping) for row in rows}
        for row in rows:
            node = node_dict[row.id]
            if row.parent_id is None:
                tree.append(node)
            else:
                parent = node_dict[row.parent_id]
                if 'children' not in parent:
                    parent['children'] = []
                parent['children'].append(node)
        return tree

class ReviewRepository:
    async def add_products_to_review(self, session: AsyncSession, review_id: int, product_ids: List[int]):
        for pid in product_ids:
            rp = ReviewProduct(review_id=review_id, product_id=pid)
            session.add(rp)
        await session.flush()

    async def get_by_id(self, session: AsyncSession, review_id: int) -> Review | None:
        statement = select(Review).where(Review.id == review_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_all(
        self, session: AsyncSession, page: int = 0, size: int = 100, product_id: Optional[int] = None,
        start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> List[Review]:
        statement = select(Review).order_by(Review.created_at.desc())
        if product_id:
            statement = statement.join(ReviewProduct).where(ReviewProduct.product_id == product_id)
        if start_date:
            statement = statement.where(Review.date >= start_date)
        if end_date:
            statement = statement.where(Review.date <= end_date)
        statement = statement.offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()

    async def count_all(self, session: AsyncSession) -> int:
        statement = select(sql_func.count()).select_from(Review)
        result = await session.execute(statement)
        return result.scalar_one()

    async def save(self, session: AsyncSession, review: Review) -> Review:
        session.add(review)
        await session.flush()
        await session.commit()
        await session.refresh(review)
        return review

    async def update(self, session: AsyncSession, review: Review) -> Review:
        await session.merge(review)
        await session.commit()
        await session.refresh(review)
        return review

    async def delete(self, session: AsyncSession, review_id: int) -> bool:
        statement = select(Review).where(Review.id == review_id)
        result = await session.execute(statement)
        review = result.scalar_one_or_none()
        if review:
            await session.delete(review)
            await session.commit()
            return True
        return False

    async def bulk_create(self, session: AsyncSession, reviews: List[Review]) -> List[Review]:
        session.add_all(reviews)
        await session.flush()
        return reviews

    async def count_by_product_and_period(
        self, session: AsyncSession, product_ids: List[int], start_date: date, end_date: date, 
        source: Optional[str] = None, sentiment: Optional[str] = None
    ) -> int:
        if not product_ids:
            return 0
        statement = select(func.count(func.distinct(Review.id))).join(ReviewProduct).where(
            ReviewProduct.product_id.in_(product_ids),
            Review.date >= start_date,
            Review.date <= end_date
        )
        if source:
            statement = statement.where(Review.source == source)
        if sentiment:
            statement = statement.where(ReviewProduct.sentiment == sentiment)
        result = await session.execute(statement)
        return result.scalar() or 0

    async def get_tonality_counts_by_product_and_period(
        self, session: AsyncSession, product_ids: List[int], start_date: date, end_date: date, 
        source: Optional[str] = None
    ) -> Dict[str, int]:
        if not product_ids:
            return {"positive": 0, "neutral": 0, "negative": 0}
        statement = select(
            ReviewProduct.sentiment,  # Теперь берем из ReviewProduct
            func.count(func.distinct(Review.id)).label("count")
        ).join(ReviewProduct).where(
            ReviewProduct.product_id.in_(product_ids),
            Review.date >= start_date,
            Review.date <= end_date,
            ReviewProduct.sentiment.isnot(None)  # Убедимся, что тональность есть
        ).group_by(ReviewProduct.sentiment)
        if source:
            statement = statement.where(Review.source == source)
        result = await session.execute(statement)
        tonality = {row[0]: row[1] for row in result.all() if row[0]}
        return {
            "positive": tonality.get("positive", 0),
            "neutral": tonality.get("neutral", 0),
            "negative": tonality.get("negative", 0)
        }

    async def get_avg_rating_by_products(
        self, session: AsyncSession, product_ids: List[int], start_date: Optional[date] = None, 
        end_date: Optional[date] = None, source: Optional[str] = None
    ) -> float:
        if not product_ids:
            return 0.0
        statement = select(
            func.avg(Review.rating).label("avg_rating")
        ).join(ReviewProduct).where(
            ReviewProduct.product_id.in_(product_ids),
            Review.rating.isnot(None)
        )
        
        if start_date:
            statement = statement.where(Review.date >= start_date)
        if end_date:
            statement = statement.where(Review.date <= end_date)
        if source:
            statement = statement.where(Review.source == source)
            
        result = await session.execute(statement)
        avg_rating = result.scalar() or 0.0
        return float(avg_rating)

    async def get_reviews_by_product_and_period(
        self, session: AsyncSession, product_ids: List[int], start_date: date, end_date: date,
        page: int = 0, size: int = 100, cluster_id: Optional[int] = None, 
        sentiment: Optional[str] = None, source: Optional[str] = None
    ) -> List[Review]:
        statement = select(Review).join(ReviewProduct).where(
            ReviewProduct.product_id.in_(product_ids),
            Review.date >= start_date,
            Review.date <= end_date
        ).order_by(Review.date.desc()).offset(page * size).limit(size)
        
        if cluster_id:
            statement = statement.join(ReviewCluster).where(ReviewCluster.cluster_id == cluster_id)
        if sentiment:
            statement = statement.where(ReviewProduct.sentiment == sentiment)
        if source:
            statement = statement.where(Review.source == source)
            
        result = await session.execute(statement)
        return result.scalars().all()

class ClusterRepository:
    async def get_by_id(self, session: AsyncSession, cluster_id: int) -> Cluster | None:
        statement = select(Cluster).where(Cluster.id == cluster_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_by_name(self, session: AsyncSession, name: str) -> Cluster | None:
        statement = select(Cluster).where(func.lower(Cluster.name) == func.lower(name))
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def exists_by_name(self, session: AsyncSession, name: str) -> bool:
        statement = select(exists().where(func.lower(Cluster.name) == func.lower(name)))
        result = await session.execute(statement)
        return result.scalar()

    async def get_all(self, session: AsyncSession, page: int = 0, size: int = 100) -> List[Cluster]:
        statement = select(Cluster).order_by(Cluster.name).offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()

    async def count_all(self, session: AsyncSession) -> int:
        statement = select(sql_func.count()).select_from(Cluster)
        result = await session.execute(statement)
        return result.scalar_one()

    async def save(self, session: AsyncSession, cluster: Cluster) -> Cluster:
        session.add(cluster)
        await session.flush()
        await session.commit()
        await session.refresh(cluster)
        return cluster

    async def update(self, session: AsyncSession, cluster: Cluster) -> Cluster:
        await session.merge(cluster)
        await session.commit()
        await session.refresh(cluster)
        return cluster

    async def delete(self, session: AsyncSession, cluster_id: int) -> bool:
        statement = select(Cluster).where(Cluster.id == cluster_id)
        result = await session.execute(statement)
        cluster = result.scalar_one_or_none()
        if cluster:
            await session.delete(cluster)
            await session.commit()
            return True
        return False

class ReviewClusterRepository:
    async def get_by_id(self, session: AsyncSession, rc_id: int) -> ReviewCluster | None:
        statement = select(ReviewCluster).where(ReviewCluster.id == rc_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_all_by_review(self, session: AsyncSession, review_id: int) -> List[ReviewCluster]:
        statement = select(ReviewCluster).where(ReviewCluster.review_id == review_id)
        result = await session.execute(statement)
        return result.scalars().all()

    async def get_all_by_cluster(self, session: AsyncSession, cluster_id: int) -> List[ReviewCluster]:
        statement = select(ReviewCluster).where(ReviewCluster.cluster_id == cluster_id)
        result = await session.execute(statement)
        return result.scalars().all()

    async def save(self, session: AsyncSession, rc: ReviewCluster) -> ReviewCluster:
        session.add(rc)
        await session.flush()
        await session.commit()
        await session.refresh(rc)
        return rc

    async def delete(self, session: AsyncSession, rc_id: int) -> bool:
        statement = select(ReviewCluster).where(ReviewCluster.id == rc_id)
        result = await session.execute(statement)
        rc = result.scalar_one_or_none()
        if rc:
            await session.delete(rc)
            await session.commit()
            return True
        return False

    async def count_by_cluster_and_period(
        self, session: AsyncSession, cluster_id: int, product_ids: List[int], 
        start_date: date, end_date: date
    ) -> int:
        if not product_ids:
            return 0
        
        statement = select(func.count(func.distinct(Review.id))).select_from(ReviewCluster)\
            .join(Review).join(ReviewProduct).where(
                and_(
                    ReviewProduct.product_id.in_(product_ids),
                    Review.date >= start_date,
                    Review.date <= end_date,
                    ReviewCluster.cluster_id == cluster_id
                )
            )
        
        result = await session.execute(statement)
        return result.scalar() or 0

class MonthlyStatsRepository:
    async def get_by_product_and_month(self, session: AsyncSession, product_id: int, month: date) -> MonthlyStats | None:
        statement = select(MonthlyStats).where(
            and_(MonthlyStats.product_id == product_id, MonthlyStats.month == month)
        )
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_by_product_and_period(
        self, session: AsyncSession, product_id: int, start_date: date, end_date: date
    ) -> List[MonthlyStats]:
        statement = select(MonthlyStats).where(
            and_(
                MonthlyStats.product_id == product_id,
                MonthlyStats.month >= start_date,
                MonthlyStats.month <= end_date
            )
        ).order_by(MonthlyStats.month)
        result = await session.execute(statement)
        return result.scalars().all()

    async def save(self, session: AsyncSession, stats: MonthlyStats) -> MonthlyStats:
        session.add(stats)
        await session.flush()
        await session.commit()
        await session.refresh(stats)
        return stats

class ClusterStatsRepository:
    async def get_by_cluster_and_product_and_month(
        self, session: AsyncSession, cluster_id: int, product_id: int, month: Optional[date] = None
    ) -> ClusterStats | None:
        statement = select(ClusterStats).where(
            and_(ClusterStats.cluster_id == cluster_id, ClusterStats.product_id == product_id)
        )
        if month:
            statement = statement.where(ClusterStats.month == month)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def save(self, session: AsyncSession, stats: ClusterStats) -> ClusterStats:
        session.add(stats)
        await session.flush()
        await session.commit()
        await session.refresh(stats)
        return stats

class NotificationRepository:
    async def get_by_user_id(self, session: AsyncSession, user_id: int, is_read: Optional[bool] = None) -> List[Notification]:
        statement = select(Notification).where(Notification.user_id == user_id).order_by(Notification.created_at.desc())
        if is_read is not None:
            statement = statement.where(Notification.is_read == is_read)
        result = await session.execute(statement)
        return result.scalars().all()

    async def get_by_id(self, session: AsyncSession, notification_id: int, user_id: int) -> Notification | None:
        statement = select(Notification).where(
            and_(Notification.id == notification_id, Notification.user_id == user_id)
        )
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def save(self, session: AsyncSession, notification: Notification) -> Notification:
        session.add(notification)
        await session.flush()
        await session.commit()
        await session.refresh(notification)
        return notification

    async def update_read_status(self, session: AsyncSession, notification_id: int, user_id: int) -> bool:
        statement = select(Notification).where(
            and_(Notification.id == notification_id, Notification.user_id == user_id)
        )
        result = await session.execute(statement)
        notification = result.scalar_one_or_none()
        if notification:
            notification.is_read = True
            await session.commit()
            await session.refresh(notification)
            return True
        return False

    async def delete(self, session: AsyncSession, notification_id: int, user_id: int) -> bool:
        statement = select(Notification).where(
            and_(Notification.id == notification_id, Notification.user_id == user_id)
        )
        result = await session.execute(statement)
        notification = result.scalar_one_or_none()
        if notification:
            await session.delete(notification)
            await session.commit()
            return True
        return False

class NotificationConfigRepository:
    async def get_by_id(self, session: AsyncSession, config_id: int, user_id: int) -> NotificationConfig | None:
        statement = select(NotificationConfig).where(
            and_(NotificationConfig.id == config_id, NotificationConfig.user_id == user_id)
        )
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_all_by_user(self, session: AsyncSession, user_id: int) -> List[NotificationConfig]:
        statement = select(NotificationConfig).where(NotificationConfig.user_id == user_id)
        result = await session.execute(statement)
        return result.scalars().all()

    async def save(self, session: AsyncSession, config: NotificationConfig) -> NotificationConfig:
        session.add(config)
        await session.flush()
        await session.commit()
        await session.refresh(config)
        return config

    async def update(self, session: AsyncSession, config: NotificationConfig) -> NotificationConfig:
        await session.merge(config)
        await session.commit()
        await session.refresh(config)
        return config

    async def delete(self, session: AsyncSession, config_id: int, user_id: int) -> bool:
        statement = select(NotificationConfig).where(
            and_(NotificationConfig.id == config_id, NotificationConfig.user_id == user_id)
        )
        result = await session.execute(statement)
        config = result.scalar_one_or_none()
        if config:
            await session.delete(config)
            await session.commit()
            return True
        return False

    async def get_active_configs(self, session: AsyncSession) -> List[NotificationConfig]:
        statement = select(NotificationConfig).where(NotificationConfig.active == True)
        result = await session.execute(statement)
        return result.scalars().all()
    
class ReviewsForModelRepository:
    async def get_by_id(self, session: AsyncSession, review_id: int) -> ReviewsForModel | None:
        statement = select(ReviewsForModel).where(ReviewsForModel.id == review_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_all(self, session: AsyncSession, page: int = 0, size: int = 100, 
                     processed: Optional[bool] = None, category: Optional[str] = None) -> List[ReviewsForModel]:
        statement = select(ReviewsForModel).order_by(ReviewsForModel.parsed_at.desc())
        
        if processed is not None:
            statement = statement.where(ReviewsForModel.processed == processed)
        if category:
            statement = statement.where(ReviewsForModel.category == category)
            
        statement = statement.offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()

    async def get_unprocessed(self, session: AsyncSession, limit: int = 100) -> List[ReviewsForModel]:
        statement = select(ReviewsForModel).where(ReviewsForModel.processed == False).order_by(ReviewsForModel.parsed_at).limit(limit)
        result = await session.execute(statement)
        return result.scalars().all()

    async def count_all(self, session: AsyncSession) -> int:
        statement = select(sql_func.count()).select_from(ReviewsForModel)
        result = await session.execute(statement)
        return result.scalar_one()

    async def save(self, session: AsyncSession, review: ReviewsForModel) -> ReviewsForModel:
        session.add(review)
        await session.flush()
        await session.commit()
        await session.refresh(review)
        return review

    async def bulk_create(self, session: AsyncSession, reviews: List[ReviewsForModel]) -> List[ReviewsForModel]:
        session.add_all(reviews)
        await session.flush()
        return reviews

    async def save_parsed_reviews(self, session: AsyncSession, reviews: List[Dict], product: str) -> int:
        """Сохранить данные из парсера в базу"""
        reviews_to_save = []
        
        for review_data in reviews:
            review = ReviewsForModel(
                bank_name=review_data.get('bank_name', ''),
                bank_slug=review_data.get('bank_slug', ''),
                product_name=product,
                review_theme=review_data.get('review_theme', ''),
                rating=review_data.get('rating', ''),
                verification_status=review_data.get('verification_status', ''),
                review_text=review_data.get('review_text', ''),
                review_date=review_data.get('review_date', ''),
                review_timestamp=review_data.get('review_timestamp'),
                source_url=review_data.get('source_url', ''),
                parsed_at=datetime.utcnow(),
                processed=False
            )
            reviews_to_save.append(review)
        
        if reviews_to_save:
            await self.bulk_create(session, reviews_to_save)
            await session.commit()
        
        return len(reviews_to_save)

    async def mark_as_processed(self, session: AsyncSession, review_id: int) -> bool:
        statement = select(ReviewsForModel).where(ReviewsForModel.id == review_id)
        result = await session.execute(statement)
        review = result.scalar_one_or_none()
        if review:
            review.processed = True
            await session.commit()
            return True
        return False

    async def mark_bulk_as_processed(self, session: AsyncSession, review_ids: List[int]) -> bool:
        statement = update(ReviewsForModel).where(ReviewsForModel.id.in_(review_ids)).values(processed=True)
        await session.execute(statement)
        await session.commit()
        return True

    async def delete(self, session: AsyncSession, review_id: int) -> bool:
        statement = select(ReviewsForModel).where(ReviewsForModel.id == review_id)
        result = await session.execute(statement)
        review = result.scalar_one_or_none()
        if review:
            await session.delete(review)
            await session.commit()
            return True
        return False
    
    async def save_sravni_reviews(self, session: AsyncSession, reviews: List[Dict], bank_slug: str) -> int:
        """Сохранить данные из парсера sravni.ru в базу"""
        reviews_to_save = []
        
        for review_data in reviews:
            review = ReviewsForModel(
                bank_name=review_data.get('bank_name', ''),
                bank_slug=bank_slug,
                product_name=review_data.get('product_name', 'general'),
                review_theme=review_data.get('review_theme', ''),
                rating=review_data.get('rating', ''),
                verification_status=review_data.get('verification_status', ''),
                review_text=review_data.get('review_text', ''),
                review_date=review_data.get('review_date', ''),
                review_timestamp=review_data.get('review_timestamp'),
                source_url=review_data.get('source_url', ''),
                parsed_at=datetime.utcnow(),
                processed=False,
                additional_data=review_data.get('additional_data', {})
            )
            reviews_to_save.append(review)
        
        if reviews_to_save:
            await self.bulk_create(session, reviews_to_save)
            await session.commit()
        
        return len(reviews_to_save)

    async def bulk_create_from_jsonl(self, session: AsyncSession, reviews_data: List[Dict]) -> int:
        """Массовое создание записей из JSONL данных"""
        reviews_to_save = []
        
        for review_data in reviews_data:
            review = ReviewsForModel(
                bank_name=review_data.get('bank_name', ''),
                bank_slug=review_data.get('bank_slug', ''),
                product_name=review_data.get('product_name', 'general'),
                review_theme=review_data.get('review_theme', ''),
                rating=review_data.get('rating', ''),
                verification_status=review_data.get('verification_status', ''),
                review_text=review_data.get('review_text', ''),
                review_date=review_data.get('review_date', ''),
                review_timestamp=review_data.get('review_timestamp'),
                source_url=review_data.get('source_url', ''),
                parsed_at=review_data.get('parsed_at'),
                processed=review_data.get('processed', False),
                additional_data=review_data.get('additional_data', {})
            )
            reviews_to_save.append(review)
        
        if reviews_to_save:
            session.add_all(reviews_to_save)
            await session.flush()
        
        return len(reviews_to_save)

class AuditLogRepository:
    async def save(self, session: AsyncSession, user_id: Optional[int], action: str) -> AuditLog:
        log = AuditLog(user_id=user_id, action=action)
        session.add(log)
        await session.flush()
        await session.commit()
        await session.refresh(log)
        return log

    async def get_all_by_user(self, session: AsyncSession, user_id: int, page: int = 0, size: int = 100) -> List[AuditLog]:
        statement = select(AuditLog).where(AuditLog.user_id == user_id).order_by(AuditLog.timestamp.desc()).offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()