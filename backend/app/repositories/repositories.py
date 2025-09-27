from sqlalchemy import exists, func, select, and_, case, cast, Float, literal, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func as sql_func
from typing import List, Optional, Dict
from datetime import date
from app.schemas.schemas import ProductTreeNode
from app.models.models import NotificationConfig

from app.core.exceptions import (
    EntityAlreadyExistsException,
    EntityNotFoundException,
)
from app.models.models import (
    Product, Review, Cluster, ReviewCluster, MonthlyStats, ClusterStats, Notification, AuditLog,
    Sentiment, ProductType, ClientType
)
from app.schemas.schemas import (
    ProductCreate, ReviewCreate, ClusterCreate, ReviewClusterCreate,
    MonthlyStatsCreate, ClusterStatsCreate
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
        # Базовый запрос для CTE: корневые узлы (parent_id IS NULL)
        base_query = select(
            Product.id.label('id'),
            Product.name.label('name'),
            Product.type.label('type'),
            Product.client_type.label('client_type'),
            Product.parent_id.label('parent_id'),
            Product.level.label('level')  # Используем Product.level
        ).where(Product.parent_id.is_(None))

        if client_type:
            base_query = base_query.where(Product.client_type == client_type)

        # Рекурсивный CTE
        recursive_cte = base_query.cte("product_tree", recursive=True)

        # Рекурсивная часть
        recursive_part = select(
            Product.id,
            Product.name,
            Product.type,
            Product.client_type,
            Product.parent_id,
            Product.level  # Используем Product.level
        ).join(recursive_cte, Product.parent_id == recursive_cte.c.id)

        # Полный CTE
        full_cte = recursive_cte.union_all(recursive_part)

        # Запрос для получения всех узлов
        statement = select(
            full_cte.c.id,
            full_cte.c.name,
            full_cte.c.type,
            full_cte.c.client_type,
            full_cte.c.parent_id,
            full_cte.c.level
        ).order_by(full_cte.c.level, full_cte.c.id)

        result = await session.execute(statement)
        rows = result.fetchall()

        # Построение дерева
        tree = self._build_tree_from_rows(rows)
        return tree

    def _build_tree_from_rows(self, rows: List) -> List[Dict[str, Any]]:
        """
        Построить дерево из плоского списка узлов.
        """
        nodes = {}
        for row in rows:
            node = {
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "client_type": row[3],
                "level": row[5],  # Убедимся, что level извлекается
                "children": []
            }
            nodes[row[0]] = node

        root_nodes = []
        for row in rows:
            node_id = row[0]
            parent_id = row[4]
            if parent_id is None:
                root_nodes.append(nodes[node_id])
            elif parent_id in nodes:
                nodes[parent_id]["children"].append(nodes[node_id])

        return root_nodes


class ReviewRepository:
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
            statement = statement.where(Review.product_id == product_id)
        if start_date:
            statement = statement.where(Review.date >= start_date)
        if end_date:
            statement = statement.where(Review.date <= end_date)
        statement = statement.offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()

    async def count_by_product_and_period(
        self, session: AsyncSession, product_ids: List[int], start_date: date, end_date: date, source: Optional[str] = None
    ) -> int:
        if not product_ids:
            return 0
        statement = select(func.count()).select_from(Review).where(
            Review.product_id.in_(product_ids),
            Review.date >= start_date,
            Review.date <= end_date
        )
        if source:
            statement = statement.where(Review.source == source)
        result = await session.execute(statement)
        return result.scalar() or 0

    async def count_by_product_and_period_and_sentiment(self, session: AsyncSession, product_ids: List[int], start_date: date, end_date: date, sentiment: str, source: Optional[str] = None) -> int:
        statement = select(func.count(Review.id)).where(
            Review.product_id.in_(product_ids),
            Review.date >= start_date,
            Review.date <= end_date,
            Review.sentiment == sentiment
        )
        if source:
            statement = statement.where(Review.source == source)
        result = await session.execute(statement)
        return result.scalar_one() or 0
    
    async def get_avg_rating_by_products(
        self, session: AsyncSession, product_ids: List[int], source: Optional[str] = None
    ) -> float:
        product_ids = [product_ids] if isinstance(product_ids, int) else product_ids
        if not product_ids:
            return 0.0
        statement = select(func.avg(Review.rating)).where(Review.product_id.in_(product_ids))
        if source:
            statement = statement.where(Review.source == source)
        result = await session.execute(statement)
        avg_rating = result.scalar()
        return float(avg_rating) if avg_rating else 0.0

    async def get_tonality_counts_by_cluster_and_period(
        self, session: AsyncSession, cluster_id: int, product_ids: List[int], start_date: date, end_date: date, source: Optional[str] = None
    ) -> Dict[str, int]:
        product_ids = [product_ids] if isinstance(product_ids, int) else product_ids
        if not product_ids:
            return {'positive': 0, 'neutral': 0, 'negative': 0}
        statement = select(
            Review.sentiment,
            func.count(Review.id)
        ).join(ReviewCluster).where(
            ReviewCluster.cluster_id == cluster_id,
            Review.product_id.in_(product_ids),
            Review.date >= start_date,
            Review.date <= end_date
        )
        if source:
            statement = statement.where(Review.source == source)
        statement = statement.group_by(Review.sentiment)
        result = await session.execute(statement)
        counts = {row[0]: row[1] for row in result.all() if row[0]}
        return {
            'positive': counts.get('positive', 0),
            'neutral': counts.get('neutral', 0),
            'negative': counts.get('negative', 0)
        }

    async def get_tonality_counts_by_product_and_period(
        self, session: AsyncSession, product_ids: List[int], start_date: date, end_date: date, source: Optional[str] = None
    ) -> Dict[str, int]:
        if not product_ids:
            return {'positive': 0, 'neutral': 0, 'negative': 0}
        statement = select(
            Review.sentiment,
            func.count(Review.id)
        ).where(
            Review.product_id.in_(product_ids),
            Review.date >= start_date,
            Review.date <= end_date
        )
        if source:
            statement = statement.where(Review.source == source)
        statement = statement.group_by(Review.sentiment)
        result = await session.execute(statement)
        counts = {row[0]: row[1] for row in result.all() if row[0]}
        return {
            'positive': counts.get('positive', 0),
            'neutral': counts.get('neutral', 0),
            'negative': counts.get('negative', 0)
        }
    
    async def bulk_create(self, session: AsyncSession, reviews: List[Review]) -> None:
        session.add_all(reviews)
    
class ClusterRepository:
    async def get_by_id(self, session: AsyncSession, cluster_id: int) -> Cluster | None:
        statement = select(Cluster).where(Cluster.id == cluster_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def get_all(self, session: AsyncSession, page: int = 0, size: int = 100) -> List[Cluster]:
        statement = select(Cluster).order_by(Cluster.name).offset(page * size).limit(size)
        result = await session.execute(statement)
        return result.scalars().all()

    async def save(self, session: AsyncSession, cluster: Cluster) -> Cluster:
        session.add(cluster)
        await session.flush()
        await session.commit()
        await session.refresh(cluster)
        return cluster

class ReviewClusterRepository:
    async def get_by_review_id(self, session: AsyncSession, review_id: int) -> List[ReviewCluster]:
        statement = select(ReviewCluster).where(ReviewCluster.review_id == review_id)
        result = await session.execute(statement)
        return result.scalars().all()

    async def save(self, session: AsyncSession, review_cluster: ReviewCluster) -> ReviewCluster:
        session.add(review_cluster)
        await session.flush()
        await session.commit()
        await session.refresh(review_cluster)
        return review_cluster

    async def count_by_cluster_and_period(
        self, session: AsyncSession, cluster_id: int, product_ids: List[int], start_date: date, end_date: date
    ) -> float:
        product_ids = [product_ids] if isinstance(product_ids, int) else product_ids  # Фикс для single int
        if not product_ids:
            return 0.0
        statement = select(func.sum(ReviewCluster.topic_weight)).join(Review).where(
            Review.product_id.in_(product_ids),
            Review.date >= start_date,
            Review.date <= end_date,
            ReviewCluster.cluster_id == cluster_id
        )
        result = await session.execute(statement)
        return result.scalar() or 0.0

class MonthlyStatsRepository:
    async def get_by_product_and_month(self, session: AsyncSession, product_id: int, month: date) -> MonthlyStats | None:
        statement = select(MonthlyStats).where(
            and_(MonthlyStats.product_id == product_id, MonthlyStats.month == month)
        )
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    async def save(self, session: AsyncSession, stats: MonthlyStats) -> MonthlyStats:
        session.add(stats)
        await session.flush()
        await session.commit()
        await session.refresh(stats)
        return stats

    async def get_stats_for_product(
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