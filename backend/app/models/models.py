from sqlalchemy import (
    Column, Integer, String, ForeignKey, Date, Float, Boolean, JSON, Text,
    CheckConstraint, Enum, TIMESTAMP, Index, DateTime
)
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from enum import Enum
from datetime import date, datetime
from app.core.db_manager import Base
from app.models.user_models import UserRole

# Enums
class ProductType(str, Enum):
    CATEGORY = "category"
    SUBCATEGORY = "subcategory"
    SUBTYPE = "subtype"
    PRODUCT = "product"

class ClientType(str, Enum):
    INDIVIDUAL = "individual"
    BUSINESS = "business"
    BOTH = "both"

class Sentiment(str, Enum):
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"

class NotificationType(str, Enum):
    REVIEW_SPIKE = "review_spike"  # Увеличение отзывов > threshold%
    SENTIMENT_DECLINE = "sentiment_decline"  # Падение позитивной тональности > threshold%
    RATING_DROP = "rating_drop"  # Падение среднего рейтинга > threshold
    NEGATIVE_SPIKE = "negative_spike"  # Увеличение негативных отзывов > threshold%
    CLUSTER_ALERT = "cluster_alert"  # Изменение в конкретном кластере (нужен доп. cluster_id)
    # Add other types as needed


# Новая промежуточная таблица
class ReviewProduct(Base):
    __tablename__ = "review_products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    review_id: Mapped[int] = mapped_column(ForeignKey("reviews.id", ondelete="CASCADE"), nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)

    __table_args__ = (
        Index("idx_review_products_review_id", "review_id"),
        Index("idx_review_products_product_id", "product_id"),
    )

# Product Model
class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"))
    level: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    type: Mapped[ProductType] = mapped_column(String(20), nullable=False)
    client_type: Mapped[ClientType] = mapped_column(String(20), default=ClientType.BOTH, nullable=False)
    attributes: Mapped[Optional[dict]] = mapped_column(JSON)
    description: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("type IN ('category', 'subcategory', 'subtype', 'product')"),
        CheckConstraint("client_type IN ('individual', 'business', 'both')"),
        Index("idx_products_parent_id", "parent_id"),
        Index("idx_products_name", "name"),
        Index("idx_products_client_type", "client_type"),
    )

    # Relationships
    reviews = relationship("Review", secondary="review_products", back_populates="products")
    parent = relationship("Product", remote_side=[id], back_populates="children")
    children = relationship("Product", back_populates="parent")

# Review Model
class Review(Base):
    __tablename__ = "reviews"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    rating: Mapped[Optional[int]] = mapped_column(Integer)
    sentiment: Mapped[Optional[Sentiment]] = mapped_column(String(20))
    sentiment_score: Mapped[Optional[float]] = mapped_column(Float)
    source: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.current_timestamp())

    products = relationship("Product", secondary="review_products", back_populates="reviews")
    clusters = relationship("ReviewCluster", back_populates="review")  # Без изменений
    __table_args__ = (
        CheckConstraint("rating BETWEEN 1 AND 5"),
        CheckConstraint("sentiment IN ('positive', 'neutral', 'negative')"),
        CheckConstraint("sentiment_score BETWEEN -1 AND 1"),
        Index("idx_reviews_date", "date"),
        Index("idx_reviews_sentiment", "sentiment"),
        # Note: GIN index for text requires PostgreSQL-specific configuration, handled in DB creation
    )


# Cluster Model
class Cluster(Base):
    __tablename__ = "clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    keywords: Mapped[Optional[dict]] = mapped_column(JSON)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Relationships
    review_clusters = relationship("ReviewCluster", back_populates="cluster")

# ReviewCluster Model
class ReviewCluster(Base):
    __tablename__ = "review_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    review_id: Mapped[int] = mapped_column(ForeignKey("reviews.id", ondelete="CASCADE"), nullable=False)
    cluster_id: Mapped[int] = mapped_column(ForeignKey("clusters.id", ondelete="CASCADE"), nullable=False)
    topic_weight: Mapped[float] = mapped_column(Float, default=1.0)
    sentiment_contribution: Mapped[Optional[Sentiment]] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.current_timestamp())

    __table_args__ = (
        CheckConstraint("topic_weight BETWEEN 0 AND 1"),
        CheckConstraint("sentiment_contribution IN ('positive', 'neutral', 'negative')"),
        Index("idx_review_clusters_review_id", "review_id"),
        Index("idx_review_clusters_cluster_id", "cluster_id"),
    )

    # Relationships
    review = relationship("Review", back_populates="clusters")
    cluster = relationship("Cluster", back_populates="review_clusters")

# MonthlyStats Model
class MonthlyStats(Base):
    __tablename__ = "monthly_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    month: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    review_count: Mapped[int] = mapped_column(Integer, default=0)
    count_change_percent: Mapped[Optional[float]] = mapped_column(Float)
    avg_rating: Mapped[Optional[float]] = mapped_column(Float)
    positive_count: Mapped[int] = mapped_column(Integer, default=0)
    neutral_count: Mapped[int] = mapped_column(Integer, default=0)
    negative_count: Mapped[int] = mapped_column(Integer, default=0)
    sentiment_trend: Mapped[Optional[float]] = mapped_column(Float)

    __table_args__ = (
        Index("idx_monthly_stats_product_id", "product_id"),
        Index("idx_monthly_stats_month", "month"),
    )

    # Relationships
    product = relationship("Product")

# ClusterStats Model
class ClusterStats(Base):
    __tablename__ = "cluster_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[int] = mapped_column(ForeignKey("clusters.id", ondelete="CASCADE"), nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    month: Mapped[Optional[datetime.date]] = mapped_column(Date)
    weighted_review_count: Mapped[float] = mapped_column(Float, default=0.0)
    positive_percent: Mapped[Optional[float]] = mapped_column(Float)
    neutral_percent: Mapped[Optional[float]] = mapped_column(Float)
    negative_percent: Mapped[Optional[float]] = mapped_column(Float)
    avg_rating: Mapped[Optional[float]] = mapped_column(Float)

    __table_args__ = (
        Index("idx_cluster_stats_cluster_id", "cluster_id"),
        Index("idx_cluster_stats_product_id", "product_id"),
    )

    # Relationships
    cluster = relationship("Cluster")
    product = relationship("Product")

# Notification Model
class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[NotificationType] = mapped_column(String(50), nullable=False)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.current_timestamp())

    __table_args__ = (
        Index("idx_notifications_user_id", "user_id"),
        Index("idx_notifications_type", "type"),
    )

    # Relationships
    user = relationship("User")

class NotificationConfig(Base):
    __tablename__ = "notification_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    notification_type: Mapped[NotificationType] = mapped_column(String(50), nullable=False)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)  # e.g., 20.0 for 20% change
    period: Mapped[str] = mapped_column(String(20), default="monthly")  # monthly/weekly/daily
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.current_timestamp())

    __table_args__ = (
        Index("idx_notification_configs_user_id", "user_id"),
        Index("idx_notification_configs_product_id", "product_id"),
        CheckConstraint("period IN ('daily', 'weekly', 'monthly')"),
    )

    # Relationships
    user = relationship("User")
    product = relationship("Product")

# ReviewsForModel Model - для хранения сырых отзывов с парсера
# ReviewsForModel Model - для хранения сырых отзывов с парсера
class ReviewsForModel(Base):
    __tablename__ = "reviews_for_model"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bank_name: Mapped[str] = mapped_column(String(100), nullable=False)
    bank_slug: Mapped[str] = mapped_column(String(100), nullable=True)
    product_name: Mapped[str] = mapped_column(String(100), nullable=False)
    review_theme: Mapped[str] = mapped_column(String(200), nullable=True)
    rating: Mapped[str] = mapped_column(String(20), nullable=True)
    verification_status: Mapped[str] = mapped_column(String(100), nullable=True)
    review_text: Mapped[str] = mapped_column(Text, nullable=False)
    review_date: Mapped[str] = mapped_column(String(50), nullable=True)
    review_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    source_url: Mapped[str] = mapped_column(String(500), nullable=True)
    parsed_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.current_timestamp())
    processed: Mapped[bool] = mapped_column(Boolean, default=False)
    additional_data: Mapped[Optional[dict]] = mapped_column(JSON)  # Для дополнительных данных

    __table_args__ = (
        Index("idx_reviews_for_model_parsed_at", "parsed_at"),
        Index("idx_reviews_for_model_processed", "processed"),
        Index("idx_reviews_for_model_bank_slug", "bank_slug"),
        Index("idx_reviews_for_model_product_name", "product_name"),
        Index("idx_reviews_for_model_review_timestamp", "review_timestamp"),
        Index("idx_reviews_for_model_bank_product", "bank_slug", "product_name"),
    )

# AuditLog Model
class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.current_timestamp())

    # Relationships
    user = relationship("User")