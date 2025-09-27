from pydantic import BaseModel, field_validator
from typing import Optional, List, Dict, Any, Literal
from enum import StrEnum, Enum
from datetime import date, datetime
from app.utils.utils import NonEmptyStr
from app.models.user_models import UserRole

class ProductType(StrEnum):
    CATEGORY = "category"
    SUBCATEGORY = "subcategory"
    SUBTYPE = "subtype"
    PRODUCT = "product"

class ClientType(StrEnum):
    INDIVIDUAL = "individual"
    BUSINESS = "business"
    BOTH = "both"

class Sentiment(StrEnum):
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"

class NotificationType(str, Enum):
    REVIEW_SPIKE = "review_spike"
    SENTIMENT_DECLINE = "sentiment_decline"
    RATING_DROP = "rating_drop"
    NEGATIVE_SPIKE = "negative_spike" 
    CLUSTER_ALERT = "cluster_alert"

class ProductBase(BaseModel):
    name: NonEmptyStr
    parent_id: Optional[int] = None
    type: ProductType
    client_type: ClientType = ClientType.BOTH
    attributes: Optional[Dict[str, Any]] = None
    description: Optional[str] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        if len(v) > 150:
            raise ValueError("Product name too long")
        return v

class ProductCreate(ProductBase):
    pass

class ProductTreeNode(BaseModel):
    id: int
    name: str
    type: Literal["category", "subcategory", "product"]
    client_type: str
    level: int
    children: List["ProductTreeNode"] = []

    class Config:
        from_attributes = True

class ProductResponse(ProductBase):
    id: int
    level: int

    class Config:
        from_attributes = True

class ReviewBase(BaseModel):
    text: NonEmptyStr
    date: date
    product_id: int
    rating: Optional[int] = None
    sentiment: Optional[Sentiment] = None
    sentiment_score: Optional[float] = None
    source: Optional[str] = None

    @field_validator("rating")
    @classmethod
    def validate_rating(cls, v):
        if v is not None and not (1 <= v <= 5):
            raise ValueError("Rating must be between 1 and 5")
        return v

    @field_validator("sentiment_score")
    @classmethod
    def validate_sentiment_score(cls, v):
        if v is not None and not (-1 <= v <= 1):
            raise ValueError("Sentiment score must be between -1 and 1")
        return v

class ReviewCreate(ReviewBase):
    pass

class ReviewResponse(ReviewBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class ClusterBase(BaseModel):
    name: NonEmptyStr

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        if len(v) > 100:
            raise ValueError("Cluster name too long")
        return v

class ClusterCreate(ClusterBase):
    pass

class ClusterResponse(ClusterBase):
    id: int

    class Config:
        from_attributes = True

class ReviewBulkItem(BaseModel):
    id: int
    text: NonEmptyStr

    @field_validator("text")
    @classmethod
    def validate_text(cls, v):
        if len(v) > 1000:  # Reasonable limit for review text
            raise ValueError("Review text too long")
        return v
    
class ReviewBulkCreate(BaseModel):
    data: List[ReviewBulkItem]

    @field_validator("data")
    @classmethod
    def validate_data(cls, v):
        if not v:
            raise ValueError("Data array must not be empty")
        if len(v) > 1000:  # Reasonable upper limit for bulk upload
            raise ValueError("Too many reviews, maximum is 1000")
        # Check for duplicate IDs
        ids = [item.id for item in v]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate IDs in data")
        return v
    
# ReviewCluster Schema
class ReviewClusterBase(BaseModel):
    review_id: int
    cluster_id: int
    topic_weight: float

    @field_validator("topic_weight")
    @classmethod
    def validate_topic_weight(cls, v):
        if not 0 <= v <= 1:
            raise ValueError("Topic weight must be between 0 and 1")
        return v

class ReviewClusterCreate(ReviewClusterBase):
    pass

class ReviewClusterResponse(ReviewClusterBase):
    id: int

    class Config:
        from_attributes = True

# MonthlyStats Schema
class MonthlyStatsBase(BaseModel):
    product_id: int
    month: date
    review_count: int
    avg_rating: Optional[float] = None

    @field_validator("avg_rating")
    @classmethod
    def validate_avg_rating(cls, v):
        if v is not None and not (0 <= v <= 5):
            raise ValueError("Average rating must be between 0 and 5")
        return v

class MonthlyStatsCreate(MonthlyStatsBase):
    pass

class MonthlyStatsResponse(MonthlyStatsBase):
    id: int

    class Config:
        from_attributes = True

# ClusterStats Schema
class ClusterStatsBase(BaseModel):
    cluster_id: int
    product_id: int
    month: Optional[date] = None
    weighted_review_count: float
    positive_percent: Optional[float] = None
    neutral_percent: Optional[float] = None
    negative_percent: Optional[float] = None
    avg_rating: Optional[float] = None

    @field_validator("positive_percent", "neutral_percent", "negative_percent")
    @classmethod
    def validate_percent(cls, v):
        if v is not None and not (0 <= v <= 100):
            raise ValueError("Percent must be between 0 and 100")
        return v

    @field_validator("avg_rating")
    @classmethod
    def validate_avg_rating(cls, v):
        if v is not None and not (0 <= v <= 5):
            raise ValueError("Average rating must be between 0 and 5")
        return v

class ClusterStatsCreate(ClusterStatsBase):
    pass

# class ClusterStatsResponse(ClusterStatsBase):
#     id: int

#     class Config:
#         from_attributes = True

# Notification Schema
class NotificationBase(BaseModel):
    user_id: int
    message: NonEmptyStr
    type: NotificationType
    is_read: bool = False

    @field_validator("message")
    @classmethod
    def validate_message(cls, v):
        if len(v) > 255:  # Reasonable limit for message
            raise ValueError("Notification message too long")
        return v

class NotificationCreate(NotificationBase):
    pass

class NotificationResponse(NotificationBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class NotificationConfigBase(BaseModel):
    product_id: int
    notification_type: NotificationType
    threshold: float  # e.g., 20.0
    period: Literal["daily", "weekly", "monthly"] = "monthly"
    active: bool = True

    @field_validator("threshold")
    @classmethod
    def validate_threshold(cls, v):
        if v <= 0:
            raise ValueError("Threshold must be positive")
        return v

class NotificationConfigCreate(NotificationConfigBase):
    pass

class NotificationConfigResponse(NotificationConfigBase):
    id: int
    user_id: int
    created_at: datetime

    class Config:
        from_attributes = True

# AuditLog Schema
# class AuditLogBase(BaseModel):
#     user_id: Optional[int] = None
#     action: NonEmptyStr

#     @field_validator("action")
#     @classmethod
#     def validate_action(cls, v):
#         if len(v) > 100:
#             raise ValueError("Action description too long")
#         return v

# class AuditLogCreate(AuditLogBase):
#     pass

# class AuditLogResponse(AuditLogBase):
#     id: int
#     timestamp: datetime

#     class Config:
#         from_attributes = True

class ProductStatsResponse(BaseModel):
    product_name: str
    change_percent: float
    change_color: str
    count: int
    tonality: Dict[str, Any]
    avg_rating: float

class PeriodPieData(BaseModel):
    labels: List[str]
    data: List[float]
    colors: List[str]
    total: int

class ChangesPieData(BaseModel):
    labels: List[str]
    percentage_point_changes: List[float]
class ChangeChartResponse(BaseModel):
    total: int
    change_percent: float

class MonthlyPieChartResponse(BaseModel):
    period1: PeriodPieData
    period2: PeriodPieData
    changes: ChangesPieData

class SmallBarChartsResponse(BaseModel):
    title: str
    reviews_count: int
    change_percent: int
    data: List[Dict[str, Any]]

# class RatingTrendResponse(BaseModel):
#     month: str
#     avg_rating: float
#     review_count: int

# class ClusterAnalysisResponse(BaseModel):
#     cluster_id: int
#     cluster_name: str
#     review_count: int
#     positive_percent: float
#     neutral_percent: float
#     negative_percent: float
#     avg_rating: float

# class MonthlyReviewCountResponse(BaseModel):
#     period1: List[Dict[str, Any]]
#     period2: List[Dict[str, Any]]
#     changes: List[Dict[str, Any]]

class TonalityStackedBarsResponse(BaseModel):
    period1: List[Dict[str, Any]]
    period2: List[Dict[str, Any]]
    changes: List[Dict[str, Any]]