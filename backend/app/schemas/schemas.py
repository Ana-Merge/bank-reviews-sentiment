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
            raise ValueError("Название продукта слишком длинное")
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
    product_ids: List[int]
    rating: Optional[int] = None
    sentiment: Optional[Sentiment] = None
    sentiment_score: Optional[float] = None
    source: Optional[str] = None

    @field_validator("product_ids")
    @classmethod
    def validate_product_ids(cls, v):
        if not v:
            raise ValueError("Хотя бы 1 продукт должен быть")
        return v

    @field_validator("rating")
    @classmethod
    def validate_rating(cls, v):
        if v is not None and not (1 <= v <= 5):
            raise ValueError("Рейтинг должен быть между 1 и 5")
        return v

    @field_validator("sentiment_score")
    @classmethod
    def validate_sentiment_score(cls, v):
        if v is not None and not (-1 <= v <= 1):
            raise ValueError("Sentiment score должен быть между -1 и 1")
        return v

class ReviewCreate(ReviewBase):
    pass

class ReviewResponse(ReviewBase):
    id: int
    created_at: datetime
    product_ids: List[int]

    class Config:
        from_attributes = True

class ReviewsResponse(BaseModel):
    total: int
    reviews: List[ReviewResponse]
    
    class Config:
        from_attributes = True

class ClusterBase(BaseModel):
    name: NonEmptyStr

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        if len(v) > 100:
            raise ValueError("Название кластера слишком длинное")
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
        if len(v) > 1000:
            raise ValueError("Текст отзыва слишком большой")
        return v
    
class ReviewBulkCreate(BaseModel):
    data: List[ReviewBulkItem]

    @field_validator("data")
    @classmethod
    def validate_data(cls, v):
        if not v:
            raise ValueError("Массив не должен быть пустым")
        if len(v) > 1000:
            raise ValueError("Слишком много отзывов, должно быть меньше 1000")
        ids = [item.id for item in v]
        if len(ids) != len(set(ids)):
            raise ValueError("Дубликат IDs в данных")
        return v

    
class ReviewClusterBase(BaseModel):
    review_id: int
    cluster_id: int
    topic_weight: float

    @field_validator("topic_weight")
    @classmethod
    def validate_topic_weight(cls, v):
        if not 0 <= v <= 1:
            raise ValueError("Веса должен быть между 0 и 1")
        return v

class ReviewClusterCreate(ReviewClusterBase):
    pass

class ReviewClusterResponse(ReviewClusterBase):
    id: int

    class Config:
        from_attributes = True

class MonthlyStatsBase(BaseModel):
    product_id: int
    month: date
    review_count: int
    avg_rating: Optional[float] = None

    @field_validator("avg_rating")
    @classmethod
    def validate_avg_rating(cls, v):
        if v is not None and not (0 <= v <= 5):
            raise ValueError("Средний рейтинг должен быть между 0 и 5")
        return v

class MonthlyStatsCreate(MonthlyStatsBase):
    pass

class MonthlyStatsResponse(MonthlyStatsBase):
    id: int

    class Config:
        from_attributes = True

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
            raise ValueError("Процент должен быть между 0 и 100")
        return v

    @field_validator("avg_rating")
    @classmethod
    def validate_avg_rating(cls, v):
        if v is not None and not (0 <= v <= 5):
            raise ValueError("Средний рейтинг должен быть 0 и 5")
        return v

class ClusterStatsCreate(ClusterStatsBase):
    pass

class NotificationBase(BaseModel):
    user_id: int
    message: NonEmptyStr
    type: NotificationType
    is_read: bool = False

    @field_validator("message")
    @classmethod
    def validate_message(cls, v):
        if len(v) > 255:
            raise ValueError("Текст уведомления слишком длинный")
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
    threshold: float
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

class TonalityStackedBarsResponse(BaseModel):
    period1: List[Dict[str, Any]]
    period2: List[Dict[str, Any]]
    changes: List[Dict[str, Any]]