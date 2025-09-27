from typing import Annotated, List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import date
from pydantic import BaseModel
from app.models.user_models import User
from app.schemas.schemas import ProductTreeNode, ReviewResponse, ReviewBulkCreate, ChangeChartResponse
from app.repositories.repositories import ProductRepository, ClusterRepository
from app.models.models import Product
from app.models.user_models import UserRole
from app.core.dependencies import get_current_user, DbSession, StatsServiceDep, get_db
from app.services.stats_service import StatsService
from app.schemas.schemas import ProductStatsResponse, MonthlyPieChartResponse, SmallBarChartsResponse, ClusterResponse, TonalityStackedBarsResponse

dashboards_router = APIRouter(prefix="/api/v1/dashboards", tags=["dashboards"])

@dashboards_router.get("/product-stats", response_model=List[ProductStatsResponse])
async def get_product_stats(
    db: DbSession,
    stats_service: StatsServiceDep,
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    start_date2: str = Query(..., description="Second period start date in YYYY-MM-DD format"),
    end_date2: str = Query(..., description="Second period end date in YYYY-MM-DD format"),
    product_id: Optional[int] = Query(None, description="Product ID for filtering"),
    source: Optional[str] = Query(None, description="Filter reviews by source (e.g., 'Banki.ru', 'App Store', 'Google Play')")
):
    """
    Получение статистики по продуктам: количество отзывов, средний рейтинг, распределение по тональности.
    Фильтры: даты, конкретный продукт.

    **Что передавать**:
    - **Параметры запроса**:
      - `start_date`: Начальная дата периода (обязательно, формат YYYY-MM-DD, например, 2025-01-01).
      - `end_date`: Конечная дата периода (обязательно, формат YYYY-MM-DD, например, 2025-06-30).
      - `start_date2`: Начальная дата второго периода (опционально, формат YYYY-MM-DD, например, 2025-07-01).
      - `end_date2`: Конечная дата второго периода (опционально, формат YYYY-MM-DD, например, 2025-12-31).
      - `product_id`: ID продукта для фильтрации (опционально, например, 3 для карты "Мир"; если не указан, агрегируется по всем продуктам).
    - **Тело запроса**: Не требуется (GET-запрос).

    **Что получите в ответе**:
    - **Код 200 OK**: Список статистики по продуктам (массив объектов `ProductStatsResponse`).
      - **Формат JSON**:
        ```json
        [
          {
            "product_name": "карта \"Мир\"",
            "change_percent": 0.0,
            "change_color": "green",
            "count": 10,
            "tonality": {"negative": 1, "neutral": 1, "positive": 8},
            "avg_rating": 4.5
          }
        ]
        ```
    """
    try:
        data = await stats_service.get_product_stats(db, start_date, end_date, start_date2, end_date2, source=source)
        if product_id:
            result = await db.execute(select(Product).where(Product.id == product_id))
            product = result.scalar_one_or_none()
            if not product:
                return []
            data = [stat for stat in data if stat["product_name"] == product.name]
        return [ProductStatsResponse(**stat) for stat in data]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching product stats: {str(e)}")


@dashboards_router.get("/monthly-review-count", response_model=Dict[str, List[Dict[str, Any]]])
async def get_monthly_review_count(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    start_date2: str = Query(..., description="Second period start date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    end_date2: str = Query(..., description="Second period end date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    aggregation_type: str = Query(..., description="Aggregation type: 'month', 'week', or 'day'"),
    source: Optional[str] = Query(None, description="Filter reviews by source (e.g., 'Banki.ru', 'App Store', 'Google Play')")
):
    try:
        data = await stats_service.get_monthly_review_count(
            db, product_id, start_date, end_date, start_date2, end_date2, aggregation_type, source=source)
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching monthly review count: {str(e)}")
    

@dashboards_router.get("/bar_chart_changes", response_model=Dict[str, List[Dict[str, Any]]])
async def get_bar_chart_changes(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    start_date2: str = Query(..., description="Second period start date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    end_date2: str = Query(..., description="Second period end date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    aggregation_type: str = Query(..., description="Aggregation type: 'month', 'week', or 'day'"),
    source: Optional[str] = Query(None, description="Filter reviews by source (e.g., 'Banki.ru', 'App Store', 'Google Play')"),
):
    try:
        data = await stats_service.get_bar_chart_changes(
            db, product_id, start_date, end_date, start_date2, end_date2, aggregation_type, source=source
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching monthly review count: {str(e)}")
    
@dashboards_router.get("/monthly-pie-chart", response_model=MonthlyPieChartResponse)
async def get_monthly_pie_chart(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(..., description="Product ID for filtering"),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD or YYYY-MM format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD or YYYY-MM format"),
    start_date2: str = Query(..., description="Second period start date in YYYY-MM-DD or YYYY-MM format"),
    end_date2: str = Query(..., description="Second period end date in YYYY-MM-DD or YYYY-MM format"),
    source: Optional[str] = Query(None, description="Filter reviews by source (e.g., 'Banki.ru', 'App Store', 'Google Play')")
):
    """
    Retrieve pie chart data with percentage distribution of reviews by cluster for two periods and percentage point changes.

    Parameters:
    - product_id: ID of the product to filter reviews (required).
    - start_date: Start date for the first period (required, YYYY-MM-DD or YYYY-MM).
    - end_date: End date for the first period (required, YYYY-MM-DD or YYYY-MM).
    - start_date2: Start date for the second period (required, YYYY-MM-DD or YYYY-MM).
    - end_date2: End date for the second period (required, YYYY-MM-DD or YYYY-MM).
    - source: Filter reviews by source (optional, e.g., 'Banki.ru', 'App Store', 'Google Play').

    Returns:
    - JSON object with 'period1', 'period2', and 'changes' containing labels, percentage data, colors, and total review counts or percentage point changes.
    """
    try:
        data = await stats_service.get_monthly_pie_chart(
            db, product_id, start_date, end_date, start_date2, end_date2, source
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching monthly pie chart: {str(e)}")

@dashboards_router.get("/small-bar-charts", response_model=List[SmallBarChartsResponse])
async def get_small_bar_charts(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
    cluster_id: Optional[int] = Query(None),
):
    try:
        data = await stats_service.get_small_bar_charts(db, product_id, start_date, end_date, None, cluster_id)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching small bar charts: {str(e)}")

@dashboards_router.get("/monthly-stacked-bars", response_model=Dict[str, List[Dict[str, Any]]])
async def get_monthly_stacked_bars(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(..., description="Product ID for filtering"),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    start_date2: str = Query(..., description="Second period start date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    end_date2: str = Query(..., description="Second period end date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    aggregation_type: str = Query(..., description="Aggregation type: 'month', 'week', or 'day'"),
    source: Optional[str] = Query(None, description="Filter reviews by source (e.g., 'Banki.ru', 'App Store', 'Google Play')"),
    cluster_id: Optional[int] = Query(None, description="Filter by specific cluster ID (optional)")
):
    """
    Retrieve stacked bar chart data with review counts per cluster for two periods and percentage changes.

    Parameters:
    - product_id: ID of the product to filter reviews (required).
    - start_date: Start date for the first period (required, YYYY-MM-DD or YYYY-MM for monthly).
    - end_date: End date for the first period (required, YYYY-MM-DD or YYYY-MM for monthly).
    - start_date2: Start date for the second period (required, YYYY-MM-DD or YYYY-MM for monthly).
    - end_date2: End date for the second period (required, YYYY-MM-DD or YYYY-MM for monthly).
    - aggregation_type: Type of aggregation ('month', 'week', or 'day').
    - source: Filter reviews by source (optional, e.g., 'Banki.ru', 'App Store', 'Google Play').
    - cluster_id: Filter by a specific cluster ID (optional; if not provided, includes all clusters).

    Returns:
    - JSON object with 'period1', 'period2', and 'changes' lists containing aggregation dates, review counts per cluster, and percentage changes.
    """
    try:
        data = await stats_service.get_monthly_stacked_bars(
            db, product_id, start_date, end_date, start_date2, end_date2, aggregation_type, source, cluster_id
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching monthly stacked bars: {str(e)}")

@dashboards_router.get("/tonality-stacked-bars", response_model=TonalityStackedBarsResponse)
async def get_tonality_stacked_bars(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    start_date2: str = Query(..., description="Second period start date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    end_date2: str = Query(..., description="Second period end date in YYYY-MM-DD or YYYY-MM format for monthly aggregation"),
    aggregation_type: str = Query(..., description="Aggregation type: 'month', 'week', or 'day'"),
    source: Optional[str] = Query(None, description="Filter reviews by source (e.g., 'Banki.ru', 'App Store', 'Google Play')")
):
    """
    Получение stacked bars по тональности (positive, neutral, negative) с агрегацией по периоду и изменениями со вторым периодом.

    **Что передавать**:
    - **Параметры запроса**:
      - `product_id`: ID продукта (обязательно).
      - `start_date`, `end_date`: Первый период.
      - `start_date2`, `end_date2`: Второй период.
      - `aggregation_type`: 'month', 'week', или 'day'.
      - `source`: Опционально.
    - **Тело запроса**: Не требуется.

    **Что получите в ответе**:
    - **Код 200 OK**: Данные для stacked bars по тональности.
      - **Формат JSON**:
        ```json
        {
          "period1": [
            {
              "date": "2025-01",
              "tonalities": [
                {"sentiment": "positive", "count": 50, "color": "green"},
                {"sentiment": "neutral", "count": 30, "color": "yellow"},
                {"sentiment": "negative", "count": 20, "color": "red"}
              ]
            }
          ],
          "period2": [...],
          "changes": [
            {
              "date": "2025-01",
              "tonalities": [
                {"sentiment": "positive", "change": 10, "color": "green"},
                ...
              ]
            }
          ]
        }
        ```
    """
    try:
        data = await stats_service.get_tonality_stacked_bars(
            db, product_id, start_date, end_date, start_date2, end_date2, aggregation_type, source=source
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching tonality stacked bars: {str(e)}")

@dashboards_router.get("/line-and-bar-pie-chart", response_model=MonthlyPieChartResponse)
async def get_line_and_bar_pie_chart(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(..., description="Product ID for filtering"),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    start_date2: str = Query(..., description="Second period start date in YYYY-MM-DD format"),
    end_date2: str = Query(..., description="Second period end date in YYYY-MM-DD format"),
    source: Optional[str] = Query(None, description="Filter reviews by source (e.g., 'Banki.ru', 'App Store', 'Google Play')"),
):
    """
    Получение распределения по тональности (pie chart) для двух периодов и изменений.

    **Что передавать**:
    - **Параметры запроса**:
      - `product_id`: ID продукта (обязательно).
      - `start_date`: Начальная дата первого периода (обязательно, формат YYYY-MM-DD).
      - `end_date`: Конечная дата первого периода (обязательно, формат YYYY-MM-DD).
      - `start_date2`: Начальная дата второго периода (обязательно, формат YYYY-MM-DD).
      - `end_date2`: Конечная дата второго периода (обязательно, формат YYYY-MM-DD).
      - `source`: Источник отзывов (опционально).
    - **Тело запроса**: Не требуется (GET-запрос).

    **Что получите в ответе**:
    - **Код 200 OK**: Распределение по тональности для двух периодов и изменений (объект `MonthlyPieChartResponse`).
      - **Формат JSON**:
        ```json
        {
          "period1": {
            "labels": ["negative", "neutral", "positive"],
            "data": [10.0, 20.0, 70.0],
            "colors": ["red", "yellow", "green"],
            "total": 100
          },
          "period2": {
            "labels": ["negative", "neutral", "positive"],
            "data": [15.0, 25.0, 60.0],
            "colors": ["red", "yellow", "green"],
            "total": 80
          },
          "changes": {
            "labels": ["negative", "neutral", "positive"],
            "percentage_point_changes": [-5.0, -5.0, 10.0]
          }
        }
        ```
    """
    try:
        data = await stats_service.get_tonality_pie_chart(db, product_id, start_date, end_date, start_date2, end_date2, source)
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching tonality pie chart: {str(e)}")


@dashboards_router.get(
    "/public-product-tree",
    response_model=List[ProductTreeNode],
    summary="Get product hierarchy tree (public)",
    description="Retrieve all products in a hierarchical tree structure (categories → subcategories → products) without authentication or parameters.",
    response_description="List of root nodes representing the product hierarchy."
)
async def get_public_product_tree(
    db: AsyncSession = Depends(get_db),
    product_repo: ProductRepository = Depends(lambda: ProductRepository())
):
    """
    Получить все продукты в виде дерева иерархии (категории → подкатегории → продукты) без авторизации.
    """
    try:
        tree = await product_repo.get_product_tree(db)
        return tree
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve product tree")

@dashboards_router.get("/change-chart", response_model=ChangeChartResponse)
async def get_change_chart(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(..., description="Product ID for filtering"),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    start_date2: str = Query(..., description="Second period start date in YYYY-MM-DD format"),
    end_date2: str = Query(..., description="Second period end date in YYYY-MM-DD format"),
    source: Optional[str] = Query(None, description="Filter reviews by source (e.g., 'Banki.ru', 'App Store', 'Google Play')"),
):
    """
    Получение общего количества отзывов по продукту за первый период и процент изменения относительно второго периода.

    **Что передавать**:
    - **Параметры запроса**:
      - `product_id`: ID продукта (обязательно, например, 3 для карты "Мир").
      - `start_date`: Начальная дата первого периода (обязательно, формат YYYY-MM-DD, например, 2025-01-01).
      - `end_date`: Конечная дата первого периода (обязательно, формат YYYY-MM-DD, например, 2025-06-30).
      - `start_date2`: Начальная дата второго периода (обязательно, формат YYYY-MM-DD, например, 2024-07-01).
      - `end_date2`: Конечная дата второго периода (обязательно, формат YYYY-MM-DD, например, 2024-12-31).
      - `source`: Источник отзывов (опционально, например, 'Banki.ru', 'App Store', 'Google Play').
    - **Тело запроса**: Не требуется (GET-запрос).

    **Что получите в ответе**:
    - **Код 200 OK**: Объект с общим количеством отзывов за первый период и процентом изменения.
      - **Формат JSON**:
        ```json
        {
          "total": 100,
          "change_percent": 25.0
        }
        ```
    """
    try:
        data = await stats_service.get_change_chart(db, product_id, start_date, end_date, start_date2, end_date2, source)
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching change chart: {str(e)}")
    
@dashboards_router.get("/reviews", response_model=List[ReviewResponse])
async def get_reviews(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    cluster_id: Optional[int] = Query(None),
    page: int = Query(0, ge=0),
    size: int = Query(30, ge=1, le=100),
):
    """
    Получение списка отзывов по продукту с опциональной фильтрацией по кластеру и дате, с пагинацией.

    **Что передавать**:
    - **Параметры запроса**:
      - `product_id`: ID продукта (обязательно, например, 3 для карты "Мир").
      - `start_date`: Начальная дата периода (опционально, формат YYYY-MM-DD, например, 2025-01-01).
      - `end_date`: Конечная дата периода (опционально, формат YYYY-MM-DD, например, 2025-06-30).
      - `cluster_id`: ID кластера для фильтрации (опционально, например, 4 для конкретного кластера; если не указан, возвращаются все отзывы).
      - `page`: Номер страницы (опционально, по умолчанию 0).
      - `size`: Количество отзывов на странице (опционально, по умолчанию 30, максимум 100).
    - **Тело запроса**: Не требуется (GET-запрос).

    **Что получите в ответе**:
    - **Код 200 OK**: Список объектов отзывов (массив объектов `ReviewResponse`).
      - **Формат JSON**:
        ```json
        [
          {
            "id": 1,
            "text": "Отличный продукт!",
            "date": "2025-01-15",
            "product_id": 3,
            "rating": 5,
            "sentiment": "positive",
            "sentiment_score": 0.9,
            "source": "Banki.ru"
          }
        ]
        ```
    """
    try:
        data = await stats_service.get_reviews(db, product_id, start_date, end_date, cluster_id, page, size)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching reviews: {str(e)}")
    
@dashboards_router.get(
    "/clusters",
    response_model=List[ClusterResponse],
    summary="Get list of clusters (public)",
    description="Retrieve all clusters without authentication.",
    response_description="List of clusters with their IDs, names, and descriptions."
)
async def get_clusters(
    db: AsyncSession = Depends(get_db),
    cluster_repo: ClusterRepository = Depends(lambda: ClusterRepository())
):
    """
    Получить список всех кластеров без авторизации.
    """
    try:
        clusters = await cluster_repo.get_all(db)
        return [ClusterResponse.from_orm(cluster) for cluster in clusters]
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve clusters")
    

@dashboards_router.post(
    "/reviews",
    response_model=Dict[str, Any],
    summary="Create multiple reviews (public)",
    description="Create multiple reviews in bulk without authentication. Each review contains a text and is assigned a creation date.",
    response_description="Status and count of created reviews."
)
async def create_reviews(
    db: DbSession,
    stats_service: StatsServiceDep,
    reviews_data: ReviewBulkCreate
):
    """
    Массовое создание отзывов через JSON.

    **Что передавать**:
    - **Тело запроса** (JSON):
      ```json
      {
        "data": [
          {"id": 1, "text": "текст отзыва"},
          {"id": 2, "text": "текст другого отзыва"}
        ]
      }
      ```
      - `id`: Игнорируется (ID генерируется базой данных).
      - `text`: Текст отзыва (обязательно, не пустой, максимум 1000 символов).
    - **Ограничения**:
      - Минимум 1 отзыв, максимум 1000 отзывов за раз.
      - Дубликаты `id` в JSON недопустимы.

    **Что получите в ответе**:
    - **Код 200 OK**: Подтверждение успешного создания.
      - **Формат JSON**:
        ```json
        {
          "status": "success",
          "created_count": 2
        }
        ```
    - **Код 400 Bad Request**: Если входные данные некорректны.
    - **Код 500 Internal Server Error**: Если произошла ошибка сервера.
    """
    try:
        result = await stats_service.create_reviews_bulk(db, reviews_data)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating reviews: {str(e)}")