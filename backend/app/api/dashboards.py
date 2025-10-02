from typing import Annotated, List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import date
import os
import aiohttp
import asyncio
import sys
from pydantic import BaseModel
from app.models.user_models import User
from app.schemas.schemas import ProductTreeNode, ReviewResponse, ReviewBulkCreate, ChangeChartResponse, ReviewsResponse, ReviewAnalysisResponse
from app.repositories.repositories import ProductRepository, ClusterRepository, ReviewsForModelRepository
from app.models.models import Product
from app.services.parser_service import ParserService
from app.models.user_models import UserRole
from app.core.dependencies import get_current_user, DbSession, StatsServiceDep, get_db
from app.services.stats_service import StatsService
from app.schemas.schemas import ProductStatsResponse, MonthlyPieChartResponse, SmallBarChartsResponse, ClusterResponse, TonalityStackedBarsResponse
ML_SERVICE_URL = os.getenv("ML_SERVICE_URL", "http://158.160.25.202:8002")
ML_PREDICT_ENDPOINT = f"{ML_SERVICE_URL}/predict"
ML_TIMEOUT = int(os.getenv("ML_TIMEOUT", 5))
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)

dashboards_router = APIRouter(prefix="/api/v1/dashboards", tags=["dashboards"])

@dashboards_router.get("/product-stats", response_model=List[ProductStatsResponse])
async def get_product_stats(
    db: DbSession,
    stats_service: StatsServiceDep,
    start_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD"),
    end_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD"),
    start_date2: str = Query(..., description="Начальная дата второго периода в формате YYYY-MM-DD"),
    end_date2: str = Query(..., description="Конечная дата второго периода в формате YYYY-MM-DD"),
    product_id: Optional[int] = Query(None, description="ID продукта для фильтрации"),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов (например, 'Banki.ru', 'App Store', 'Google Play')")
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
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статистики продуктов: {str(e)}")


@dashboards_router.get("/monthly-review-count", response_model=Dict[str, List[Dict[str, Any]]])
async def get_monthly_review_count(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    end_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    start_date2: str = Query(..., description="Начальная дата второго периода в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    end_date2: str = Query(..., description="Конечная дата второго периода в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    aggregation_type: str = Query(..., description="Тип агрегации: 'month', 'week', или 'day'"),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов (например, 'Banki.ru', 'App Store', 'Google Play')")
):
    try:
        data = await stats_service.get_monthly_review_count(
            db, product_id, start_date, end_date, start_date2, end_date2, aggregation_type, source=source)
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении количества месячных отзывов: {str(e)}")
    

@dashboards_router.get("/bar_chart_changes", response_model=Dict[str, List[Dict[str, Any]]])
async def get_bar_chart_changes(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    end_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    start_date2: str = Query(..., description="Начальная дата второго периода в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    end_date2: str = Query(..., description="Конечная дата второго периода в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    aggregation_type: str = Query(..., description="Тип агрегации: 'month', 'week', или 'day'"),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов (например, 'Banki.ru', 'App Store', 'Google Play')"),
):
    try:
        data = await stats_service.get_bar_chart_changes(
            db, product_id, start_date, end_date, start_date2, end_date2, aggregation_type, source=source
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении количества месячных отзывов: {str(e)}")
    
@dashboards_router.get("/monthly-pie-chart", response_model=MonthlyPieChartResponse)
async def get_monthly_pie_chart(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(..., description="ID продукта для фильтрации"),
    start_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD или YYYY-MM"),
    end_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD или YYYY-MM"),
    start_date2: str = Query(..., description="Начальная дата второго периода в формате YYYY-MM-DD или YYYY-MM"),
    end_date2: str = Query(..., description="Конечная дата второго периода в формате YYYY-MM-DD или YYYY-MM"),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов (например, 'Banki.ru', 'App Store', 'Google Play')")
):
    """
    Получение данных для круговой диаграммы с процентным распределением отзывов по кластерам для двух периодов и изменениями в процентных пунктах.

    Параметры:
    - product_id: ID продукта для фильтрации отзывов (обязательно).
    - start_date: Начальная дата первого периода (обязательно, YYYY-MM-DD или YYYY-MM).
    - end_date: Конечная дата первого периода (обязательно, YYYY-MM-DD или YYYY-MM).
    - start_date2: Начальная дата второго периода (обязательно, YYYY-MM-DD или YYYY-MM).
    - end_date2: Конечная дата второго периода (обязательно, YYYY-MM-DD или YYYY-MM).
    - source: Фильтр по источнику отзывов (опционально, например, 'Banki.ru', 'App Store', 'Google Play').

    Возвращает:
    - JSON объект с 'period1', 'period2', и 'changes', содержащий метки, процентные данные, цвета и общее количество отзывов или изменения в процентных пунктах.
    """
    try:
        data = await stats_service.get_monthly_pie_chart(
            db, product_id, start_date, end_date, start_date2, end_date2, source
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных круговой диаграммы: {str(e)}")

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
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных малых столбчатых диаграмм: {str(e)}")

@dashboards_router.get("/monthly-stacked-bars", response_model=Dict[str, List[Dict[str, Any]]])
async def get_monthly_stacked_bars(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(..., description="ID продукта для фильтрации"),
    start_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    end_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    start_date2: str = Query(..., description="Начальная дата второго периода в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    end_date2: str = Query(..., description="Конечная дата второго периода в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    aggregation_type: str = Query(..., description="Тип агрегации: 'month', 'week', или 'day'"),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов (например, 'Banki.ru', 'App Store', 'Google Play')"),
    cluster_id: Optional[int] = Query(None, description="Фильтр по конкретному ID кластера (опционально)")
):
    """
    Получение данных для stacked bar chart с количеством отзывов по кластерам для двух периодов и процентными изменениями.

    Параметры:
    - product_id: ID продукта для фильтрации отзывов (обязательно).
    - start_date: Начальная дата первого периода (обязательно, YYYY-MM-DD или YYYY-MM для месячной).
    - end_date: Конечная дата первого периода (обязательно, YYYY-MM-DD или YYYY-MM для месячной).
    - start_date2: Начальная дата второго периода (обязательно, YYYY-MM-DD или YYYY-MM для месячной).
    - end_date2: Конечная дата второго периода (обязательно, YYYY-MM-DD или YYYY-MM для месячной).
    - aggregation_type: Тип агрегации ('month', 'week', или 'day').
    - source: Фильтр по источнику отзывов (опционально, например, 'Banki.ru', 'App Store', 'Google Play').
    - cluster_id: Фильтр по конкретному ID кластера (опционально; если не указан, включаются все кластеры).

    Возвращает:
    - JSON объект с 'period1', 'period2', и 'changes' списками, содержащими даты агрегации, количество отзывов по кластерам и процентные изменения.
    """
    try:
        data = await stats_service.get_monthly_stacked_bars(
            db, product_id, start_date, end_date, start_date2, end_date2, aggregation_type, source, cluster_id
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных stacked bars: {str(e)}")

@dashboards_router.get("/tonality-stacked-bars", response_model=TonalityStackedBarsResponse)
async def get_tonality_stacked_bars(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    end_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    start_date2: str = Query(..., description="Начальная дата второго периода в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    end_date2: str = Query(..., description="Конечная дата второго периода в формате YYYY-MM-DD или YYYY-MM для месячной агрегации"),
    aggregation_type: str = Query(..., description="Тип агрегации: 'month', 'week', или 'day'"),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов (например, 'Banki.ru', 'App Store', 'Google Play')")
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
        raise HTTPException(status_code=500, detail=f"Ошибка при получении stacked bars по тональности: {str(e)}")

@dashboards_router.get("/line-and-bar-pie-chart", response_model=MonthlyPieChartResponse)
async def get_line_and_bar_pie_chart(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(..., description="ID продукта для фильтрации"),
    start_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD"),
    end_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD"),
    start_date2: str = Query(..., description="Начальная дата второго периода в формате YYYY-MM-DD"),
    end_date2: str = Query(..., description="Конечная дата второго периода в формате YYYY-MM-DD"),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов (например, 'Banki.ru', 'App Store', 'Google Play')"),
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
        raise HTTPException(status_code=500, detail=f"Ошибка при получении круговой диаграммы тональности: {str(e)}")


@dashboards_router.get(
    "/public-product-tree",
    response_model=List[ProductTreeNode],
    summary="Получить дерево иерархии продуктов (публичное)",
    description="Получить все продукты в виде иерархической древовидной структуры (категории → подкатегории → продукты) без аутентификации или параметров.",
    response_description="Список корневых узлов, представляющих иерархию продуктов."
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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Не удалось получить дерево продуктов")

@dashboards_router.get("/change-chart", response_model=ChangeChartResponse)
async def get_change_chart(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(..., description="ID продукта для фильтрации"),
    start_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD"),
    end_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD"),
    start_date2: str = Query(..., description="Начальная дата второго периода в формате YYYY-MM-DD"),
    end_date2: str = Query(..., description="Конечная дата второго периода в формате YYYY-MM-DD"),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов (например, 'Banki.ru', 'App Store', 'Google Play')"),
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
        raise HTTPException(status_code=500, detail=f"Ошибка при получении графика изменений: {str(e)}")
    
@dashboards_router.get("/reviews", response_model=ReviewsResponse)
async def get_reviews(
    db: DbSession,
    stats_service: StatsServiceDep,
    product_id: int = Query(...),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    cluster_id: Optional[int] = Query(None),
    source: Optional[str] = Query(None, description="Фильтр по источнику отзывов"),
    sentiment: Optional[str] = Query(None, description="Фильтр по тональности: 'positive', 'neutral', 'negative'"),
    order_by: str = Query("desc", description="Сортировка по дате: 'asc' или 'desc'"),
    page: int = Query(0, ge=0),
    size: int = Query(30, ge=1, le=100),
):
    """
    Получение списка отзывов по продукту с опциональной фильтрацией по кластеру, дате, источнику и тональности, с пагинацией.
    """
    try:
        if order_by not in ["asc", "desc"]:
            raise HTTPException(status_code=400, detail="order_by must be 'asc' or 'desc'")
        
        if sentiment and sentiment not in ["positive", "neutral", "negative"]:
            raise HTTPException(status_code=400, detail="sentiment must be 'positive', 'neutral', or 'negative'")
            
        data = await stats_service.get_reviews(
            db, product_id, start_date, end_date, cluster_id, source, sentiment, order_by, page, size
        )
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении отзывов: {str(e)}")
    
@dashboards_router.get(
    "/clusters",
    response_model=List[ClusterResponse],
    summary="Получить список кластеров (публичное)",
    description="Получить все кластеры без аутентификации.",
    response_description="Список кластеров с их ID, названиями и описаниями."
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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Не удалось получить кластеры")
    
@dashboards_router.post(
    "/reviews",
    response_model=Dict[str, Any],
    summary="Создать несколько отзывов (публичное)",
    description="Создать несколько отзывов массово без аутентификации. Отзывы сохраняются в таблицу для обработки моделью.",
    response_description="Статус и количество созданных отзывов."
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
      - `id`: Уникальный идентификатор отзыва в рамках этого запроса (обязательно).
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
          "created_count": 2,
          "message": "Reviews saved for model processing..."
        }
        ```
    - **Код 400 Bad Request**: Если входные данные некорректны.
    - **Код 500 Internal Server Error**: Если произошла ошибка сервера.
    """
    try:
        result = await stats_service.create_reviews_bulk(db, reviews_data)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при создании отзывов: {str(e)}")
    


@dashboards_router.post(
    "/predict",
    response_model=Dict[str, Any],
    summary="Анализ отзывов через нейронную модель (внешний сервис)",
    description="Отправляет отзывы на внешний ML-сервис для анализа и возвращает темы и тональности.",
    response_description="Результаты анализа отзывов от ML-сервиса."
)
async def analyze_reviews(
    reviews_data: ReviewBulkCreate
):
    """
    Анализ отзывов через внешний ML-сервис.

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
      - `id`: Уникальный идентификатор отзыва в рамках этого запроса (обязательно).
      - `text`: Текст отзыва (обязательно, не пустой, максимум 1000 символов).

    **Что получите в ответе**:
    - **Код 200 OK**: Результаты анализа от ML-сервиса.
      - **Формат JSON**:
        ```json
        {
          "predictions": [
            {
              "id": 1, 
              "topics": ["Обслуживание", "Мобильное приложение"], 
              "sentiments": ["положительно", "отрицательно"]
            },
            {
              "id": 2, 
              "topics": ["Кредитная карта"], 
              "sentiments": ["нейтрально"]
            }
          ]
        }
        ```
    - **Код 502 Bad Gateway**: Если ML-сервис недоступен.
    - **Код 504 Gateway Timeout**: Если ML-сервис не ответил вовремя.
    - **Код 500 Internal Server Error**: Если произошла другая ошибка.
    """
    try:
        # Преобразуем Pydantic модель в словарь для сериализации
        payload = {
            "data": [
                {"id": item.id, "text": item.text}
                for item in reviews_data.data
            ]
        }
        
        logger.info(f"Sending {len(payload['data'])} reviews to ML service")
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    ML_PREDICT_ENDPOINT,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=ML_TIMEOUT)
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"Successfully received predictions for {len(result.get('predictions', []))} reviews")
                        return result
                    elif response.status == 422:
                        error_detail = await response.text()
                        logger.error(f"ML service validation error: {error_detail}")
                        raise HTTPException(
                            status_code=400,
                            detail=f"Некорректные данные для ML-сервиса: {error_detail}"
                        )
                    elif response.status == 504:
                        logger.error("ML service timeout")
                        raise HTTPException(
                            status_code=504,
                            detail="ML-сервис не ответил вовремя. Попробуйте позже."
                        )
                    else:
                        error_detail = await response.text()
                        logger.error(f"ML service error {response.status}: {error_detail}")
                        raise HTTPException(
                            status_code=502,
                            detail=f"Ошибка ML-сервиса: {error_detail}"
                        )
                        
            except aiohttp.ClientConnectorError as e:
                logger.error(f"ML service connection error: {e}")
                raise HTTPException(
                    status_code=502,
                    detail="ML-сервис недоступен. Проверьте подключение."
                )
            except aiohttp.ServerTimeoutError:
                logger.error("ML service timeout")
                raise HTTPException(
                    status_code=504,
                    detail="Превышено время ожидания ответа от ML-сервиса."
                )
            except asyncio.TimeoutError:
                logger.error("ML service timeout")
                raise HTTPException(
                    status_code=504,
                    detail="Таймаут при обращении к ML-сервису."
                )
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при анализе отзывов: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера: {str(e)}"
        )