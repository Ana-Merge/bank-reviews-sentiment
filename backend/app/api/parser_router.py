from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from app.repositories.repositories import ReviewsForModelRepository
from app.services.parser_service import ParserService
from app.core.dependencies import DbSession

parsers_router = APIRouter(prefix="/api/v1/parsers", tags=["parsers"])

@parsers_router.post(
    "/run-bank-parser",
    response_model=Dict[str, Any],
    summary="Прасер сайта banki.ru",
    description="Запуск парсера для сбора отзывов с с айта banki.ru",
    response_description="Parser execution results"
)
async def run_bank_parser(
    db: DbSession,
    bank_slug: str = Query(..., description="Название банка (e.g., gazprombank, sberbank)"),
    products: List[str] = Query(..., description="Список продуктов для парсинга (e.g., debitcards, deposits, credits)"),
    start_date: Optional[str] = Query(None, description="Начальная дата для фильтрации (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Конечная дата для фильтрации (YYYY-MM-DD)"),
    max_pages: int = Query(100, description="Максимальное число страниц для одного продутка"),
    delay: float = Query(1.0, description="Задержка между запросами в секундах"),
    parser_service: ParserService = Depends(lambda: ParserService(ReviewsForModelRepository()))
):
    """
    Запуск парсера banki.ru для сбора отзывов по конкретному банку.

    **Что передавать**:
    - **Параметры запроса**:
      - `bank_slug`: Slug банка (обязательно, например: "gazprombank")
      - `products`: Список продуктов (обязательно, например: ["debitcards", "deposits"])
      - `start_date`: Начальная дата фильтрации (опционально, формат: YYYY-MM-DD)
      - `end_date`: Конечная дата фильтрации (опционально, формат: YYYY-MM-DD)
      - `max_pages`: Максимальное количество страниц (по умолчанию 100)
      - `delay`: Задержка между запросами в секундах (по умолчанию 1.0)

    **Что получите в ответе**:
    - **Код 200 OK**: Результаты работы парсера.
      - **Формат JSON**:
        ```json
        {
          "status": "success",
          "bank_slug": "gazprombank",
          "products_processed": ["debitcards", "deposits"],
          "total_reviews_parsed": 1500,
          "total_saved": 1500,
          "start_date": "2025-01-01",
          "end_date": "2025-09-17",
          "message": "Successfully parsed and saved 1500 reviews for bank gazprombank"
        }
        ```
    """
    try:
        result = await parser_service.run_parser(
            db, bank_slug, products, start_date, end_date, max_pages, delay
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Parser error: {str(e)}")
    
@parsers_router.post(
    "/process-parsed-reviews",
    response_model=Dict[str, Any],
    summary="Обработка спарсенных отзывов",
    description="Перенос данных из временной таблицы в основные таблицы reviews и review_products",
    response_description="Результаты обработки отзывов"
)
async def process_parsed_reviews(
    db: DbSession,
    bank_slug: str = Query(..., description="Slug банка для обработки"),
    product_name: str = Query(..., description="Название продукта для обработки"),
    limit: int = Query(1000, description="Максимальное количество отзывов для обработки"),
    mark_processed: bool = Query(True, description="Помечать ли обработанные отзывы"),
    parser_service: ParserService = Depends(lambda: ParserService(ReviewsForModelRepository()))
):
    """
    Перенос спарсенных отзывов из reviews_for_model в основные таблицы.
    
    **Что передавать**:
    - **Параметры запроса**:
      - `bank_slug`: Slug банка (обязательно)
      - `product_name`: Название продукта (обязательно)
      - `limit`: Максимальное количество отзывов (по умолчанию 1000)
      - `mark_processed`: Помечать как обработанные (по умолчанию True)
    
    **Что получите в ответе**:
    - **Код 200 OK**: Результаты обработки.
      - **Формат JSON**:
        ```json
        {
          "status": "success",
          "bank_slug": "gazprombank",
          "product_name": "debitcards",
          "reviews_processed": 150,
          "reviews_created": 150,
          "products_created": 1,
          "message": "Successfully processed 150 reviews"
        }
        ```
    """
    try:
        result = await parser_service.process_parsed_reviews(
            db, bank_slug, product_name, limit, mark_processed
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")

@parsers_router.post(
    "/run-sravni-parser",
    response_model=Dict[str, Any],
    summary="Запуск парсера sravni.ru",
    description="Запускает парсер с сайта sravni.ru для конкретного продукта",
    response_description="Результаты парсинга"
)
async def run_sravni_parser(
    db: DbSession,
    bank_slugs: List[str] = Query(..., description="Список банков"),
    start_date: Optional[str] = Query(None, description="Начальная дата для фильтрации (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Конечная дата для фильтрации (YYYY-MM-DD)"),
    max_pages: int = Query(100, description="Максимальное количество страниц для банка"),
    delay: float = Query(1.0, description="Задержка между запросами в секундах"),
    parser_service: ParserService = Depends(lambda: ParserService(ReviewsForModelRepository()))
):
    """
    Запуск парсера sravni.ru для сбора отзывов по банкам.

    **Что передавать**:
    - **Параметры запроса**:
      - `bank_slugs`: Список slug банков (обязательно, например: ["gazprombank", "sberbank"])
      - `start_date`: Начальная дата фильтрации (опционально, формат: YYYY-MM-DD)
      - `end_date`: Конечная дата фильтрации (опционально, формат: YYYY-MM-DD)
      - `max_pages`: Максимальное количество страниц (по умолчанию 100)
      - `delay`: Задержка между запросами в секундах (по умолчанию 1.0)

    **Что получите в ответе**:
    - **Код 200 OK**: Результаты работы парсера.
      - **Формат JSON**:
        ```json
        {
          "status": "success",
          "bank_slugs": ["gazprombank", "sberbank"],
          "total_reviews_parsed": 1500,
          "total_saved": 1500,
          "start_date": "2025-01-01",
          "end_date": "2025-09-17",
          "message": "Successfully parsed and saved 1500 reviews from sravni.ru"
        }
        ```
    """
    try:
        result = await parser_service.run_sravni_parser(
            db, bank_slugs, start_date, end_date, max_pages, delay
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sravni parser error: {str(e)}")