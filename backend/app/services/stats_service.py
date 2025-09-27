import pandas as pd
from fastapi import HTTPException
from typing import List, Dict, Any, Optional
from datetime import date, timedelta, datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, func
from calendar import monthrange
from sqlalchemy import select
from app.repositories.repositories import (
    ProductRepository, ReviewRepository, MonthlyStatsRepository, ClusterStatsRepository,
    ClusterRepository, ReviewClusterRepository, ReviewCluster, Review
)
from app.models.user_models import User
from app.schemas.schemas import ReviewResponse, ClusterResponse, ReviewBulkCreate
from app.models.models import ProductType, Sentiment, ReviewProduct

class StatsService:
    def __init__(
        self,
        product_repo: ProductRepository,
        review_repo: ReviewRepository,
        monthly_stats_repo: MonthlyStatsRepository,
        cluster_stats_repo: ClusterStatsRepository,
        cluster_repo: ClusterRepository,
        review_cluster_repo: ReviewClusterRepository,
    ):
        self._product_repo = product_repo
        self._review_repo = review_repo
        self._monthly_stats_repo = monthly_stats_repo
        self._cluster_stats_repo = cluster_stats_repo
        self._cluster_repo = cluster_repo
        self._review_cluster_repo = review_cluster_repo

    async def get_product_stats(
        self, 
        session: AsyncSession, 
        start_date: str, 
        end_date: str,
        start_date2: str, 
        end_date2: str,
        source: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        def parse_date(date_str: str) -> date:
            """Parse date string in YYYY-MM-DD format."""
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError as e:
                raise ValueError(f"Invalid date format for {date_str}. Expected YYYY-MM-DD") from e

        try:
            start_date_parsed = parse_date(start_date)
            end_date_parsed = parse_date(end_date)
            start_date2_parsed = parse_date(start_date2)
            end_date2_parsed = parse_date(end_date2)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        products = await self._product_repo.get_all(session)
        stats = []
        for product in products:
            if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
                descendants = await self._product_repo.get_all_descendants(session, product.id)
                product_ids = [p.id for p in descendants] + [product.id]
            else:
                product_ids = [product.id]

            total_count = await self._review_repo.count_by_product_and_period(
                session, product_ids, start_date_parsed, end_date_parsed, source=source
            )
            tonality = await self._review_repo.get_tonality_counts_by_product_and_period(
                session, product_ids, start_date_parsed, end_date_parsed, source=source
            )
            avg_rating = await self._review_repo.get_avg_rating_by_products(
                session, product_ids, source=source
            ) if total_count > 0 else 0.0

            prev_count = await self._review_repo.count_by_product_and_period(
                session, product_ids, start_date2_parsed, end_date2_parsed, source=source
            )

            change_percent = (
                round(((total_count - prev_count) / prev_count * 100), 1)
                if prev_count > 0
                else 100.0 if total_count > 0 else 0.0
            )
            change_color = "green" if change_percent >= 0 else "red"

            stats.append({
                "product_name": product.name,
                "change_percent": change_percent,
                "change_color": change_color,
                "count": total_count,
                "tonality": tonality,
                "avg_rating": round(avg_rating, 1) if avg_rating else 0.0
            })
        return stats

    async def get_monthly_review_count(
        self, session: AsyncSession, product_id: int, start_date: str, end_date: str,
        start_date2: str, end_date2: str, aggregation_type: str, source: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        def parse_date(date_str: str, agg_type: str, is_start_date: bool) -> datetime.date:
            """Parse date string based on aggregation type and whether it's a start or end date."""
            try:
                if agg_type == "month":
                    year, month = map(int, date_str.split("-"))
                    if is_start_date:
                        parsed_date = datetime(year, month, 1).date()
                    else:
                        _, last_day = monthrange(year, month)
                        parsed_date = datetime(year, month, last_day).date()
                else:
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                return parsed_date
            except ValueError as e:
                raise ValueError(f"Invalid date format for {date_str}. Expected {'YYYY-MM' if agg_type == 'month' else 'YYYY-MM-DD'}") from e

        try:
            start_date_parsed = parse_date(start_date, aggregation_type, is_start_date=True)
            end_date_parsed = parse_date(end_date, aggregation_type, is_start_date=False)
            start_date2_parsed = parse_date(start_date2, aggregation_type, is_start_date=True)
            end_date2_parsed = parse_date(end_date2, aggregation_type, is_start_date=False)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": [], "period2": [], "changes": []}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        if aggregation_type not in ["month", "week", "day"]:
            raise ValueError("Invalid aggregation type. Must be 'month', 'week', or 'day'.")

        if aggregation_type == "month":
            date_trunc = "month"
            date_format = "%Y-%m"
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"

        # Query for period1
        agg_date = func.date_trunc(date_trunc, Review.date).label("agg_date")
        period1_query = select(
            agg_date,
            Review.sentiment,
            func.count(func.distinct(Review.id)).label("count")
        ).join(ReviewProduct).where(
            and_(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed,
                Review.sentiment.isnot(None)
            )
        )
        if source:
            period1_query = period1_query.where(Review.source == source)
        period1_query = period1_query.group_by(agg_date, Review.sentiment).order_by(agg_date)
        period1_result = await session.execute(period1_query)
        period1_data = period1_result.all()

        period1_dict = {}
        for row in period1_data:
            agg_date_str = row.agg_date.strftime(date_format)
            if agg_date_str not in period1_dict:
                period1_dict[agg_date_str] = {"positive": 0, "neutral": 0, "negative": 0}
            period1_dict[agg_date_str][row.sentiment] = row.count
        
        # Query for period2
        period2_query = select(
            agg_date,
            Review.sentiment,
            func.count(func.distinct(Review.id)).label("count")
        ).join(ReviewProduct).where(
            and_(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date2_parsed,
                Review.date <= end_date2_parsed,
                Review.sentiment.isnot(None)
            )
        )
        if source:
            period2_query = period2_query.where(Review.source == source)
        period2_query = period2_query.group_by(agg_date, Review.sentiment).order_by(agg_date)
        period2_result = await session.execute(period2_query)
        period2_data = period2_result.all()

        period2_dict = {}
        for row in period2_data:
            agg_date_str = row.agg_date.strftime(date_format)
            if agg_date_str not in period2_dict:
                period2_dict[agg_date_str] = {"positive": 0, "neutral": 0, "negative": 0}
            period2_dict[agg_date_str][row.sentiment] = row.count
        
        # Generate date ranges
        def generate_date_range(start: datetime.date, end: datetime.date, agg_type: str) -> List[str]:
            result = []
            current = start
            if agg_type == "week":
                # Начинаем с понедельника недели, содержащей start date
                days_to_monday = current.weekday()
                current = current - timedelta(days=days_to_monday)
            while current <= end:
                if agg_type == "month":
                    month_str = current.strftime("%Y-%m")
                    result.append(month_str)
                    if current.month == 12:
                        current = date(current.year + 1, 1, 1)
                    else:
                        current = date(current.year, current.month + 1, 1)
                elif agg_type == "week":
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=7)
                else:  # day
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
            return result

        period1_dates = generate_date_range(start_date_parsed, end_date_parsed, aggregation_type)
        period2_dates = generate_date_range(start_date2_parsed, end_date2_parsed, aggregation_type)

        # Format period1 and period2
        period1 = [
            {
                "aggregation": date,
                "tonality": period1_dict.get(date, {"positive": 0, "neutral": 0, "negative": 0})
            }
            for date in period1_dates
        ]
        period2 = [
            {
                "aggregation": date,
                "tonality": period2_dict.get(date, {"positive": 0, "neutral": 0, "negative": 0})
            }
            for date in period2_dates
        ]

        # Calculate changes - сравниваем соответствующие позиции в периодах
        changes = []
        min_length = min(len(period1), len(period2))
        
        for i in range(min_length):
            period1_item = period1[i]
            period2_item = period2[i]
            
            period1_tonality = period1_item["tonality"]
            period2_tonality = period2_item["tonality"]
            
            # Безопасный расчет процентных изменений
            def safe_percentage_change(current, previous):
                if previous > 0:
                    return round(((current - previous) / previous * 100), 1)
                elif current > 0:
                    return 100.0  # Рост от 0 до положительного значения
                elif previous > 0 and current == 0:
                    return -100.0  # Падение от положительного до 0
                else:
                    return 0.0  # Оба значения 0

            percentage_change = {
                "positive": safe_percentage_change(period1_tonality["positive"], period2_tonality["positive"]),
                "neutral": safe_percentage_change(period1_tonality["neutral"], period2_tonality["neutral"]),
                "negative": safe_percentage_change(period1_tonality["negative"], period2_tonality["negative"])
            }
            
            # Используем дату из первого периода для агрегации
            changes.append({
                "aggregation": period1_item["aggregation"],
                "percentage_change": percentage_change
            })

        # Если периоды разной длины, добавляем оставшиеся даты без изменений
        if len(period1) > min_length:
            for i in range(min_length, len(period1)):
                changes.append({
                    "aggregation": period1[i]["aggregation"],
                    "percentage_change": {
                        "positive": 100.0 if period1[i]["tonality"]["positive"] > 0 else 0.0,
                        "neutral": 100.0 if period1[i]["tonality"]["neutral"] > 0 else 0.0,
                        "negative": 100.0 if period1[i]["tonality"]["negative"] > 0 else 0.0
                    }
                })
        elif len(period2) > min_length:
            for i in range(min_length, len(period2)):
                changes.append({
                    "aggregation": period2[i]["aggregation"],
                    "percentage_change": {
                        "positive": -100.0 if period2[i]["tonality"]["positive"] > 0 else 0.0,
                        "neutral": -100.0 if period2[i]["tonality"]["neutral"] > 0 else 0.0,
                        "negative": -100.0 if period2[i]["tonality"]["negative"] > 0 else 0.0
                    }
                })

        return {
            "period1": period1,
            "period2": period2,
            "changes": changes
        }

    async def get_bar_chart_changes(
        self, session: AsyncSession, product_id: int, start_date: str, end_date: str,
        start_date2: str, end_date2: str, aggregation_type: str, source: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        def parse_date(date_str: str, agg_type: str, is_start_date: bool) -> datetime.date:
            try:
                if agg_type == "month":
                    year, month = map(int, date_str.split("-"))
                    if is_start_date:
                        parsed_date = datetime(year, month, 1).date()
                    else:
                        _, last_day = monthrange(year, month)
                        parsed_date = datetime(year, month, last_day).date()
                else:
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                return parsed_date
            except ValueError as e:
                raise ValueError(f"Invalid date format for {date_str}. Expected {'YYYY-MM' if agg_type == 'month' else 'YYYY-MM-DD'}") from e

        try:
            start_date_parsed = parse_date(start_date, aggregation_type, is_start_date=True)
            end_date_parsed = parse_date(end_date, aggregation_type, is_start_date=False)
            start_date2_parsed = parse_date(start_date2, aggregation_type, is_start_date=True)
            end_date2_parsed = parse_date(end_date2, aggregation_type, is_start_date=False)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": [], "period2": [], "changes": []}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        if aggregation_type not in ["month", "week", "day"]:
            raise ValueError("Invalid aggregation type. Must be 'month', 'week', or 'day'.")

        if aggregation_type == "month":
            date_trunc = "month"
            date_format = "%Y-%m"
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"

        # Query for period1 - общее количество отзывов
        agg_date = func.date_trunc(date_trunc, Review.date).label("agg_date")
        period1_query = select(
            agg_date,
            func.count(func.distinct(Review.id)).label("count")
        ).join(ReviewProduct).where(
            and_(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed
            )
        )
        if source:
            period1_query = period1_query.where(Review.source == source)
        period1_query = period1_query.group_by(agg_date).order_by(agg_date)
        period1_result = await session.execute(period1_query)
        period1_data = period1_result.all()

        period1_dict = {}
        for row in period1_data:
            agg_date_str = row.agg_date.strftime(date_format)
            period1_dict[agg_date_str] = row.count
        
        # Query for period2 - общее количество отзывов
        period2_query = select(
            agg_date,
            func.count(func.distinct(Review.id)).label("count")
        ).join(ReviewProduct).where(
            and_(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date2_parsed,
                Review.date <= end_date2_parsed
            )
        )
        if source:
            period2_query = period2_query.where(Review.source == source)
        period2_query = period2_query.group_by(agg_date).order_by(agg_date)
        period2_result = await session.execute(period2_query)
        period2_data = period2_result.all()

        period2_dict = {}
        for row in period2_data:
            agg_date_str = row.agg_date.strftime(date_format)
            period2_dict[agg_date_str] = row.count
        
        # Generate date ranges
        def generate_date_range(start: datetime.date, end: datetime.date, agg_type: str) -> List[str]:
            result = []
            current = start
            if agg_type == "week":
                days_to_monday = current.weekday()
                current = current - timedelta(days=days_to_monday)
            while current <= end:
                if agg_type == "month":
                    month_str = current.strftime("%Y-%m")
                    result.append(month_str)
                    if current.month == 12:
                        current = date(current.year + 1, 1, 1)
                    else:
                        current = date(current.year, current.month + 1, 1)
                elif agg_type == "week":
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=7)
                else:  # day
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
            return result

        period1_dates = generate_date_range(start_date_parsed, end_date_parsed, aggregation_type)
        period2_dates = generate_date_range(start_date2_parsed, end_date2_parsed, aggregation_type)

        # Format period1 and period2
        period1 = [
            {
                "aggregation": date,
                "count": period1_dict.get(date, 0)
            }
            for date in period1_dates
        ]
        period2 = [
            {
                "aggregation": date,
                "count": period2_dict.get(date, 0)
            }
            for date in period2_dates
        ]

        # Calculate changes - сравниваем соответствующие позиции в периодах
        changes = []
        min_length = min(len(period1), len(period2))
        
        for i in range(min_length):
            period1_count = period1[i]["count"]
            period2_count = period2[i]["count"]
            
            # Безопасный расчет процентных изменений
            if period2_count > 0:
                change_percent = round(((period1_count - period2_count) / period2_count * 100), 1)
            elif period1_count > 0:
                change_percent = 100.0  # Рост от 0 до положительного значения
            elif period2_count > 0 and period1_count == 0:
                change_percent = -100.0  # Падение от положительного до 0
            else:
                change_percent = 0.0  # Оба значения 0
            
            changes.append({
                "aggregation": period1[i]["aggregation"],
                "change_percent": change_percent
            })

        # Если периоды разной длины, добавляем оставшиеся даты без изменений
        if len(period1) > min_length:
            for i in range(min_length, len(period1)):
                changes.append({
                    "aggregation": period1[i]["aggregation"],
                    "change_percent": 100.0 if period1[i]["count"] > 0 else 0.0
                })
        elif len(period2) > min_length:
            for i in range(min_length, len(period2)):
                changes.append({
                    "aggregation": period2[i]["aggregation"],
                    "change_percent": -100.0 if period2[i]["count"] > 0 else 0.0
                })

        return {
            "period1": period1,
            "period2": period2,
            "changes": changes
        }

    async def get_monthly_pie_chart(
        self, session: AsyncSession, product_id: int, start_date: str, end_date: str,
        start_date2: str, end_date2: str, source: Optional[str] = None
    ) -> Dict[str, Any]:
        def parse_date(date_str: str) -> date:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError as e:
                raise ValueError(f"Invalid date format for {date_str}. Expected YYYY-MM-DD") from e

        try:
            start_date_parsed = parse_date(start_date)
            end_date_parsed = parse_date(end_date)
            start_date2_parsed = parse_date(start_date2)
            end_date2_parsed = parse_date(end_date2)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {
                "period1": {"labels": [], "data": [], "colors": [], "total": 0},
                "period2": {"labels": [], "data": [], "colors": [], "total": 0},
                "changes": {"labels": [], "percentage_point_changes": []}
            }

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        total1 = await self._review_repo.count_by_product_and_period(session, product_ids, start_date_parsed, end_date_parsed, source)
        tonality1 = await self._review_repo.get_tonality_counts_by_product_and_period(session, product_ids, start_date_parsed, end_date_parsed, source)
        if total1 > 0:
            data1 = [
                round(tonality1.get('negative', 0) / total1 * 100, 1),
                round(tonality1.get('neutral', 0) / total1 * 100, 1),
                round(tonality1.get('positive', 0) / total1 * 100, 1)
            ]
        else:
            data1 = [0.0, 0.0, 0.0]

        total2 = await self._review_repo.count_by_product_and_period(session, product_ids, start_date2_parsed, end_date2_parsed, source)
        tonality2 = await self._review_repo.get_tonality_counts_by_product_and_period(session, product_ids, start_date2_parsed, end_date2_parsed, source)
        if total2 > 0:
            data2 = [
                round(tonality2.get('negative', 0) / total2 * 100, 1),
                round(tonality2.get('neutral', 0) / total2 * 100, 1),
                round(tonality2.get('positive', 0) / total2 * 100, 1)
            ]
        else:
            data2 = [0.0, 0.0, 0.0]

        percentage_point_changes = [data1[i] - data2[i] for i in range(3)]
        labels = ["negative", "neutral", "positive"]
        colors = ["red", "yellow", "green"]

        return {
            "period1": {"labels": labels, "data": data1, "colors": colors, "total": total1},
            "period2": {"labels": labels, "data": data2, "colors": colors, "total": total2},
            "changes": {"labels": labels, "percentage_point_changes": percentage_point_changes}
        }

    async def get_change_chart(
        self, session: AsyncSession, product_id: int, start_date: str, end_date: str,
        start_date2: str, end_date2: str, source: Optional[str] = None
    ) -> Dict[str, Any]:
        def parse_date(date_str: str) -> date:
            """Parse date string in YYYY-MM-DD format."""
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError as e:
                raise ValueError(f"Invalid date format for {date_str}. Expected YYYY-MM-DD") from e

        try:
            start_date_parsed = parse_date(start_date)
            end_date_parsed = parse_date(end_date)
            start_date2_parsed = parse_date(start_date2)
            end_date2_parsed = parse_date(end_date2)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"total": 0, "change_percent": 0.0}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        total = await self._review_repo.count_by_product_and_period(session, product_ids, start_date_parsed, end_date_parsed, source=source)
        prev_total = await self._review_repo.count_by_product_and_period(session, product_ids, start_date2_parsed, end_date2_parsed, source=source)
        change_percent = round(((total - prev_total) / prev_total * 100), 1) if prev_total > 0 else 100.0 if total > 0 else 0.0

        return {
            "total": total,
            "change_percent": change_percent
        }

    async def get_small_bar_charts(
        self, session: AsyncSession, product_id: int, start_date: date, end_date: date, user: User, cluster_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        logger.debug(f"Fetching small bar charts for product_id={product_id}, cluster_id={cluster_id}, start_date={start_date}, end_date={end_date}")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            logger.warning(f"Product with ID {product_id} not found")
            return []

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
            logger.debug(f"Product IDs (including descendants): {product_ids}")
        else:
            product_ids = [product_id]
            logger.debug(f"Product ID: {product_id}")

        if cluster_id is not None:
            cluster = await self._cluster_repo.get_by_id(session, cluster_id)
            if not cluster:
                logger.warning(f"Cluster with ID {cluster_id} not found")
                return []
            clusters = [cluster]
            logger.debug(f"Processing single cluster: ID={cluster.id}, Name={cluster.name}")
        else:
            clusters = await self._cluster_repo.get_all(session)
            logger.debug(f"Processing {len(clusters)} clusters: {[c.name for c in clusters]}")

        if not clusters:
            logger.warning("No clusters found")
            return []

        result = []
        prev_start = start_date - timedelta(days=30)
        
        for cluster in clusters:
            # Подсчет общего количества отзывов для кластера (с учетом связи через review_products)
            total_count_query = select(func.count(func.distinct(Review.id))).select_from(ReviewCluster)\
                .join(Review).join(ReviewProduct).where(
                    and_(
                        ReviewProduct.product_id.in_(product_ids),
                        Review.date >= start_date,
                        Review.date <= end_date,
                        ReviewCluster.cluster_id == cluster.id
                    )
                )
            total_count_result = await session.execute(total_count_query)
            total_count = total_count_result.scalar() or 0
            logger.debug(f"Total count for cluster {cluster.name}: {total_count}")
            
            if total_count == 0:
                continue

            # Подсчет количества отзывов за предыдущий период
            prev_count_query = select(func.count(func.distinct(Review.id))).select_from(ReviewCluster)\
                .join(Review).join(ReviewProduct).where(
                    and_(
                        ReviewProduct.product_id.in_(product_ids),
                        Review.date >= prev_start,
                        Review.date < start_date,
                        ReviewCluster.cluster_id == cluster.id
                    )
                )
            prev_count_result = await session.execute(prev_count_query)
            prev_count = prev_count_result.scalar() or 0
            logger.debug(f"Previous count: {prev_count}")
            
            change_percent = round(((total_count - prev_count) / prev_count * 100), 1) if prev_count > 0 else 100.0 if total_count > 0 else 0.0
            logger.debug(f"Change percent: {change_percent}")

            # Запрос для тональности с учетом взвешенных counts
            effective_sentiment = func.coalesce(ReviewCluster.sentiment_contribution, Review.sentiment).label("effective_sentiment")
            statement = select(
                effective_sentiment,
                func.sum(ReviewCluster.topic_weight).label("weighted_count")
            ).select_from(ReviewCluster)\
            .join(Review).join(ReviewProduct).where(
                and_(
                    ReviewProduct.product_id.in_(product_ids),
                    Review.date >= start_date,
                    Review.date <= end_date,
                    ReviewCluster.cluster_id == cluster.id
                )
            ).group_by(effective_sentiment)

            result_counts = await session.execute(statement)
            rows = result_counts.fetchall()
            logger.debug(f"Tonality query results for cluster {cluster.name}: {rows}")

            tonality = {Sentiment.POSITIVE: 0.0, Sentiment.NEUTRAL: 0.0, Sentiment.NEGATIVE: 0.0}
            for row in rows:
                sentiment = row.effective_sentiment
                if sentiment:
                    tonality[sentiment] = float(row.weighted_count or 0.0)
                else:
                    logger.debug(f"Found null effective_sentiment for cluster {cluster.name}")

            total_tonality = sum(tonality.values())
            logger.debug(f"Tonality for cluster {cluster.name}: {tonality}, Total: {total_tonality}")

            # Расчет процентов для тональности
            data = []
            if total_tonality > 0:
                data = [
                    {"label": "Негатив", "percent": round(tonality[Sentiment.NEGATIVE] / total_tonality * 100, 1), "color": "orange"},
                    {"label": "Нейтрал", "percent": round(tonality[Sentiment.NEUTRAL] / total_tonality * 100, 1), "color": "cyan"},
                    {"label": "Позитив", "percent": round(tonality[Sentiment.POSITIVE] / total_tonality * 100, 1), "color": "blue"}
                ]
            else:
                data = [
                    {"label": "Негатив", "percent": 0.0, "color": "orange"},
                    {"label": "Нейтрал", "percent": 0.0, "color": "cyan"},
                    {"label": "Позитив", "percent": 0.0, "color": "blue"}
                ]

            result.append({
                "title": cluster.name,
                "reviews_count": int(total_count),
                "change_percent": int(change_percent),
                "data": data
            })

        logger.debug(f"Final result: {result}")
        return result

    async def get_monthly_stacked_bars(
        self, session: AsyncSession, product_id: int, start_date: str, end_date: str,
        start_date2: str, end_date2: str, aggregation_type: str, source: Optional[str] = None, cluster_id: Optional[int] = None
    ) -> Dict[str, List[Dict[str, Any]]]:

        def parse_date(date_str: str, agg_type: str, is_start_date: bool) -> datetime.date:
            """Parse date string based on aggregation type and whether it's a start or end date."""
            try:
                if agg_type == "month":
                    year, month = map(int, date_str.split("-"))
                    if is_start_date:
                        parsed_date = datetime(year, month, 1).date()
                    else:
                        _, last_day = monthrange(year, month)
                        parsed_date = datetime(year, month, last_day).date()
                else:
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                return parsed_date
            except ValueError as e:
                raise ValueError(f"Invalid date format for {date_str}. Expected {'YYYY-MM' if agg_type == 'month' else 'YYYY-MM-DD'}") from e

        try:
            start_date_parsed = parse_date(start_date, aggregation_type, is_start_date=True)
            end_date_parsed = parse_date(end_date, aggregation_type, is_start_date=False)
            start_date2_parsed = parse_date(start_date2, aggregation_type, is_start_date=True) if start_date2 else None
            end_date2_parsed = parse_date(end_date2, aggregation_type, is_start_date=False) if end_date2 else None
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed and end_date2_parsed and start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": [], "period2": [], "changes": []}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        if aggregation_type not in ["month", "week", "day"]:
            raise ValueError("Invalid aggregation type. Must be 'month', 'week', or 'day'.")

        if aggregation_type == "month":
            date_trunc = "month"
            date_format = "%Y-%m"
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"

        if cluster_id is not None:
            cluster = await self._cluster_repo.get_by_id(session, cluster_id)
            if not cluster:
                return {"period1": [], "period2": [], "changes": []}
            clusters = [cluster]
        else:
            clusters = await self._cluster_repo.get_all(session)

        if not clusters:
            return {"period1": [], "period2": [], "changes": []}

        # Query for period1 - исправленный запрос с учетом связи через review_products
        agg_date = func.date_trunc(date_trunc, Review.date).label("agg_date")
        period1_query = select(
            agg_date,
            ReviewCluster.cluster_id,
            func.count(func.distinct(Review.id)).label("total")  # Учитываем уникальные отзывы
        ).select_from(ReviewCluster)\
        .join(Review).join(ReviewProduct).where(
            and_(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed,
                ReviewCluster.cluster_id.in_([c.id for c in clusters])
            )
        )
        if source:
            period1_query = period1_query.where(Review.source == source)
        period1_query = period1_query.group_by(
            agg_date,
            ReviewCluster.cluster_id
        ).order_by(agg_date)

        period1_result = await session.execute(period1_query)
        period1_data = period1_result.all()

        period1_dict = {}
        cluster_names = {c.id: c.name for c in clusters}
        for row in period1_data:
            agg_date_str = row.agg_date.strftime(date_format)
            if agg_date_str not in period1_dict:
                period1_dict[agg_date_str] = {}
            cluster_name = cluster_names.get(row.cluster_id, f"Cluster_{row.cluster_id}")
            period1_dict[agg_date_str][cluster_name] = row.total

        period2_dict = {}
        if start_date2_parsed and end_date2_parsed:
            # Query for period2 - аналогично исправленный запрос
            period2_query = select(
                agg_date,
                ReviewCluster.cluster_id,
                func.count(func.distinct(Review.id)).label("total")
            ).select_from(ReviewCluster)\
            .join(Review).join(ReviewProduct).where(
                and_(
                    ReviewProduct.product_id.in_(product_ids),
                    Review.date >= start_date2_parsed,
                    Review.date <= end_date2_parsed,
                    ReviewCluster.cluster_id.in_([c.id for c in clusters])
                )
            )
            if source:
                period2_query = period2_query.where(Review.source == source)
            period2_query = period2_query.group_by(
                agg_date,
                ReviewCluster.cluster_id
            ).order_by(agg_date)

            period2_result = await session.execute(period2_query)
            period2_data = period2_result.all()

            for row in period2_data:
                agg_date_str = row.agg_date.strftime(date_format)
                if agg_date_str not in period2_dict:
                    period2_dict[agg_date_str] = {}
                cluster_name = cluster_names.get(row.cluster_id, f"Cluster_{row.cluster_id}")
                period2_dict[agg_date_str][cluster_name] = row.total

        def generate_date_range(start: date, end: date, agg_type: str) -> List[str]:
            result = []
            current = start
            if agg_type == "week":
                days_to_monday = current.weekday()
                current = current - timedelta(days=days_to_monday)
            while current <= end:
                if agg_type == "month":
                    month_str = current.strftime("%Y-%m")
                    result.append(month_str)
                    if current.month == 12:
                        current = date(current.year + 1, 1, 1)
                    else:
                        current = date(current.year, current.month + 1, 1)
                elif agg_type == "week":
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=7)
                else:
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
            return result

        period1_dates = generate_date_range(start_date_parsed, end_date_parsed, aggregation_type)
        period2_dates = generate_date_range(start_date2_parsed, end_date2_parsed, aggregation_type) if start_date2_parsed and end_date2_parsed else []

        default_clusters = {c.name: 0 for c in clusters}
        period1 = [
            {
                "aggregation": date,
                "clusters": {**default_clusters, **period1_dict.get(date, {})}
            }
            for date in period1_dates
        ]

        period2 = [
            {
                "aggregation": date,
                "clusters": {**default_clusters, **period2_dict.get(date, {})}
            }
            for date in period2_dates
        ]

        # Calculate changes - теперь сравниваем второй период с первым (p2 относительно p1)
        changes = []
        min_length = min(len(period1), len(period2))
        
        for i in range(min_length):
            period1_item = period1[i]
            period2_item = period2[i]
            
            period1_clusters = period1_item["clusters"]
            period2_clusters = period2_item["clusters"]

            percentage_change = {}
            for cluster_name in default_clusters:
                p1_count = period1_clusters[cluster_name]  # Первый период (база для сравнения)
                p2_count = period2_clusters[cluster_name]  # Второй период (сравниваемый)
                
                # Безопасный расчет процентных изменений: (p2 - p1) / p1 * 100
                if p2_count > 0:
                    change_percent = round(((p1_count - p2_count) / p2_count * 100), 1)
                elif p1_count > 0:
                    change_percent = 100.0  # Рост от 0 до положительного значения
                elif p2_count > 0 and p1_count == 0:
                    change_percent = -100.0  # Падение от положительного до 0
                else:
                    change_percent = 0.0  # Оба значения 0
                    
                percentage_change[cluster_name] = change_percent

            changes.append({
                "aggregation": period1_item["aggregation"],  # Используем дату из первого периода
                "percentage_change": percentage_change
            })

        # Если периоды разной длины, добавляем оставшиеся даты
        if len(period1) > min_length:
            for i in range(min_length, len(period1)):
                # Для дат, которые есть только в первом периоде - изменения -100% (потому что во втором периоде их нет)
                period1_clusters = period1[i]["clusters"]
                percentage_change = {}
                for cluster_name in default_clusters:
                    p1_count = period1_clusters[cluster_name]
                    percentage_change[cluster_name] = -100.0 if p1_count > 0 else 0.0
                    
                changes.append({
                    "aggregation": period1[i]["aggregation"],
                    "percentage_change": percentage_change
                })
        elif len(period2) > min_length:
            for i in range(min_length, len(period2)):
                # Для дат, которые есть только во втором периоде - изменения +100% (потому что в первом периоде их нет)
                period2_clusters = period2[i]["clusters"]
                percentage_change = {}
                for cluster_name in default_clusters:
                    p2_count = period2_clusters[cluster_name]
                    percentage_change[cluster_name] = 100.0 if p2_count > 0 else 0.0
                    
                changes.append({
                    "aggregation": period2[i]["aggregation"],
                    "percentage_change": percentage_change
                })

        return {
            "period1": period1,
            "period2": period2,
            "changes": changes
        }                                                 
    
    
    async def get_tonality_pie_chart(
        self, session: AsyncSession, product_id: int, start_date: str, end_date: str,
        start_date2: str, end_date2: str, source: Optional[str] = None
    ) -> Dict[str, Any]:
        def parse_date(date_str: str) -> date:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError as e:
                raise ValueError(f"Invalid date format for {date_str}. Expected YYYY-MM-DD") from e

        try:
            start_date_parsed = parse_date(start_date)
            end_date_parsed = parse_date(end_date)
            start_date2_parsed = parse_date(start_date2)
            end_date2_parsed = parse_date(end_date2)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {
                "period1": {"labels": [], "data": [], "colors": [], "total": 0},
                "period2": {"labels": [], "data": [], "colors": [], "total": 0},
                "changes": {"labels": [], "percentage_point_changes": []}
            }

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        # Period 1 - с учетом связи через review_products
        total1 = await self._review_repo.count_by_product_and_period(session, product_ids, start_date_parsed, end_date_parsed, source)
        tonality1 = await self._review_repo.get_tonality_counts_by_product_and_period(session, product_ids, start_date_parsed, end_date_parsed, source)
        
        if total1 > 0:
            data1 = [
                round(tonality1.get('negative', 0) / total1 * 100, 1),
                round(tonality1.get('neutral', 0) / total1 * 100, 1),
                round(tonality1.get('positive', 0) / total1 * 100, 1)
            ]
        else:
            data1 = [0.0, 0.0, 0.0]

        # Period 2 - аналогично
        total2 = await self._review_repo.count_by_product_and_period(session, product_ids, start_date2_parsed, end_date2_parsed, source)
        tonality2 = await self._review_repo.get_tonality_counts_by_product_and_period(session, product_ids, start_date2_parsed, end_date2_parsed, source)
        
        if total2 > 0:
            data2 = [
                round(tonality2.get('negative', 0) / total2 * 100, 1),
                round(tonality2.get('neutral', 0) / total2 * 100, 1),
                round(tonality2.get('positive', 0) / total2 * 100, 1)
            ]
        else:
            data2 = [0.0, 0.0, 0.0]

        # Changes - процентные изменения и процентные пункты
        percentage_point_changes = [data1[i] - data2[i] for i in range(3)]
        
        # Добавляем процентные изменения (relative changes)
        percentage_changes = []
        for i in range(3):
            p1_value = data1[i]
            p2_value = data2[i]
            
            # Расчет процентного изменения: (p1 - p2) / p2 * 100
            if p2_value > 0:
                percentage_change = round(((p1_value - p2_value) / p2_value * 100), 1)
            elif p1_value > 0:
                percentage_change = 100.0  # Рост от 0 до положительного значения
            elif p2_value > 0 and p1_value == 0:
                percentage_change = -100.0  # Падение от положительного до 0
            else:
                percentage_change = 0.0  # Оба значения 0
                
            percentage_changes.append(percentage_change)

        labels = ["negative", "neutral", "positive"]
        colors = ["red", "yellow", "green"]

        return {
            "period1": {
                "labels": labels, 
                "data": data1, 
                "colors": colors, 
                "total": total1
            },
            "period2": {
                "labels": labels, 
                "data": data2, 
                "colors": colors, 
                "total": total2
            },
            "changes": {
                "labels": labels, 
                "percentage_point_changes": percentage_point_changes,
                "percentage_changes": percentage_changes
            }
        }   

    async def get_tonality_stacked_bars(
        self, session: AsyncSession, product_id: int, start_date: str, end_date: str,
        start_date2: str, end_date2: str, aggregation_type: str, source: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        def parse_date(date_str: str, agg_type: str, is_start_date: bool) -> date:
            """Parse date string based on aggregation type and whether it's a start or end date."""
            try:
                if agg_type == "month":
                    year, month = map(int, date_str.split("-"))
                    if is_start_date:
                        parsed_date = datetime(year, month, 1).date()
                    else:
                        _, last_day = monthrange(year, month)
                        parsed_date = datetime(year, month, last_day).date()
                else:
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                return parsed_date
            except ValueError as e:
                raise ValueError(f"Invalid date format for {date_str}. Expected {'YYYY-MM' if agg_type == 'month' else 'YYYY-MM-DD'}") from e

        try:
            start_date_parsed = parse_date(start_date, aggregation_type, True)
            end_date_parsed = parse_date(end_date, aggregation_type, False)
            start_date2_parsed = parse_date(start_date2, aggregation_type, True)
            end_date2_parsed = parse_date(end_date2, aggregation_type, False)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": [], "period2": [], "changes": []}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        if aggregation_type not in ["month", "week", "day"]:
            raise ValueError("Invalid aggregation type. Must be 'month', 'week', or 'day'.")

        if aggregation_type == "month":
            date_trunc = "month"
            date_format = "%Y-%m"
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"

        sentiments = ['positive', 'neutral', 'negative']
        colors = {'positive': 'green', 'neutral': 'yellow', 'negative': 'red'}

        # Query for period1 - с учетом связи через review_products
        agg_date = func.date_trunc(date_trunc, Review.date).label("agg_date")
        period1_query = select(
            agg_date,
            Review.sentiment,
            func.count(func.distinct(Review.id)).label("count")
        ).join(ReviewProduct).where(
            and_(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed,
                Review.sentiment.isnot(None)
            )
        )
        if source:
            period1_query = period1_query.where(Review.source == source)
        period1_query = period1_query.group_by(agg_date, Review.sentiment).order_by(agg_date)
        period1_result = await session.execute(period1_query)
        period1_data_raw = period1_result.all()

        # Организуем данные по датам и тональностям для period1
        period1_dict = {}
        for row in period1_data_raw:
            agg_date_str = row.agg_date.strftime(date_format)
            if agg_date_str not in period1_dict:
                period1_dict[agg_date_str] = {sentiment: 0 for sentiment in sentiments}
            period1_dict[agg_date_str][row.sentiment] = row.count

        # Query for period2 - аналогично
        period2_query = select(
            agg_date,
            Review.sentiment,
            func.count(func.distinct(Review.id)).label("count")
        ).join(ReviewProduct).where(
            and_(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date2_parsed,
                Review.date <= end_date2_parsed,
                Review.sentiment.isnot(None)
            )
        )
        if source:
            period2_query = period2_query.where(Review.source == source)
        period2_query = period2_query.group_by(agg_date, Review.sentiment).order_by(agg_date)
        period2_result = await session.execute(period2_query)
        period2_data_raw = period2_result.all()

        period2_dict = {}
        for row in period2_data_raw:
            agg_date_str = row.agg_date.strftime(date_format)
            if agg_date_str not in period2_dict:
                period2_dict[agg_date_str] = {sentiment: 0 for sentiment in sentiments}
            period2_dict[agg_date_str][row.sentiment] = row.count

        # Генерируем полный диапазон дат для каждого периода
        def generate_date_range(start: date, end: date, agg_type: str) -> List[str]:
            result = []
            current = start
            if agg_type == "week":
                days_to_monday = current.weekday()
                current = current - timedelta(days=days_to_monday)
            while current <= end:
                if agg_type == "month":
                    month_str = current.strftime("%Y-%m")
                    result.append(month_str)
                    if current.month == 12:
                        current = date(current.year + 1, 1, 1)
                    else:
                        current = date(current.year, current.month + 1, 1)
                elif agg_type == "week":
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=7)
                else:
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
            return result

        period1_dates = generate_date_range(start_date_parsed, end_date_parsed, aggregation_type)
        period2_dates = generate_date_range(start_date2_parsed, end_date2_parsed, aggregation_type)

        # Форматируем данные для period1
        period1_data = []
        for date_str in period1_dates:
            tonality_data = period1_dict.get(date_str, {sentiment: 0 for sentiment in sentiments})
            tonalities = []
            for sentiment in sentiments:
                tonalities.append({
                    "sentiment": sentiment,
                    "count": tonality_data[sentiment],
                    "color": colors[sentiment]
                })
            period1_data.append({
                "date": date_str,
                "tonalities": tonalities
            })

        # Форматируем данные для period2
        period2_data = []
        for date_str in period2_dates:
            tonality_data = period2_dict.get(date_str, {sentiment: 0 for sentiment in sentiments})
            tonalities = []
            for sentiment in sentiments:
                tonalities.append({
                    "sentiment": sentiment,
                    "count": tonality_data[sentiment],
                    "color": colors[sentiment]
                })
            period2_data.append({
                "date": date_str,
                "tonalities": tonalities
            })

        # Calculate changes - сравниваем соответствующие даты
        changes = []
        min_length = min(len(period1_data), len(period2_data))
        
        for i in range(min_length):
            p1_item = period1_data[i]
            p2_item = period2_data[i]
            
            change_data = {"date": p1_item["date"], "tonalities": []}
            
            for p1_tonality, p2_tonality in zip(p1_item["tonalities"], p2_item["tonalities"]):
                # Абсолютное изменение: p1 - p2
                absolute_change = p1_tonality["count"] - p2_tonality["count"]
                
                # Процентное изменение: (p1 - p2) / p2 * 100
                if p2_tonality["count"] > 0:
                    percentage_change = round((absolute_change / p2_tonality["count"] * 100), 1)
                elif p1_tonality["count"] > 0:
                    percentage_change = 100.0  # Рост от 0 до положительного значения
                elif p2_tonality["count"] > 0 and p1_tonality["count"] == 0:
                    percentage_change = -100.0  # Падение от положительного до 0
                else:
                    percentage_change = 0.0  # Оба значения 0
                
                change_data["tonalities"].append({
                    "sentiment": p1_tonality["sentiment"],
                    "change": absolute_change,  # Абсолютное изменение
                    "change_percent": percentage_change,  # Процентное изменение
                    "color": p1_tonality["color"]
                })
            
            changes.append(change_data)

        # Добавляем оставшиеся даты, если периоды разной длины
        if len(period1_data) > min_length:
            for i in range(min_length, len(period1_data)):
                change_data = {"date": period1_data[i]["date"], "tonalities": []}
                for tonality in period1_data[i]["tonalities"]:
                    # Для дат, которые есть только в первом периоде
                    absolute_change = tonality["count"]  # p1 - 0 = p1
                    percentage_change = 100.0 if tonality["count"] > 0 else 0.0  # Рост от 0 до p1
                    
                    change_data["tonalities"].append({
                        "sentiment": tonality["sentiment"],
                        "change": absolute_change,
                        "change_percent": percentage_change,
                        "color": tonality["color"]
                    })
                changes.append(change_data)
        
        elif len(period2_data) > min_length:
            for i in range(min_length, len(period2_data)):
                change_data = {"date": period2_data[i]["date"], "tonalities": []}
                for tonality in period2_data[i]["tonalities"]:
                    # Для дат, которые есть только во втором периоде
                    absolute_change = -tonality["count"]  # 0 - p2 = -p2
                    percentage_change = -100.0 if tonality["count"] > 0 else 0.0  # Падение от p2 до 0
                    
                    change_data["tonalities"].append({
                        "sentiment": tonality["sentiment"],
                        "change": absolute_change,
                        "change_percent": percentage_change,
                        "color": tonality["color"]
                    })
                changes.append(change_data)

        return {
            "period1": period1_data,
            "period2": period2_data,
            "changes": changes
        }

    async def _get_weighted_count_by_month(self, session: AsyncSession, product_id: int, cluster_id: int, month_date: date) -> int:
        end_month = month_date + timedelta(days=31)
        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return 0
        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        statement = select(func.sum(ReviewCluster.topic_weight)).join(Review).join(ReviewProduct).where(
            and_(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= month_date,
                Review.date < end_month,
                ReviewCluster.cluster_id == cluster_id
            )
        )
        result = await session.execute(statement)
        weight = result.scalar() or 0
        return int(weight)

    async def get_reviews(
        self, session: AsyncSession, product_id: int, start_date: Optional[date] = None, end_date: Optional[date] = None,
        cluster_id: Optional[int] = None, page: int = 0, size: int = 30
    ) -> List[Dict[str, Any]]:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        logger.debug(f"Fetching reviews for product_id={product_id}, cluster_id={cluster_id}, start_date={start_date}, end_date={end_date}, page={page}, size={size}")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            logger.warning(f"Product with ID {product_id} not found")
            return []

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        # Основной запрос для получения отзывов
        statement = select(Review).join(ReviewProduct).where(ReviewProduct.product_id.in_(product_ids))

        if start_date:
            statement = statement.where(Review.date >= start_date)
        if end_date:
            statement = statement.where(Review.date <= end_date)

        if cluster_id:
            cluster = await self._cluster_repo.get_by_id(session, cluster_id)
            if not cluster:
                logger.warning(f"Cluster with ID {cluster_id} not found")
                return []
            statement = statement.join(ReviewCluster).where(ReviewCluster.cluster_id == cluster_id)
            logger.debug(f"Filtering by cluster ID: {cluster_id}")

        statement = statement.order_by(Review.date.desc()).offset(page * size).limit(size)

        result = await session.execute(statement)
        reviews = result.scalars().all()
        logger.debug(f"Retrieved {len(reviews)} reviews")

        # Получаем product_ids для каждого отзыва
        review_ids = [r.id for r in reviews]
        product_ids_query = select(ReviewProduct.review_id, ReviewProduct.product_id).where(ReviewProduct.review_id.in_(review_ids))
        product_ids_result = await session.execute(product_ids_query)
        review_product_map = {}
        for row in product_ids_result:
            review_id, prod_id = row
            if review_id not in review_product_map:
                review_product_map[review_id] = []
            review_product_map[review_id].append(prod_id)

        result = []
        for review in reviews:
            # Создаем словарь вручную, а не через from_orm
            review_dict = {
                "id": review.id,
                "text": review.text,
                "date": review.date,
                "rating": review.rating,
                "sentiment": review.sentiment,
                "sentiment_score": review.sentiment_score,
                "source": review.source,
                "created_at": review.created_at,
                "product_ids": review_product_map.get(review.id, [])  # Добавляем product_ids
            }
            result.append(review_dict)
        
        logger.debug(f"Final result: {result}")
        return result

    async def create_reviews_bulk(
        self, session: AsyncSession, reviews_data: ReviewBulkCreate
    ) -> Dict[str, Any]:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        logger.debug(f"Creating {len(reviews_data.data)} reviews")

        # Проверяем существование всех product_ids
        all_product_ids = set()
        for item in reviews_data.data:
            all_product_ids.update(item.product_ids)
        existing_products = await self._product_repo.get_all(session, size=len(all_product_ids))
        existing_product_ids = {p.id for p in existing_products}
        invalid_product_ids = all_product_ids - existing_product_ids
        if invalid_product_ids:
            raise HTTPException(status_code=400, detail=f"Invalid product IDs: {invalid_product_ids}")

        reviews = [
            Review(
                text=item.text,
                date=datetime.utcnow().date(),
                created_at=datetime.utcnow()
            ) for item in reviews_data.data
        ]

        try:
            reviews = await self._review_repo.bulk_create(session, reviews)
            for i, review in enumerate(reviews):
                await self._review_repo.add_products_to_review(session, review.id, reviews_data.data[i].product_ids)
            await session.commit()
            logger.debug(f"Successfully created {len(reviews)} reviews")
            return {"status": "success", "created_count": len(reviews)}
        except Exception as e:
            await session.rollback()
            logger.error(f"Error creating reviews: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error creating reviews: {str(e)}")

    def _get_color_for_cluster(self, cluster_id: int) -> str:
        colors = ["blue", "cyan", "pink", "purple", "green"]
        return colors[cluster_id % len(colors)]