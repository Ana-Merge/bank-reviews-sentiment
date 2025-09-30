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
    ClusterRepository, ReviewClusterRepository, ReviewCluster, Review, ReviewsForModelRepository
)
from app.models.user_models import User
from app.schemas.schemas import ReviewResponse, ClusterResponse, ReviewBulkCreate
from app.models.models import ProductType, Sentiment, ReviewProduct, ReviewsForModel

class StatsService:
    def __init__(
        self,
        product_repo: ProductRepository,
        review_repo: ReviewRepository,
        monthly_stats_repo: MonthlyStatsRepository,
        cluster_stats_repo: ClusterStatsRepository,
        cluster_repo: ClusterRepository,
        review_cluster_repo: ReviewClusterRepository,
        reviews_for_model_repo: ReviewsForModelRepository,
    ):
        self._product_repo = product_repo
        self._review_repo = review_repo
        self._monthly_stats_repo = monthly_stats_repo
        self._cluster_stats_repo = cluster_stats_repo
        self._cluster_repo = cluster_repo
        self._review_cluster_repo = review_cluster_repo
        self._reviews_for_model_repo = reviews_for_model_repo

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
                raise ValueError(f"Неправильный формат {date_str}. Ожидается YYYY-MM-DD") from e

        try:
            start_date_parsed = parse_date(start_date)
            end_date_parsed = parse_date(end_date)
            start_date2_parsed = parse_date(start_date2)
            end_date2_parsed = parse_date(end_date2)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date должна быть до или равна end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 должна быть до или равна end_date2")

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
                raise ValueError(f"Неправильный формат {date_str}. Ожидается {'YYYY-MM' if agg_type == 'month' else 'YYYY-MM-DD'}") from e

        try:
            start_date_parsed = parse_date(start_date, aggregation_type, is_start_date=True)
            end_date_parsed = parse_date(end_date, aggregation_type, is_start_date=False)
            start_date2_parsed = parse_date(start_date2, aggregation_type, is_start_date=True)
            end_date2_parsed = parse_date(end_date2, aggregation_type, is_start_date=False)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date должна быть до или равна end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 должна быть до или равна end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": [], "period2": [], "changes": []}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        if aggregation_type not in ["month", "week", "day"]:
            raise ValueError("Неправильный aggregation type. Должно быть 'month', 'week', или 'day'.")

        if aggregation_type == "month":
            date_trunc = "month"
            date_format = "%Y-%m"
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"

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
                else:
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
            return result

        period1_dates = generate_date_range(start_date_parsed, end_date_parsed, aggregation_type)
        period2_dates = generate_date_range(start_date2_parsed, end_date2_parsed, aggregation_type)

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

        changes = []
        min_length = min(len(period1), len(period2))
        
        for i in range(min_length):
            period1_item = period1[i]
            period2_item = period2[i]
            
            period1_tonality = period1_item["tonality"]
            period2_tonality = period2_item["tonality"]
            
            def safe_percentage_change(current, previous):
                if previous > 0:
                    return round(((current - previous) / previous * 100), 1)
                elif current > 0:
                    return 100.0 
                elif previous > 0 and current == 0:
                    return -100.0 
                else:
                    return 0.0

            percentage_change = {
                "positive": safe_percentage_change(period1_tonality["positive"], period2_tonality["positive"]),
                "neutral": safe_percentage_change(period1_tonality["neutral"], period2_tonality["neutral"]),
                "negative": safe_percentage_change(period1_tonality["negative"], period2_tonality["negative"])
            }
            
            changes.append({
                "aggregation": period1_item["aggregation"],
                "percentage_change": percentage_change
            })

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
                raise ValueError(f"Неправильный формат даты {date_str}. Ожидается {'YYYY-MM' if agg_type == 'month' else 'YYYY-MM-DD'}") from e

        try:
            start_date_parsed = parse_date(start_date, aggregation_type, is_start_date=True)
            end_date_parsed = parse_date(end_date, aggregation_type, is_start_date=False)
            start_date2_parsed = parse_date(start_date2, aggregation_type, is_start_date=True)
            end_date2_parsed = parse_date(end_date2, aggregation_type, is_start_date=False)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date должна быть до или равна end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 должна быть до или равна end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": [], "period2": [], "changes": []}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        if aggregation_type not in ["month", "week", "day"]:
            raise ValueError("Неправильный aggregation type. Должно быть 'month', 'week', или 'day'.")

        if aggregation_type == "month":
            date_trunc = "month"
            date_format = "%Y-%m"
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"

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
                else:
                    result.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
            return result

        period1_dates = generate_date_range(start_date_parsed, end_date_parsed, aggregation_type)
        period2_dates = generate_date_range(start_date2_parsed, end_date2_parsed, aggregation_type)

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

        changes = []
        min_length = min(len(period1), len(period2))
        
        for i in range(min_length):
            period1_count = period1[i]["count"]
            period2_count = period2[i]["count"]

            if period2_count > 0:
                change_percent = round(((period1_count - period2_count) / period2_count * 100), 1)
            elif period1_count > 0:
                change_percent = 100.0
            elif period2_count > 0 and period1_count == 0:
                change_percent = -100.0
            else:
                change_percent = 0.0
            
            changes.append({
                "aggregation": period1[i]["aggregation"],
                "change_percent": change_percent
            })

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

        def parse_date(date_str: str, is_start_date: bool) -> datetime.date:
            """Parse date string in YYYY-MM-DD or YYYY-MM format."""
            try:
                if len(date_str.split("-")) == 2:
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
                raise ValueError(f"Неправильный формат даты {date_str}. Ожидается YYYY-MM-DD или YYYY-MM") from e

        try:
            start_date_parsed = parse_date(start_date, is_start_date=True)
            end_date_parsed = parse_date(end_date, is_start_date=False)
            start_date2_parsed = parse_date(start_date2, is_start_date=True) if start_date2 else None
            end_date2_parsed = parse_date(end_date2, is_start_date=False) if end_date2 else None
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date должна быть до или равна end_date")
        if start_date2_parsed and end_date2_parsed and start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 должна быть до или равна end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {
                "period1": {"labels": [], "data": [], "colors": [], "total": 0},
                "period2": {"labels": [], "data": [], "colors": [], "total": 0},
                "changes": {"labels": [], "percentage_point_changes": [], "relative_percentage_changes": []}
            }
        
        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        clusters = await self._cluster_repo.get_all(session)
        if not clusters:
            return {
                "period1": {"labels": [], "data": [], "colors": [], "total": 0},
                "period2": {"labels": [], "data": [], "colors": [], "total": 0},
                "changes": {"labels": [], "percentage_point_changes": [], "relative_percentage_changes": []}
            }

        cluster_names = [c.name for c in clusters]
        cluster_ids = [c.id for c in clusters]
        colors = [self._get_color_for_cluster(c.id) for c in clusters]

        period1_total_query = select(func.count(func.distinct(Review.id)).label("total")) \
            .join(ReviewProduct, ReviewProduct.review_id == Review.id) \
            .where(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed
            )
        if source:
            period1_total_query = period1_total_query.where(Review.source == source)
        period1_total_result = await session.execute(period1_total_query)
        period1_total = period1_total_result.scalar() or 0

        period1_query = select(
            ReviewCluster.cluster_id,
            func.count(func.distinct(Review.id)).label("count")
        ).join(Review, ReviewCluster.review_id == Review.id) \
        .join(ReviewProduct, ReviewProduct.review_id == Review.id) \
        .where(
            ReviewProduct.product_id.in_(product_ids),
            Review.date >= start_date_parsed,
            Review.date <= end_date_parsed,
            ReviewCluster.cluster_id.in_(cluster_ids)
        )
        if source:
            period1_query = period1_query.where(Review.source == source)
        period1_query = period1_query.group_by(ReviewCluster.cluster_id)
        period1_result = await session.execute(period1_query)
        period1_data = period1_result.all()

        period1_counts = {c.id: 0 for c in clusters}
        for row in period1_data:
            period1_counts[row.cluster_id] = row.count
        period1_percentages = [
            round((period1_counts[c.id] / period1_total * 100), 1) if period1_total > 0 else 0.0
            for c in clusters
        ]

        period2_total = 0
        period2_percentages = [0.0] * len(clusters)
        if start_date2_parsed and end_date2_parsed:
            period2_total_query = select(func.count(func.distinct(Review.id)).label("total")) \
                .join(ReviewProduct, ReviewProduct.review_id == Review.id) \
                .where(
                    ReviewProduct.product_id.in_(product_ids),
                    Review.date >= start_date2_parsed,
                    Review.date <= end_date2_parsed
                )
            if source:
                period2_total_query = period2_total_query.where(Review.source == source)
            period2_total_result = await session.execute(period2_total_query)
            period2_total = period2_total_result.scalar() or 0

            period2_query = select(
                ReviewCluster.cluster_id,
                func.count(func.distinct(Review.id)).label("count")
            ).join(Review, ReviewCluster.review_id == Review.id) \
            .join(ReviewProduct, ReviewProduct.review_id == Review.id) \
            .where(
                ReviewProduct.product_id.in_(product_ids),
                Review.date >= start_date2_parsed,
                Review.date <= end_date2_parsed,
                ReviewCluster.cluster_id.in_(cluster_ids)
            )
            if source:
                period2_query = period2_query.where(Review.source == source)
            period2_query = period2_query.group_by(ReviewCluster.cluster_id)
            period2_result = await session.execute(period2_query)
            period2_data = period2_result.all()

            period2_counts = {c.id: 0 for c in clusters}
            for row in period2_data:
                period2_counts[row.cluster_id] = row.count
            period2_percentages = [
                round((period2_counts[c.id] / period2_total * 100), 1) if period2_total > 0 else 0.0
                for c in clusters
            ]

        percentage_point_changes = [
            round(period2_percentages[i] - period1_percentages[i], 1)
            for i in range(len(clusters))
        ]

        relative_percentage_changes = []
        for i in range(len(clusters)):
            p1 = period1_percentages[i]
            p2 = period2_percentages[i]
            if p1 > 0:
                change = round(((p2 - p1) / p1) * 100, 1)
            elif p2 > 0:
                change = 100.0
            else:
                change = 0.0
            relative_percentage_changes.append(change)

        result = {
            "period1": {
                "labels": cluster_names,
                "data": period1_percentages,
                "colors": colors,
                "total": int(period1_total)
            },
            "period2": {
                "labels": cluster_names,
                "data": period2_percentages,
                "colors": colors,
                "total": int(period2_total)
            },
            "changes": {
                "labels": cluster_names,
                "percentage_point_changes": percentage_point_changes,
                "relative_percentage_changes": relative_percentage_changes
            }
        }

        return result

    async def get_change_chart(
        self, session: AsyncSession, product_id: int, start_date: str, end_date: str,
        start_date2: str, end_date2: str, source: Optional[str] = None
    ) -> Dict[str, Any]:
        def parse_date(date_str: str) -> date:
            """Парсинг даты в формат YYYY-MM-DD"""
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError as e:
                raise ValueError(f"Неправильный формат даты {date_str}. Ожидается YYYY-MM-DD") from e

        try:
            start_date_parsed = parse_date(start_date)
            end_date_parsed = parse_date(end_date)
            start_date2_parsed = parse_date(start_date2)
            end_date2_parsed = parse_date(end_date2)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date должна быть до или равна end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 должна быть до или равна end_date2")

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
            """Парсинг даты с агрегацией."""
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
                raise ValueError(f"Неправильный формат даты {date_str}. Ожидается {'YYYY-MM' if agg_type == 'month' else 'YYYY-MM-DD'}") from e

        try:
            start_date_parsed = parse_date(start_date, aggregation_type, is_start_date=True)
            end_date_parsed = parse_date(end_date, aggregation_type, is_start_date=False)
            start_date2_parsed = parse_date(start_date2, aggregation_type, is_start_date=True) if start_date2 else None
            end_date2_parsed = parse_date(end_date2, aggregation_type, is_start_date=False) if end_date2 else None
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date должна быть до или равна end_date")
        if start_date2_parsed and end_date2_parsed and start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 должна быть до или равна end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": [], "period2": [], "changes": []}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        if aggregation_type not in ["month", "week", "day"]:
            raise ValueError("Неправильный aggregation type. Должно быть 'month', 'week', или 'day'.")

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

        agg_date = func.date_trunc(date_trunc, Review.date).label("agg_date")
        period1_query = select(
            agg_date,
            ReviewCluster.cluster_id,
            func.count(func.distinct(Review.id)).label("total")
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

        changes = []
        min_length = min(len(period1), len(period2))
        
        for i in range(min_length):
            period1_item = period1[i]
            period2_item = period2[i]
            
            period1_clusters = period1_item["clusters"]
            period2_clusters = period2_item["clusters"]

            percentage_change = {}
            for cluster_name in default_clusters:
                p1_count = period1_clusters[cluster_name]
                p2_count = period2_clusters[cluster_name]
                
                if p2_count > 0:
                    change_percent = round(((p1_count - p2_count) / p2_count * 100), 1)
                elif p1_count > 0:
                    change_percent = 100.0
                elif p2_count > 0 and p1_count == 0:
                    change_percent = -100.0
                else:
                    change_percent = 0.0 
                    
                percentage_change[cluster_name] = change_percent

            changes.append({
                "aggregation": period1_item["aggregation"],
                "percentage_change": percentage_change
            })

        if len(period1) > min_length:
            for i in range(min_length, len(period1)):
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
                raise ValueError(f"Неправильный формат даты {date_str}. Ожидается YYYY-MM-DD") from e

        try:
            start_date_parsed = parse_date(start_date)
            end_date_parsed = parse_date(end_date)
            start_date2_parsed = parse_date(start_date2)
            end_date2_parsed = parse_date(end_date2)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date должна быть до или равна end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 должна быть до или равна end_date2")

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
        
        percentage_changes = []
        for i in range(3):
            p1_value = data1[i]
            p2_value = data2[i]

            if p2_value > 0:
                percentage_change = round(((p1_value - p2_value) / p2_value * 100), 1)
            elif p1_value > 0:
                percentage_change = 100.0 
            elif p2_value > 0 and p1_value == 0:
                percentage_change = -100.0 
            else:
                percentage_change = 0.0 
                
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
            """Парсинг даты с агрегацией."""
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
                raise ValueError(f"Неправильный форммат даты {date_str}. Ожидается {'YYYY-MM' if agg_type == 'month' else 'YYYY-MM-DD'}") from e

        try:
            start_date_parsed = parse_date(start_date, aggregation_type, True)
            end_date_parsed = parse_date(end_date, aggregation_type, False)
            start_date2_parsed = parse_date(start_date2, aggregation_type, True)
            end_date2_parsed = parse_date(end_date2, aggregation_type, False)
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date должна быть до или равна end_date")
        if start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 должна быть до или равна end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": [], "period2": [], "changes": []}

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        if aggregation_type not in ["month", "week", "day"]:
            raise ValueError("Неправильный aggregation type. Должно быть 'month', 'week', или 'day'.")

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

        period1_dict = {}
        for row in period1_data_raw:
            agg_date_str = row.agg_date.strftime(date_format)
            if agg_date_str not in period1_dict:
                period1_dict[agg_date_str] = {sentiment: 0 for sentiment in sentiments}
            period1_dict[agg_date_str][row.sentiment] = row.count

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

        changes = []
        min_length = min(len(period1_data), len(period2_data))
        
        for i in range(min_length):
            p1_item = period1_data[i]
            p2_item = period2_data[i]
            
            change_data = {"date": p1_item["date"], "tonalities": []}
            
            for p1_tonality, p2_tonality in zip(p1_item["tonalities"], p2_item["tonalities"]):
                absolute_change = p1_tonality["count"] - p2_tonality["count"]
                
                if p2_tonality["count"] > 0:
                    percentage_change = round((absolute_change / p2_tonality["count"] * 100), 1)
                elif p1_tonality["count"] > 0:
                    percentage_change = 100.0
                elif p2_tonality["count"] > 0 and p1_tonality["count"] == 0:
                    percentage_change = -100.0 
                else:
                    percentage_change = 0.0 
                
                change_data["tonalities"].append({
                    "sentiment": p1_tonality["sentiment"],
                    "change": absolute_change,
                    "change_percent": percentage_change, 
                    "color": p1_tonality["color"]
                })
            
            changes.append(change_data)

        if len(period1_data) > min_length:
            for i in range(min_length, len(period1_data)):
                change_data = {"date": period1_data[i]["date"], "tonalities": []}
                for tonality in period1_data[i]["tonalities"]:
                    absolute_change = tonality["count"]
                    percentage_change = 100.0 if tonality["count"] > 0 else 0.0 
                    
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
                    absolute_change = -tonality["count"] 
                    percentage_change = -100.0 if tonality["count"] > 0 else 0.0 
                    
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
        self, session: AsyncSession, product_id: int, start_date: Optional[date] = None, 
        end_date: Optional[date] = None, cluster_id: Optional[int] = None, 
        source: Optional[str] = None, order_by: str = "desc", page: int = 0, size: int = 30
    ) -> List[Dict[str, Any]]:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        logger.debug(f"Fetching reviews for product_id={product_id}, cluster_id={cluster_id}, source={source}, start_date={start_date}, end_date={end_date}, order_by={order_by}, page={page}, size={size}")

        # Валидация параметра order_by
        if order_by not in ["asc", "desc"]:
            raise ValueError("order_by должен быть 'asc' или 'desc'")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            logger.warning(f"Product with ID {product_id} not found")
            return []

        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        statement = select(Review).join(ReviewProduct).where(ReviewProduct.product_id.in_(product_ids))

        # Фильтр по дате
        if start_date:
            statement = statement.where(Review.date >= start_date)
        if end_date:
            statement = statement.where(Review.date <= end_date)

        # Фильтр по источнику
        if source:
            statement = statement.where(Review.source == source)
            logger.debug(f"Filtering by source: {source}")

        # Фильтр по кластеру
        if cluster_id:
            cluster = await self._cluster_repo.get_by_id(session, cluster_id)
            if not cluster:
                logger.warning(f"Cluster with ID {cluster_id} not found")
                return []
            statement = statement.join(ReviewCluster).where(ReviewCluster.cluster_id == cluster_id)
            logger.debug(f"Filtering by cluster ID: {cluster_id}")

        # Сортировка по дате
        if order_by == "asc":
            statement = statement.order_by(Review.date.asc())
        else:
            statement = statement.order_by(Review.date.desc())
        
        logger.debug(f"Sorting by date: {order_by}")

        # Пагинация
        statement = statement.offset(page * size).limit(size)

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

        # Формируем результат
        result = []
        for review in reviews:
            review_dict = {
                "id": review.id,
                "text": review.text,
                "date": review.date,
                "rating": review.rating,
                "sentiment": review.sentiment,
                "sentiment_score": review.sentiment_score,
                "source": review.source,
                "created_at": review.created_at,
                "product_ids": review_product_map.get(review.id, [])
            }
            result.append(review_dict)
        
        logger.debug(f"Final result: {len(result)} reviews")
        return result

    async def create_reviews_bulk(
        self, session: AsyncSession, reviews_data: ReviewBulkCreate
    ) -> Dict[str, Any]:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        logger.debug(f"Creating {len(reviews_data.data)} reviews for model processing")

        current_time = datetime.utcnow()
        
        reviews_for_model = [
            ReviewsForModel(
                review_text=item.text,  # Используем review_text вместо text
                # Обязательные поля для ReviewsForModel
                bank_name="manual_input",
                bank_slug="manual",
                product_name="general",
                # Опциональные поля
                review_theme="",
                rating="",
                verification_status="",
                review_date=current_time.strftime("%Y-%m-%d"),
                review_timestamp=current_time,
                source_url="",
                parsed_at=current_time,
                processed=False,
                # Убираем поле text, так как его нет в модели
            ) for item in reviews_data.data
        ]

        try:
            reviews_for_model = await self._reviews_for_model_repo.bulk_create(session, reviews_for_model)
            
            await session.commit()
            logger.debug(f"Successfully created {len(reviews_for_model)} reviews in reviews_for_model table")
            
            return {
                "status": "success", 
                "created_count": len(reviews_for_model),
                "message": "Reviews saved for model processing. They will be added to main reviews table after model analysis."
            }
        except Exception as e:
            await session.rollback()
            logger.error(f"Error creating reviews for model: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error creating reviews for model processing: {str(e)}")

        
    
    async def get_unprocessed_reviews_for_model(
        self, session: AsyncSession, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Получить необработанные отзывы для нейронной модели
        """
        unprocessed_reviews = await self._reviews_for_model_repo.get_unprocessed(session, limit)
        
        return [
            {
                "id": review.id,
                "text": review.text,
                "created_at": review.created_at
            }
            for review in unprocessed_reviews
        ]

    def _get_color_for_cluster(self, cluster_id: int) -> str:
        colors = ["blue", "cyan", "pink", "purple", "green"]
        return colors[cluster_id % len(colors)]