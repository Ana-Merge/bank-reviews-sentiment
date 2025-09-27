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
from app.models.models import ProductType, Sentiment

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

            if start_date2_parsed and end_date2_parsed:
                prev_count = await self._review_repo.count_by_product_and_period(
                    session, product_ids, start_date2_parsed, end_date2_parsed, source=source
                )
            else:
                prev_start = start_date_parsed - timedelta(days=30)
                prev_count = await self._review_repo.count_by_product_and_period(
                    session, product_ids, prev_start, start_date_parsed - timedelta(days=1), source=source
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
            interval = timedelta(days=31)
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
            interval = timedelta(days=7)
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"
            interval = timedelta(days=1)

        agg_date = func.date_trunc(date_trunc, Review.date).label("agg_date")
        period1_query = select(
            agg_date,
            Review.sentiment,
            func.count().label("count")
        ).where(
            and_(
                Review.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed,
                Review.sentiment.isnot(None)
            )
        )
        if source:
            period1_query = period1_query.where(Review.source == source)
        period1_query = period1_query.group_by(
            agg_date,
            Review.sentiment
        ).order_by(
            agg_date
        )

        period1_result = await session.execute(period1_query)
        period1_data = period1_result.all()

        period1_dict = {}
        for row in period1_data:
            agg_date_str = row.agg_date.strftime(date_format)
            if agg_date_str not in period1_dict:
                period1_dict[agg_date_str] = {"positive": 0, "neutral": 0, "negative": 0}
            period1_dict[agg_date_str][row.sentiment] = row.count

        period2_dict = {}
        if start_date2_parsed and end_date2_parsed:
            period2_query = select(
                agg_date,
                Review.sentiment,
                func.count().label("count")
            ).where(
                and_(
                    Review.product_id.in_(product_ids),
                    Review.date >= start_date2_parsed,
                    Review.date <= end_date2_parsed,
                    Review.sentiment.isnot(None)
                )
            )
            if source:
                period2_query = period2_query.where(Review.source == source)
            period2_query = period2_query.group_by(
                agg_date,
                Review.sentiment
            ).order_by(
                agg_date
            )

            period2_result = await session.execute(period2_query)
            period2_data = period2_result.all()

            for row in period2_data:
                agg_date_str = row.agg_date.strftime(date_format)
                if agg_date_str not in period2_dict:
                    period2_dict[agg_date_str] = {"positive": 0, "neutral": 0, "negative": 0}
                period2_dict[agg_date_str][row.sentiment] = row.count

        def generate_date_range(start: date, end: date, agg_type: str) -> List[str]:
            result = []
            current = start
            if agg_type == "week":
                days_to_monday = (current.weekday()) % 7
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
        period2_dates = generate_date_range(start_date2_parsed, end_date2_parsed, aggregation_type) if start_date2_parsed and end_date2_parsed else []

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
        min_length = min(len(period1), len(period2)) if period2 else 0
        for i in range(min_length):
            agg_date = period1[i]["aggregation"]
            period1_tonality = period1[i]["tonality"]
            period2_tonality = period2[i]["tonality"]

            percentage_change = {
                "positive": (
                    round(((period1_tonality["positive"] - period2_tonality["positive"]) / period2_tonality["positive"] * 100), 1)
                    if period2_tonality["positive"] > 0
                    else 100.0 if period1_tonality["positive"] > 0 else 0.0
                ),
                "neutral": (
                    round(((period1_tonality["neutral"] - period2_tonality["neutral"]) / period2_tonality["neutral"] * 100), 1)
                    if period2_tonality["neutral"] > 0
                    else 100.0 if period1_tonality["neutral"] > 0 else 0.0
                ),
                "negative": (
                    round(((period1_tonality["negative"] - period2_tonality["negative"]) / period2_tonality["negative"] * 100), 1)
                    if period2_tonality["negative"] > 0
                    else 100.0 if period1_tonality["negative"] > 0 else 0.0
                )
            }
            changes.append({
                "aggregation": agg_date,
                "percentage_change": percentage_change
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
            interval = timedelta(days=31)
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
            interval = timedelta(days=7)
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"
            interval = timedelta(days=1)

        agg_date = func.date_trunc(date_trunc, Review.date).label("agg_date")
        period1_query = select(
            agg_date,
            func.count().label("total")
        ).where(
            and_(
                Review.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed,
                Review.sentiment.isnot(None)
            )
        )
        if source:
            period1_query = period1_query.where(Review.source == source)
        period1_query = period1_query.group_by(
            agg_date
        ).order_by(
            agg_date
        )

        period1_result = await session.execute(period1_query)
        period1_data = period1_result.all()

        period1_dict = {}
        for row in period1_data:
            agg_date_str = row.agg_date.strftime(date_format)
            period1_dict[agg_date_str] = row.total

        period2_dict = {}
        if start_date2_parsed and end_date2_parsed:
            period2_query = select(
                agg_date,
                func.count().label("total")
            ).where(
                and_(
                    Review.product_id.in_(product_ids),
                    Review.date >= start_date2_parsed,
                    Review.date <= end_date2_parsed,
                    Review.sentiment.isnot(None)
                )
            )
            if source:
                period2_query = period2_query.where(Review.source == source)
            period2_query = period2_query.group_by(
                agg_date
            ).order_by(
                agg_date
            )

            period2_result = await session.execute(period2_query)
            period2_data = period2_result.all()

            for row in period2_data:
                agg_date_str = row.agg_date.strftime(date_format)
                period2_dict[agg_date_str] = row.total

        def generate_date_range(start: date, end: date, agg_type: str) -> List[str]:
            result = []
            current = start
            if agg_type == "week":
                days_to_monday = (current.weekday()) % 7
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

        period1 = [
            {
                "aggregation": date,
                "total": period1_dict.get(date, 0)
            }
            for date in period1_dates
        ]

        period2 = [
            {
                "aggregation": date,
                "total": period2_dict.get(date, 0)
            }
            for date in period2_dates
        ]

        changes = []
        min_length = min(len(period1), len(period2)) if period2 else 0
        for i in range(min_length):
            agg_date = period1[i]["aggregation"]
            period1_total = period1[i]["total"]
            period2_total = period2[i]["total"]

            percentage_change = (
                round(((period1_total - period2_total) / period2_total * 100), 1)
                if period2_total > 0
                else 100.0 if period1_total > 0 else 0.0
            )
            changes.append({
                "aggregation": agg_date,
                "percentage_change": percentage_change
            })

        return {
            "period1": period1,
            "period2": period2,
            "changes": changes
        }      
    
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
            interval = timedelta(days=31)
        elif aggregation_type == "week":
            date_trunc = "week"
            date_format = "%Y-%m-%d"
            interval = timedelta(days=7)
        else:
            date_trunc = "day"
            date_format = "%Y-%m-%d"
            interval = timedelta(days=1)

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
            func.count(Review.id).label("total")
        ).join(Review).where(
            and_(
                Review.product_id.in_(product_ids),
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
        ).order_by(
            agg_date
        )

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
                func.count(Review.id).label("total")
            ).join(Review).where(
                and_(
                    Review.product_id.in_(product_ids),
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
            ).order_by(
                agg_date
            )

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
                days_to_monday = (current.weekday()) % 7
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
        min_length = min(len(period1), len(period2)) if period2 else 0
        for i in range(min_length):
            agg_date = period1[i]["aggregation"]
            period1_clusters = period1[i]["clusters"]
            period2_clusters = period2[i]["clusters"]

            percentage_change = {}
            for cluster_name in default_clusters:
                p1_count = period1_clusters[cluster_name]
                p2_count = period2_clusters[cluster_name]
                if p1_count > 0:
                    percentage_change[cluster_name] = round(((p2_count - p1_count) / p1_count * 100), 1)
                else:
                    percentage_change[cluster_name] = 100.0 if p2_count > 0 else 0.0

            changes.append({
                "aggregation": agg_date,
                "percentage_change": percentage_change
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
                raise ValueError(f"Invalid date format for {date_str}. Expected YYYY-MM-DD or YYYY-MM") from e

        try:
            start_date_parsed = parse_date(start_date, is_start_date=True)
            end_date_parsed = parse_date(end_date, is_start_date=False)
            start_date2_parsed = parse_date(start_date2, is_start_date=True) if start_date2 else None
            end_date2_parsed = parse_date(end_date2, is_start_date=False) if end_date2 else None
        except ValueError as e:
            raise ValueError(str(e))

        if start_date_parsed > end_date_parsed:
            raise ValueError("start_date must be before or equal to end_date")
        if start_date2_parsed and end_date2_parsed and start_date2_parsed > end_date2_parsed:
            raise ValueError("start_date2 must be before or equal to end_date2")

        product = await self._product_repo.get_by_id(session, product_id)
        if not product:
            return {"period1": {"labels": [], "data": [], "colors": [], "total": 0},
                    "period2": {"labels": [], "data": [], "colors": [], "total": 0},
                    "changes": {"labels": [], "percentage_point_changes": []}}
        
        if product.type in [ProductType.CATEGORY, ProductType.SUBCATEGORY]:
            descendants = await self._product_repo.get_all_descendants(session, product_id)
            product_ids = [p.id for p in descendants] + [product_id]
        else:
            product_ids = [product_id]

        clusters = await self._cluster_repo.get_all(session)
        if not clusters:
            return {"period1": {"labels": [], "data": [], "colors": [], "total": 0},
                    "period2": {"labels": [], "data": [], "colors": [], "total": 0},
                    "changes": {"labels": [], "percentage_point_changes": []}}

        cluster_names = [c.name for c in clusters]
        cluster_ids = [c.id for c in clusters]
        colors = [self._get_color_for_cluster(c.id) for c in clusters]

        period1_total_query = select(func.count(Review.id).label("total")).where(
            and_(
                Review.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed
            )
        )
        if source:
            period1_total_query = period1_total_query.where(Review.source == source)
        period1_total_result = await session.execute(period1_total_query)
        period1_total = period1_total_result.scalar() or 0

        period1_query = select(
            ReviewCluster.cluster_id,
            func.count(Review.id).label("count")
        ).join(Review).where(
            and_(
                Review.product_id.in_(product_ids),
                Review.date >= start_date_parsed,
                Review.date <= end_date_parsed,
                ReviewCluster.cluster_id.in_(cluster_ids)
            )
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
            period2_total_query = select(func.count(Review.id).label("total")).where(
                and_(
                    Review.product_id.in_(product_ids),
                    Review.date >= start_date2_parsed,
                    Review.date <= end_date2_parsed
                )
            )
            if source:
                period2_total_query = period2_total_query.where(Review.source == source)
            period2_total_result = await session.execute(period2_total_query)
            period2_total = period2_total_result.scalar() or 0

            period2_query = select(
                ReviewCluster.cluster_id,
                func.count(Review.id).label("count")
            ).join(Review).where(
                and_(
                    Review.product_id.in_(product_ids),
                    Review.date >= start_date2_parsed,
                    Review.date <= end_date2_parsed,
                    ReviewCluster.cluster_id.in_(cluster_ids)
                )
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
                "percentage_point_changes": percentage_point_changes
            }
        }

        return result

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
            date_format = "%Y-%m"
        elif aggregation_type == "week":
            date_format = "%Y-%m-%d"
        else:
            date_format = "%Y-%m-%d"

        sentiments = ['positive', 'neutral', 'negative']
        colors = {'positive': 'green', 'neutral': 'yellow', 'negative': 'red'}

        period1_data = []
        current_date = start_date_parsed
        while current_date <= end_date_parsed:
            if aggregation_type == "month":
                start_sub = current_date
                next_month = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                end_sub = next_month - timedelta(days=1)
                current_date = next_month
            elif aggregation_type == "week":
                start_sub = current_date
                end_sub = current_date + timedelta(days=6)
                current_date += timedelta(days=7)
            else:
                start_sub = current_date
                end_sub = current_date
                current_date += timedelta(days=1)

            period_data = {"date": start_sub.strftime(date_format), "tonalities": []}
            for sentiment in sentiments:
                count = await self._review_repo.count_by_product_and_period_and_sentiment(
                    session, product_ids, start_sub, end_sub, sentiment, source
                )
                period_data["tonalities"].append({
                    "sentiment": sentiment,
                    "count": count,
                    "color": colors[sentiment]
                })
            period1_data.append(period_data)

        period2_data = []
        current_date = start_date2_parsed
        while current_date <= end_date2_parsed:
            if aggregation_type == "month":
                start_sub = current_date
                next_month = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                end_sub = next_month - timedelta(days=1)
                current_date = next_month
            elif aggregation_type == "week":
                start_sub = current_date
                end_sub = current_date + timedelta(days=6)
                current_date += timedelta(days=7)
            else:  # day
                start_sub = current_date
                end_sub = current_date
                current_date += timedelta(days=1)

            period_data = {"date": start_sub.strftime(date_format), "tonalities": []}
            for sentiment in sentiments:
                count = await self._review_repo.count_by_product_and_period_and_sentiment(
                    session, product_ids, start_sub, end_sub, sentiment, source
                )
                period_data["tonalities"].append({
                    "sentiment": sentiment,
                    "count": count,
                    "color": colors[sentiment]
                })
            period2_data.append(period_data)

        changes = []
        if period1_data and period2_data:
            min_len = min(len(period1_data), len(period2_data))
            for i in range(min_len):
                p1 = period1_data[i]
                p2 = period2_data[i]
                change_data = {"date": p1["date"], "tonalities": []}
                for c1, c2 in zip(p1["tonalities"], p2["tonalities"]):
                    change = c1["count"] - c2["count"]
                    change_data["tonalities"].append({
                        "sentiment": c1["sentiment"],
                        "change": change,
                        "color": c1["color"]
                    })
                changes.append(change_data)

        return {
            "period1": period1_data,
            "period2": period2_data,
            "changes": changes
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
            total_count = await self._review_cluster_repo.count_by_cluster_and_period(session, cluster.id, product_ids, start_date, end_date)
            logger.debug(f"Total count for cluster {cluster.name}: {total_count}")
            if total_count == 0:
                continue

            prev_count = await self._review_cluster_repo.count_by_cluster_and_period(session, cluster.id, product_ids, prev_start, start_date - timedelta(days=1))
            change_percent = ((total_count - prev_count) / prev_count * 100) if prev_count > 0 else 0.0
            logger.debug(f"Previous count: {prev_count}, Change percent: {change_percent}")

            effective_sentiment = func.coalesce(ReviewCluster.sentiment_contribution, Review.sentiment).label("effective_sentiment")
            statement = select(
                effective_sentiment,
                func.sum(ReviewCluster.topic_weight).label("weighted_count")
            ).join(Review).where(
                and_(
                    Review.product_id.in_(product_ids),
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
                    tonality[sentiment] = row.weighted_count or 0.0
                else:
                    logger.debug(f"Found null effective_sentiment for cluster {cluster.name}")

            total_tonality = sum(tonality.values())
            logger.debug(f"Tonality for cluster {cluster.name}: {tonality}, Total: {total_tonality}")

            data = [
                {"label": "Негатив", "percent": round(tonality[Sentiment.NEGATIVE] / total_tonality * 100, 1) if total_tonality > 0 else 0.0, "color": "orange"},
                {"label": "Нейтрал", "percent": round(tonality[Sentiment.NEUTRAL] / total_tonality * 100, 1) if total_tonality > 0 else 0.0, "color": "cyan"},
                {"label": "Позитив", "percent": round(tonality[Sentiment.POSITIVE] / total_tonality * 100, 1) if total_tonality > 0 else 0.0, "color": "blue"}
            ]

            result.append({
                "title": cluster.name,
                "reviews_count": int(total_count),
                "change_percent": int(change_percent),
                "data": data
            })

        logger.debug(f"Final result: {result}")
        return result

    

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

        # Period 1
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

        # Period 2
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

        # Changes
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

        statement = select(func.sum(ReviewCluster.topic_weight)).join(Review).where(
            and_(
                Review.product_id.in_(product_ids),
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
            logger.debug(f"Product IDs (including descendants): {product_ids}")
        else:
            product_ids = [product_id]
            logger.debug(f"Product ID: {product_id}")

        statement = select(Review).where(Review.product_id.in_(product_ids))

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

        result = [ReviewResponse.from_orm(review).dict() for review in reviews]
        logger.debug(f"Final result: {result}")
        return result
    

    async def create_reviews_bulk(
        self, session: AsyncSession, reviews_data: ReviewBulkCreate
    ) -> Dict[str, Any]:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        logger.debug(f"Creating {len(reviews_data.data)} reviews")

        reviews = [
            Review(
                text=item.text,
                date=datetime.utcnow().date(),
                product_id=None,
                created_at=datetime.utcnow()
            ) for item in reviews_data.data
        ]

        try:
            await self._review_repo.bulk_create(session, reviews)
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