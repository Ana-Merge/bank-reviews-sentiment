from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ParserConfig(BaseModel):
    bank_slug: str  # gazprombank
    products: List[str]  # ["debitcards", "deposits", "credits"]
    start_date: Optional[str] = None  # "2025-01-01"
    end_date: Optional[str] = None  # "2025-09-17"
    max_pages: int = 100
    delay_between_requests: float = 1.0