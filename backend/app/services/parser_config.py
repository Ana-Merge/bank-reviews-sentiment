from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ParserConfig(BaseModel):
    bank_slug: str
    products: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    max_pages: int = 100
    delay_between_requests: float = 1.0