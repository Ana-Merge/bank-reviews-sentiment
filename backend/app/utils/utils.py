from typing import Annotated

from fastapi import Path, Query
from pydantic import BaseModel, Field, StringConstraints

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
IdField = Annotated[int, Path(), Field(ge=1)]
PageField = Annotated[int, Query(), Field(ge=0)]
SizeField = Annotated[int, Query(), Field(ge=1, le=100)]


def update_model_by_schema(model, schema: BaseModel):
    for key, value in schema.model_dump().items():
        setattr(model, key, value)
