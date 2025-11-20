from pydantic import BaseModel
from typing import Any, Optional


class KVEntry(BaseModel):
    key: str
    value: Any
    success: Optional[bool] = True
