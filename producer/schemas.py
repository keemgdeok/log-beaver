from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class EventType(str, Enum):
    VIEW = "view"
    CLICK = "click"
    CART = "cart"
    PURCHASE = "purchase"


class UserGender(str, Enum):
    FEMALE = "F"
    MALE = "M"


class DeviceType(str, Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


class LogEvent(BaseModel):
    """ClickHouse 싱크용 Fat Log 이벤트 모델."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    timestamp: datetime
    event_type: EventType

    user_id: int
    user_gender: UserGender
    user_age_group: str = Field(min_length=2)
    device_type: DeviceType

    item_id: str = Field(min_length=1)
    item_category: str = Field(min_length=1)
    item_price: float

    session_id: str = Field(min_length=1)
    page_url: str = Field(min_length=1)
    referrer: str = Field(min_length=1)
