from datetime import UTC, date, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, field_validator

TextId = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
EventType = Literal["VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"]
DeviceType = Literal["MOBILE", "DESKTOP", "TV", "TABLET"]
MIN_DATE = date(1970, 1, 1)
MAX_DATE = date(2149, 6, 6)


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    user_id: TextId
    movie_id: TextId
    event_type: EventType
    timestamp: AwareDatetime
    device_type: DeviceType
    session_id: TextId
    progress_seconds: int = Field(ge=0, le=2147483647, strict=True)

    @field_validator("timestamp", mode="before")
    @classmethod
    def require_datetime(cls, value):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        if not isinstance(value, datetime):
            raise ValueError("Передайте дату и время с часовым поясом")
        return value

    @field_validator("timestamp")
    @classmethod
    def utc_milliseconds(cls, value):
        try:
            value = value.astimezone(UTC)
        except OverflowError as exc:
            raise ValueError("Дата вне диапазона UTC") from exc
        if not MIN_DATE <= value.date() <= MAX_DATE:
            raise ValueError("Дата за пределами диапазона ClickHouse Date")
        return value.replace(microsecond=value.microsecond // 1000 * 1000)
