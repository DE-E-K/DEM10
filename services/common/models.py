from datetime import datetime, timezone
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, field_validator


class HeartbeatEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    customer_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    heart_rate: int

    @field_validator("customer_id")
    @classmethod
    def validate_customer(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("customer_id cannot be empty")
        return value

    @field_validator("heart_rate")
    @classmethod
    def validate_heart_rate(cls, value: int) -> int:
        if value < 0 or value > 250:
            raise ValueError("heart_rate out of hard bounds")
        return value


class AnomalyEvent(BaseModel):
    event_id: UUID
    customer_id: str
    timestamp: datetime
    heart_rate: int
    anomaly_type: str
    severity: str
    details: dict
