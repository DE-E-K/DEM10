"""
models.py – Shared Pydantic data-models used across the pipeline.

Design decisions
----------------
* All models are **frozen** (immutable after creation) so they can safely be
  passed between threads and used as dict keys / set members.
* Field validators enforce business rules at the point of data entry rather
  than scattering checks throughout consumers and detectors.
* ``InvalidEvent`` gives the invalid / DLQ topics a typed schema so downstream
  consumers can parse and act on error payloads predictably.
"""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


# Core domain event
# =======================================================================

class HeartbeatEvent(BaseModel):
    """
    A single heart-rate reading produced by the synthetic sensor simulator.

    Fields
    ------
    event_id    : Globally unique identifier for this specific reading.
                  Auto-generated (UUID v4) if not supplied.
    customer_id : Identifier of the customer the reading belongs to.
                  Must be non-empty after stripping whitespace.
    timestamp   : UTC wall-clock time the reading was captured.
                  Defaults to *now* when the event is constructed.
    heart_rate  : Beats per minute. Hard physiological bounds: [0, 250].
                  Soft domain bounds are enforced by the consumer via settings.
    """

    model_config = ConfigDict(frozen=True)  # Immutable after construction

    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique event identifier (UUID v4).",
    )
    customer_id: str = Field(
        description="Customer identifier. Format: 'cust_NNNNN'.",
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of the heart-rate reading.",
    )
    heart_rate: int = Field(
        description="Heart rate in beats per minute (bpm).",
    )

    @field_validator("customer_id")
    @classmethod
    def validate_customer_id(cls, value: str) -> str:
        """Reject blank or whitespace-only customer identifiers."""
        stripped = value.strip()
        if not stripped:
            raise ValueError("customer_id cannot be empty or whitespace")
        return stripped

    @field_validator("heart_rate")
    @classmethod
    def validate_heart_rate_hard_bounds(cls, value: int) -> int:
        """
        Enforce absolute physiological hard bounds [0, 250] bpm.

        Note: soft domain bounds (e.g. 45–185) are applied by the consumer
        using ``settings.heart_rate_min`` / ``settings.heart_rate_max``.
        """
        if not (0 <= value <= 250):
            raise ValueError(
                f"heart_rate {value} is outside hard physiological bounds [0, 250]"
            )
        return value


# Anomaly event (written to DB + events.anomaly.v1 topic)
# =======================================================================

class AnomalyEvent(BaseModel):
    """
    Represents an anomalous heart-rate reading flagged by the detector.

    Fields
    ------
    event_id     : References the originating ``HeartbeatEvent.event_id``.
    customer_id  : Customer the anomaly belongs to.
    timestamp    : UTC time of the original reading.
    heart_rate   : The bpm value that triggered the anomaly.
    anomaly_type : One of ``LOW_HEART_RATE``, ``HIGH_HEART_RATE``, ``SPIKE``.
    severity     : ``high`` (absolute threshold breach) or ``medium`` (spike).
    details      : Free-form context dict, e.g. ``{"delta": 35, "threshold": 30}``.
    """

    model_config = ConfigDict(frozen=True)

    event_id: UUID = Field(description="UUID of the originating heartbeat event.")
    customer_id: str = Field(description="Customer identifier.")
    timestamp: datetime = Field(description="UTC timestamp of the original reading.")
    heart_rate: int = Field(description="Heart rate (bpm) that triggered the anomaly.")
    anomaly_type: str = Field(description="Anomaly classification label.")
    severity: str = Field(description="Severity tier: 'high' or 'medium'.")
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Contextual metadata about why this anomaly was raised.",
    )


# Invalid / DLQ envelope
# =======================================================================

class InvalidEvent(BaseModel):
    """
    Envelope written to ``events.invalid.v1`` and ``events.dlq.v1`` topics.

    Using a typed model (rather than ad-hoc dicts) means downstream consumers
    that monitor these quarantine topics can parse them reliably.

    Fields
    ------
    error       : Human-readable description of why the event was rejected.
    raw         : The original raw message payload as a string for debugging.
    error_type  : ``VALIDATION`` (schema/domain issue) or ``PROCESSING`` (unexpected error).
    """

    model_config = ConfigDict(frozen=True)

    error: str = Field(description="Error message explaining the rejection reason.")
    raw: str = Field(description="Original raw Kafka message value (UTF-8 string).")
    error_type: str = Field(
        default="VALIDATION",
        description="'VALIDATION' for schema/domain errors, 'PROCESSING' for unexpected failures.",
    )
