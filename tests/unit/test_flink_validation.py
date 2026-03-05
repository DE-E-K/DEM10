"""
test_flink_validation.py – Unit tests for the Flink validation job logic.

Tests the validation logic extracted from the Flink ProcessFunction,
verifiable without a running Flink cluster. Covers:
* Valid event pass-through
* Soft domain bound rejection
* Malformed JSON rejection
* Missing required fields rejection
* Hard bound violation rejection
"""

import json

import pytest
from pydantic import ValidationError

from services.common.config import settings
from services.common.models import HeartbeatEvent, InvalidEvent

from datetime import datetime, timezone

_NOW = datetime.now(timezone.utc)


def _make_raw_event(customer_id: str = "cust_00001", heart_rate: int = 72) -> str:
    """Create a raw JSON string mimicking a producer message."""
    event = HeartbeatEvent(customer_id=customer_id, heart_rate=heart_rate, timestamp=_NOW)
    return json.dumps(event.model_dump(mode="json"))


def _validate(raw: str, hr_min: int = None, hr_max: int = None) -> dict:
    """
    Simulate the core validation logic from ValidateAndRouteFunction.

    Returns a dict with:
        - "result": "valid" | "invalid" | "dlq"
        - "payload": parsed dict (if valid) or error string (if invalid/dlq)
    """
    hr_min = hr_min or settings.heart_rate_min
    hr_max = hr_max or settings.heart_rate_max

    try:
        payload = json.loads(raw)
        event = HeartbeatEvent.model_validate(payload)

        if not (hr_min <= event.heart_rate <= hr_max):
            raise ValueError(
                f"heart_rate {event.heart_rate} outside domain bounds "
                f"[{hr_min}, {hr_max}]"
            )

        return {"result": "valid", "payload": payload}

    except (json.JSONDecodeError, ValidationError, ValueError) as exc:
        return {"result": "invalid", "payload": str(exc)}

    except Exception as exc:
        return {"result": "dlq", "payload": str(exc)}


# ── Valid events ────────────────────────────────────────────────────────────────

class TestValidEvents:

    def test_normal_event_passes(self):
        raw = _make_raw_event(heart_rate=72)
        result = _validate(raw)
        assert result["result"] == "valid"
        assert result["payload"]["heart_rate"] == 72

    def test_at_soft_lower_bound_passes(self):
        raw = _make_raw_event(heart_rate=settings.heart_rate_min)
        result = _validate(raw)
        assert result["result"] == "valid"

    def test_at_soft_upper_bound_passes(self):
        raw = _make_raw_event(heart_rate=settings.heart_rate_max)
        result = _validate(raw)
        assert result["result"] == "valid"


# ── Soft domain bound rejections ────────────────────────────────────────────────

class TestSoftBoundRejection:

    def test_below_soft_lower_bound_rejected(self):
        raw = _make_raw_event(heart_rate=settings.heart_rate_min - 1)
        result = _validate(raw)
        assert result["result"] == "invalid"
        assert "domain bounds" in result["payload"]

    def test_above_soft_upper_bound_rejected(self):
        raw = _make_raw_event(heart_rate=settings.heart_rate_max + 1)
        result = _validate(raw)
        assert result["result"] == "invalid"
        assert "domain bounds" in result["payload"]


# ── Schema validation failures ──────────────────────────────────────────────────

class TestSchemaRejection:

    def test_malformed_json_rejected(self):
        result = _validate("{not valid json}")
        assert result["result"] == "invalid"

    def test_missing_heart_rate_rejected(self):
        raw = json.dumps({"customer_id": "cust_001"})  # missing heart_rate
        result = _validate(raw)
        assert result["result"] == "invalid"

    def test_missing_customer_id_rejected(self):
        raw = json.dumps({"heart_rate": 72})
        result = _validate(raw)
        assert result["result"] == "invalid"

    def test_empty_customer_id_rejected(self):
        raw = json.dumps({"customer_id": "", "heart_rate": 72})
        result = _validate(raw)
        assert result["result"] == "invalid"

    def test_heart_rate_above_hard_bound_rejected(self):
        """Heart rate > 250 fails Pydantic hard bounds before soft check."""
        raw = json.dumps({"customer_id": "cust_001", "heart_rate": 300})
        result = _validate(raw)
        assert result["result"] == "invalid"

    def test_negative_heart_rate_rejected(self):
        raw = json.dumps({"customer_id": "cust_001", "heart_rate": -10})
        result = _validate(raw)
        assert result["result"] == "invalid"


# ── InvalidEvent envelope ───────────────────────────────────────────────────────

class TestInvalidEventEnvelope:

    def test_envelope_has_correct_error_type(self):
        """InvalidEvent should default to VALIDATION error_type."""
        envelope = InvalidEvent(error="test", raw="raw_data")
        assert envelope.error_type == "VALIDATION"

    def test_processing_error_type(self):
        envelope = InvalidEvent(error="unexpected", raw="data", error_type="PROCESSING")
        assert envelope.error_type == "PROCESSING"

    def test_envelope_is_json_serialisable(self):
        envelope = InvalidEvent(error="bad data", raw='{"corrupt": true}')
        serialised = json.dumps(envelope.model_dump())
        assert "bad data" in serialised
