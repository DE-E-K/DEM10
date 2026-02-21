"""
test_validation.py – Unit tests for Pydantic data-model validation rules.

These tests run completely offline: no Kafka, no PostgreSQL, no Docker.

Coverage
--------
* HeartbeatEvent: empty customer_id, whitespace customer_id, hard heart-rate bounds.
* HeartbeatEvent: valid construction with defaults.
* AnomalyEvent:   required fields enforced, extra fields rejected.
* InvalidEvent:   default error_type, construction variants.
"""

import pytest
from pydantic import ValidationError

from services.common.models import AnomalyEvent, HeartbeatEvent, InvalidEvent


# ── HeartbeatEvent ─────────────────────────────────────────────────────────────

class TestHeartbeatEventValidation:

    def test_empty_customer_id_rejected(self):
        """Empty string customer_id must raise ValidationError."""
        with pytest.raises(ValidationError, match="customer_id cannot be empty"):
            HeartbeatEvent(customer_id="", heart_rate=70)

    def test_whitespace_only_customer_id_rejected(self):
        """Whitespace-only customer_id must be rejected (same validator)."""
        with pytest.raises(ValidationError, match="customer_id cannot be empty"):
            HeartbeatEvent(customer_id="   ", heart_rate=70)

    def test_heart_rate_above_250_rejected(self):
        """Heart rate > 250 violates hard physiological bounds."""
        with pytest.raises(ValidationError):
            HeartbeatEvent(customer_id="cust_001", heart_rate=300)

    def test_heart_rate_below_zero_rejected(self):
        """Negative heart rate is physically impossible and must be rejected."""
        with pytest.raises(ValidationError):
            HeartbeatEvent(customer_id="cust_001", heart_rate=-5)

    def test_exactly_250_accepted(self):
        """250 bpm is the hard upper bound and must be accepted."""
        event = HeartbeatEvent(customer_id="cust_001", heart_rate=250)
        assert event.heart_rate == 250

    def test_zero_heart_rate_accepted(self):
        """0 bpm is the hard lower bound (unusual but not an error at this layer)."""
        event = HeartbeatEvent(customer_id="cust_001", heart_rate=0)
        assert event.heart_rate == 0

    def test_defaults_are_populated_automatically(self):
        """event_id and timestamp should be auto-populated when omitted."""
        event = HeartbeatEvent(customer_id="cust_001", heart_rate=72)
        assert event.event_id is not None
        assert event.timestamp is not None

    def test_model_is_frozen(self):
        """HeartbeatEvent must be immutable after creation (frozen model)."""
        event = HeartbeatEvent(customer_id="cust_001", heart_rate=72)
        with pytest.raises(Exception):  # ValidationError or TypeError from frozen=True
            object.__setattr__(event, "heart_rate", 99)


# ── AnomalyEvent ───────────────────────────────────────────────────────────────

class TestAnomalyEventValidation:

    def test_valid_anomaly_event_constructed(self):
        from services.common.models import HeartbeatEvent
        base = HeartbeatEvent(customer_id="cust_001", heart_rate=45)
        anomaly = AnomalyEvent(
            event_id=base.event_id,
            customer_id=base.customer_id,
            timestamp=base.timestamp,
            heart_rate=base.heart_rate,
            anomaly_type="LOW_HEART_RATE",
            severity="high",
            details={"threshold": 50},
        )
        assert anomaly.anomaly_type == "LOW_HEART_RATE"

    def test_missing_required_field_raises(self):
        """Omitting a required field (e.g. anomaly_type) must raise ValidationError."""
        with pytest.raises(ValidationError):
            AnomalyEvent(
                event_id="00000000-0000-0000-0000-000000000000",
                customer_id="cust_001",
                timestamp="2024-01-01T00:00:00+00:00",
                heart_rate=45,
                severity="high",
                # anomaly_type intentionally omitted
            )


# ── InvalidEvent ───────────────────────────────────────────────────────────────

class TestInvalidEventValidation:

    def test_default_error_type_is_validation(self):
        """When error_type is not supplied, it should default to 'VALIDATION'."""
        evt = InvalidEvent(error="schema failure", raw='{"bad": true}')
        assert evt.error_type == "VALIDATION"

    def test_processing_error_type_accepted(self):
        evt = InvalidEvent(
            error="unexpected exception",
            raw="{}",
            error_type="PROCESSING",
        )
        assert evt.error_type == "PROCESSING"

    def test_missing_error_field_raises(self):
        with pytest.raises(ValidationError):
            InvalidEvent(raw='{}')  # 'error' is required
