"""
test_flink_anomaly_logic.py – Unit tests for the anomaly detection logic
used inside the Flink AnomalyDetectFunction.

Tests the rule evaluation logic that mirrors anomaly_rules.py but is
embedded in the Flink KeyedProcessFunction. Verifiable without Flink.
"""

import json

import pytest

from services.common.config import settings
from services.common.models import HeartbeatEvent, AnomalyEvent

from datetime import datetime, timezone

_NOW = datetime.now(timezone.utc)


def _make_event(heart_rate: int, customer_id: str = "cust_00001") -> HeartbeatEvent:
    return HeartbeatEvent(customer_id=customer_id, heart_rate=heart_rate, timestamp=_NOW)


def _evaluate_rules(
    event: HeartbeatEvent,
    history: list[int],
    low_threshold: int = None,
    high_threshold: int = None,
    spike_delta: int = None,
) -> AnomalyEvent | None:
    """
    Pure function replicating the rule logic from AnomalyDetectFunction._evaluate_rules.
    """
    low_threshold = low_threshold or settings.anomaly_low_threshold
    high_threshold = high_threshold or settings.anomaly_high_threshold
    spike_delta = spike_delta or settings.anomaly_spike_delta

    rate = event.heart_rate

    if rate <= low_threshold:
        return AnomalyEvent(
            event_id=event.event_id,
            customer_id=event.customer_id,
            timestamp=event.timestamp,
            heart_rate=rate,
            anomaly_type="LOW_HEART_RATE",
            severity="high",
            details={"threshold": low_threshold, "measured": rate},
        )

    if rate >= high_threshold:
        return AnomalyEvent(
            event_id=event.event_id,
            customer_id=event.customer_id,
            timestamp=event.timestamp,
            heart_rate=rate,
            anomaly_type="HIGH_HEART_RATE",
            severity="high",
            details={"threshold": high_threshold, "measured": rate},
        )

    if history:
        previous = history[-1]
        delta = abs(rate - previous)
        if delta >= spike_delta:
            return AnomalyEvent(
                event_id=event.event_id,
                customer_id=event.customer_id,
                timestamp=event.timestamp,
                heart_rate=rate,
                anomaly_type="SPIKE",
                severity="medium",
                details={
                    "delta": delta,
                    "threshold": spike_delta,
                    "previous": previous,
                    "measured": rate,
                },
            )

    return None


# ── LOW_HEART_RATE ──────────────────────────────────────────────────────────────

class TestFlinkLowHeartRate:

    def test_at_low_threshold_fires(self):
        event = _make_event(50)
        anomaly = _evaluate_rules(event, [])
        assert anomaly is not None
        assert anomaly.anomaly_type == "LOW_HEART_RATE"
        assert anomaly.severity == "high"

    def test_below_low_threshold_fires(self):
        event = _make_event(30)
        anomaly = _evaluate_rules(event, [])
        assert anomaly is not None
        assert anomaly.anomaly_type == "LOW_HEART_RATE"


# ── HIGH_HEART_RATE ─────────────────────────────────────────────────────────────

class TestFlinkHighHeartRate:

    def test_at_high_threshold_fires(self):
        event = _make_event(140)
        anomaly = _evaluate_rules(event, [])
        assert anomaly is not None
        assert anomaly.anomaly_type == "HIGH_HEART_RATE"
        assert anomaly.severity == "high"

    def test_above_high_threshold_fires(self):
        event = _make_event(200)
        anomaly = _evaluate_rules(event, [])
        assert anomaly is not None
        assert anomaly.anomaly_type == "HIGH_HEART_RATE"


# ── SPIKE ───────────────────────────────────────────────────────────────────────

class TestFlinkSpike:

    def test_spike_upward_fires(self):
        event = _make_event(100)
        anomaly = _evaluate_rules(event, [65])  # delta=35 >= 30
        assert anomaly is not None
        assert anomaly.anomaly_type == "SPIKE"
        assert anomaly.severity == "medium"

    def test_spike_downward_fires(self):
        event = _make_event(65)
        anomaly = _evaluate_rules(event, [100])  # delta=35 >= 30
        assert anomaly is not None
        assert anomaly.anomaly_type == "SPIKE"

    def test_no_spike_when_delta_below_threshold(self):
        event = _make_event(80)
        anomaly = _evaluate_rules(event, [70])  # delta=10 < 30
        assert anomaly is None

    def test_no_spike_on_empty_history(self):
        event = _make_event(80)
        anomaly = _evaluate_rules(event, [])
        assert anomaly is None

    def test_spike_uses_last_entry_only(self):
        """SPIKE should compare to the most recent value (last in list)."""
        event = _make_event(100)
        anomaly = _evaluate_rules(event, [60, 65, 95])  # delta from 95 = 5
        assert anomaly is None


# ── Normal events ───────────────────────────────────────────────────────────────

class TestFlinkNormalEvent:

    def test_normal_rate_no_anomaly(self):
        event = _make_event(72)
        anomaly = _evaluate_rules(event, [70, 71, 73])
        assert anomaly is None

    def test_just_above_low_threshold_no_anomaly(self):
        event = _make_event(51)
        anomaly = _evaluate_rules(event, [52, 53])
        assert anomaly is None

    def test_just_below_high_threshold_no_anomaly(self):
        event = _make_event(139)
        anomaly = _evaluate_rules(event, [135, 137])
        assert anomaly is None


# ── Rule priority ───────────────────────────────────────────────────────────────

class TestFlinkRulePriority:

    def test_low_takes_priority_over_spike(self):
        """If rate is below low threshold AND has a spike, LOW wins."""
        event = _make_event(40)  # below 50 AND delta from 80 = 40
        anomaly = _evaluate_rules(event, [80])
        assert anomaly.anomaly_type == "LOW_HEART_RATE"

    def test_high_takes_priority_over_spike(self):
        """If rate is above high threshold AND has a spike, HIGH wins."""
        event = _make_event(160)  # above 140 AND delta from 80 = 80
        anomaly = _evaluate_rules(event, [80])
        assert anomaly.anomaly_type == "HIGH_HEART_RATE"


# ── Anomaly serialisation ──────────────────────────────────────────────────────

class TestAnomalySerialization:

    def test_anomaly_to_json_roundtrip(self):
        """AnomalyEvent should survive JSON serialisation/deserialisation."""
        event = _make_event(45)
        anomaly = _evaluate_rules(event, [])
        json_str = json.dumps(anomaly.model_dump(mode="json"))
        parsed = json.loads(json_str)
        assert parsed["anomaly_type"] == "LOW_HEART_RATE"
        assert parsed["severity"] == "high"
        assert parsed["heart_rate"] == 45
