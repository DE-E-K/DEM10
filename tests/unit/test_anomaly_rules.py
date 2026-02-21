"""
test_anomaly_rules.py – Unit tests for the AnomalyRules detection engine.

These tests run completely offline (no Kafka, no PostgreSQL, no Docker).
They verify every rule branch of AnomalyRules.evaluate() individually.
"""

import pytest

from services.anomaly_detector.anomaly_rules import AnomalyRules
from services.common.models import HeartbeatEvent


# Pre-build a rules instance; it's stateless so one instance covers all tests.
rules = AnomalyRules()


def _make_event(heart_rate: int) -> HeartbeatEvent:
    """Helper: create a minimal HeartbeatEvent with the given heart rate."""
    return HeartbeatEvent(customer_id="cust_00001", heart_rate=heart_rate)


# ── Rule 1: LOW_HEART_RATE ─────────────────────────────────────────────────────

class TestLowHeartRate:
    """Heart rate at or below anomaly_low_threshold (default 50) should fire."""

    def test_exactly_at_low_threshold_fires(self):
        """Edge case: rate == threshold should trigger."""
        event = _make_event(50)  # default anomaly_low_threshold
        anomaly = rules.evaluate(event, [])
        assert anomaly is not None
        assert anomaly.anomaly_type == "LOW_HEART_RATE"
        assert anomaly.severity == "high"

    def test_below_low_threshold_fires(self):
        """Rate well below threshold should trigger."""
        event = _make_event(35)
        anomaly = rules.evaluate(event, [])
        assert anomaly is not None
        assert anomaly.anomaly_type == "LOW_HEART_RATE"

    def test_details_contain_threshold_and_measured(self):
        """Details dict must carry threshold + measured for dashboard queries."""
        event = _make_event(42)
        anomaly = rules.evaluate(event, [])
        assert "threshold" in anomaly.details
        assert anomaly.details["measured"] == 42


# ── Rule 2: HIGH_HEART_RATE ────────────────────────────────────────────────────

class TestHighHeartRate:
    """Heart rate at or above anomaly_high_threshold (default 140) should fire."""

    def test_exactly_at_high_threshold_fires(self):
        """Edge case: rate == threshold should trigger."""
        event = _make_event(140)  # default anomaly_high_threshold
        anomaly = rules.evaluate(event, [])
        assert anomaly is not None
        assert anomaly.anomaly_type == "HIGH_HEART_RATE"
        assert anomaly.severity == "high"

    def test_above_high_threshold_fires(self):
        """Rate well above threshold should trigger."""
        event = _make_event(175)
        anomaly = rules.evaluate(event, [])
        assert anomaly is not None
        assert anomaly.anomaly_type == "HIGH_HEART_RATE"

    def test_details_contain_threshold_and_measured(self):
        event = _make_event(165)
        anomaly = rules.evaluate(event, [])
        assert anomaly.details["measured"] == 165


# ── Rule 3: SPIKE ──────────────────────────────────────────────────────────────

class TestSpike:
    """Delta from previous reading ≥ anomaly_spike_delta (default 30) should fire."""

    def test_spike_upward_fires(self):
        """Rate jumping 30+ bpm from the previous reading is a spike."""
        event = _make_event(100)
        history = [65]  # delta = 35 ≥ 30
        anomaly = rules.evaluate(event, history)
        assert anomaly is not None
        assert anomaly.anomaly_type == "SPIKE"
        assert anomaly.severity == "medium"

    def test_spike_downward_fires(self):
        """Rate dropping 30+ bpm is also a spike (absolute delta)."""
        event = _make_event(65)
        history = [100]  # delta = 35 ≥ 30
        anomaly = rules.evaluate(event, history)
        assert anomaly is not None
        assert anomaly.anomaly_type == "SPIKE"

    def test_spike_details_contain_delta_and_previous(self):
        """Details must include delta, previous, and measured values."""
        event = _make_event(105)
        history = [70]
        anomaly = rules.evaluate(event, history)
        assert anomaly.details["delta"] == 35
        assert anomaly.details["previous"] == 70
        assert anomaly.details["measured"] == 105

    def test_no_spike_if_delta_below_threshold(self):
        """Delta of 29 is one below the default threshold — should not fire."""
        event = _make_event(94)
        history = [65]  # delta = 29 < 30
        anomaly = rules.evaluate(event, history)
        assert anomaly is None

    def test_no_spike_when_history_empty(self):
        """With no history, SPIKE rule cannot compute a delta — must return None."""
        event = _make_event(90)
        anomaly = rules.evaluate(event, [])
        assert anomaly is None

    def test_spike_uses_last_value_from_history(self):
        """Only the last value in history is used for delta computation."""
        event = _make_event(100)
        # Only the last element (60) is relevant; delta = 40 ≥ 30
        history = [80, 75, 60]
        anomaly = rules.evaluate(event, history)
        assert anomaly is not None
        assert anomaly.details["previous"] == 60


# ── Normal reading ─────────────────────────────────────────────────────────────

class TestNormalReading:
    """Events within normal range with no large delta should return None."""

    def test_normal_range_no_history(self):
        """Mid-range rate with empty history → no anomaly."""
        event = _make_event(75)
        assert rules.evaluate(event, []) is None

    def test_normal_range_with_stable_history(self):
        """Small variation from a stable previous reading → no anomaly."""
        event = _make_event(78)
        history = [75, 76, 77]
        assert rules.evaluate(event, history) is None

    def test_low_rule_takes_priority_over_spike(self):
        """
        A rate of 50 is at the LOW threshold.  Even if there's also a large
        delta from the previous reading, LOW_HEART_RATE should be returned
        (not SPIKE) because absolute rules take priority.
        """
        event = _make_event(50)
        history = [90]  # delta = 40 → would be SPIKE, but LOW fires first
        anomaly = rules.evaluate(event, history)
        assert anomaly is not None
        assert anomaly.anomaly_type == "LOW_HEART_RATE"
