"""
test_generator.py – Unit tests for the synthetic heartbeat data simulator.

These tests run entirely offline — no Kafka, no PostgreSQL, no Docker required.

Coverage
--------
* Customer ID pool shape and format.
* Stream produces correctly shaped HeartbeatEvent objects.
* Per-customer baselines are stable (same customer always reads within a
  plausible range, not wildly swinging each tick).
* invalid_ratio correctly injects out-of-range values at the expected rate.
"""

import statistics

import pytest

from services.common.simulator import (
    _build_customer_baselines,
    customer_id_pool,
    heartbeat_stream,
)


# ── customer_id_pool ───────────────────────────────────────────────────────────

class TestCustomerIdPool:

    def test_pool_size_matches_requested_count(self):
        pool = customer_id_pool(10)
        assert len(pool) == 10

    def test_first_element_formatted_correctly(self):
        pool = customer_id_pool(10)
        assert pool[0] == "cust_00001"

    def test_last_element_formatted_correctly(self):
        pool = customer_id_pool(10)
        assert pool[-1] == "cust_00010"

    def test_all_ids_are_unique(self):
        pool = customer_id_pool(50)
        assert len(set(pool)) == 50


# ── heartbeat_stream ───────────────────────────────────────────────────────────

class TestHeartbeatStream:

    def test_emitted_event_has_correct_customer_format(self):
        stream = heartbeat_stream(customer_count=5, invalid_ratio=0.0)
        event = next(stream)
        assert event.customer_id.startswith("cust_")

    def test_heart_rate_within_physiological_hard_bounds(self):
        """All generated heart-rate values must remain within [0, 250]."""
        stream = heartbeat_stream(customer_count=10, invalid_ratio=0.0)
        for event in (next(stream) for _ in range(500)):
            assert 0 <= event.heart_rate <= 250, (
                f"Hard bound violation: {event.heart_rate}"
            )

    def test_zero_invalid_ratio_produces_no_out_of_range(self):
        """With invalid_ratio=0.0, no values outside [45, 185] should appear."""
        from services.common.config import settings
        stream = heartbeat_stream(customer_count=20, invalid_ratio=0.0)
        for event in (next(stream) for _ in range(1000)):
            assert settings.heart_rate_min <= event.heart_rate <= settings.heart_rate_max, (
                f"Domain bound violated with invalid_ratio=0: {event.heart_rate}"
            )

    def test_invalid_ratio_injects_out_of_range_values(self):
        """
        With invalid_ratio=1.0, every event should be out-of-range.
        We use 0.5 and check that at least some out-of-range values appear.
        """
        from services.common.config import settings
        stream = heartbeat_stream(customer_count=10, invalid_ratio=0.5)
        events = [next(stream) for _ in range(200)]
        out_of_range = [
            e for e in events
            if not (settings.heart_rate_min <= e.heart_rate <= settings.heart_rate_max)
        ]
        # With a 50% ratio we should get a meaningful fraction of bad values
        assert len(out_of_range) > 20, (
            f"Expected >20 out-of-range events, got {len(out_of_range)}"
        )

    def test_event_has_timezone_aware_timestamp(self):
        """Timestamps must be timezone-aware (UTC) for correct DB storage."""
        stream = heartbeat_stream(customer_count=5)
        event = next(stream)
        assert event.timestamp.tzinfo is not None

    def test_dynamic_mode_respects_fixed_active_subset_size(self):
        stream = heartbeat_stream(
            customer_count=50,
            invalid_ratio=0.0,
            dynamic_customers=True,
            active_customers_min=5,
            active_customers_max=5,
            active_set_refresh_probability=0.0,
        )
        seen_customer_ids = {next(stream).customer_id for _ in range(300)}
        assert len(seen_customer_ids) == 5


# ── Per-customer baseline stability ───────────────────────────────────────────

class TestPerCustomerBaseline:
    """
    Verify that the same customer always generates readings that are
    physiologically coherent — i.e., clustered around a stable baseline
    rather than jumping wildly each tick.
    """

    def test_single_customer_readings_have_low_stddev(self):
        """
        Readings for a single customer (no invalid_ratio, no exercise bursts)
        should have a standard deviation well below that of a uniform-random
        generator.  We allow up to 35 bpm stddev because the simulator
        includes exercise tier events for realistic variation.
        """
        stream = heartbeat_stream(customer_count=1, invalid_ratio=0.0)
        rates = [next(stream).heart_rate for _ in range(300)]
        stddev = statistics.stdev(rates)
        assert stddev < 35, (
            f"StdDev {stddev:.1f} bpm seems too high — baseline may not be stable"
        )

    def test_baselines_vary_across_customers(self):
        """
        Different customers should have different resting baselines (population
        diversity).  With 50 customers, mean rates should differ.
        """
        baselines = _build_customer_baselines(50)
        unique_baselines = set(baselines.values())
        # Very likely that at least 5 distinct baselines appear with 50 customers
        assert len(unique_baselines) >= 5
