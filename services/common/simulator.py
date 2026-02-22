"""
simulator.py – Physiologically realistic synthetic heart-rate data generator.

Design
------
Each customer is assigned a **stable per-customer resting baseline** drawn from
a realistic population distribution when the simulator starts.  Subsequent
readings are sampled as Gaussian noise around that baseline so the same
customer always produces physiologically coherent numbers over time.

Activity bursts (exercise, recovery) are layered on top with configurable
probabilities to produce interesting variation.

This is far more realistic than the previous approach of picking a random
integer each tick, which could swing 60 bpm in consecutive readings.

Usage
-----
>>> stream = heartbeat_stream(customer_count=100, invalid_ratio=0.02)
>>> event = next(stream)
>>> print(event.customer_id, event.heart_rate)
"""

import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

from services.common.config import settings
from services.common.models import HeartbeatEvent


# Customer profile generation
# =======================================================================

def _assign_resting_baseline() -> int:
    """
    Assign a stable resting heart rate (bpm) for one simulated customer.

    Modelled as a Gaussian centred on 72 bpm (healthy adult resting average)
    with a standard deviation of 10 bpm, clipped to the physiological range
    [50, 100] bpm.  This produces a realistic population distribution.

    Returns
    -------
    int
        Resting heart rate in beats per minute.
    """
    baseline = random.gauss(mu=72, sigma=10)
    return int(max(50, min(100, baseline)))


def customer_id_pool(customer_count: int) -> list[str]:
    """
    Create a deterministic list of customer ID strings.

    Parameters
    ----------
    customer_count:
        Number of unique customer identifiers to generate.

    Returns
    -------
    list[str]
        Identifiers in the form ``["cust_00001", "cust_00002", ...]``.
    """
    return [f"cust_{i:05d}" for i in range(1, customer_count + 1)]


def _build_customer_baselines(customer_count: int) -> dict[str, int]:
    """
    Pre-allocate a stable resting baseline for every simulated customer.

    Having a fixed baseline per customer means consecutive readings from the
    same customer look realistic (gradual drift rather than wild jumps).

    Parameters
    ----------
    customer_count:
        Number of customer IDs to initialise.

    Returns
    -------
    dict[str, int]
        Mapping of ``customer_id`` → resting heart rate (bpm).
    """
    pool = customer_id_pool(customer_count)
    return {cid: _assign_resting_baseline() for cid in pool}


# Heart-rate sampling
# =======================================================================

def _sample_heart_rate(resting: int) -> int:
    """
    Sample a single heart-rate reading for a customer with the given resting rate.

    Activity model
    --------------
    1. **Normal reading** (75% of the time):
       Gaussian noise ±5 bpm around the customer's resting baseline.

    2. **Light exercise** (15% of the time):
       Resting + 20–50 bpm (e.g. brisk walk).

    3. **Peak exercise** (7% of the time):
       Resting + 50–90 bpm (e.g. running or cycling).

    4. **Bradycardic dip** (3% of the time):
       Resting − 5–15 bpm (rest/sleep dip or measurement error).

    The result is clamped to [heart_rate_min, heart_rate_max] from settings so
    that the simulator never accidentally produces domain-invalid values during
    normal operation (invalid values are injected separately via invalid_ratio).

    Parameters
    ----------
    resting:
        The customer's stable resting heart rate (bpm).

    Returns
    -------
    int
        Simulated heart rate in beats per minute.
    """
    roll = random.random()

    if roll < 0.75:
        # Normal: small Gaussian noise around resting baseline
        rate = resting + random.gauss(mu=0, sigma=5)
    elif roll < 0.90:
        # Light exercise
        rate = resting + random.randint(20, 50)
    elif roll < 0.97:
        # Peak exercise
        rate = resting + random.randint(50, 90)
    else:
        # Bradycardic dip
        rate = resting - random.randint(5, 15)

    # Clamp to configured domain bounds
    return int(max(settings.heart_rate_min, min(settings.heart_rate_max, rate)))


# Public streaming generator
# =======================================================================

def _resolve_active_customer_window(
    customer_count: int,
    dynamic_customers: bool,
    active_customers_min: int,
    active_customers_max: int,
) -> tuple[int, int]:
    """
    Resolve and validate active-customer window bounds.

    In static mode, the full customer pool remains active.
    In dynamic mode, min/max bounds are clamped to [1, customer_count].
    """
    if not dynamic_customers:
        return customer_count, customer_count

    min_active = max(1, min(active_customers_min, customer_count))
    max_active = max(1, min(active_customers_max, customer_count))

    if min_active > max_active:
        raise ValueError(
            "active_customers_min cannot be greater than active_customers_max"
        )

    return min_active, max_active


def _sample_active_customers(
    customers: list[str],
    min_active: int,
    max_active: int,
) -> list[str]:
    """Return a random active subset of customers within configured bounds."""
    active_count = random.randint(min_active, max_active)
    return random.sample(customers, active_count)


def heartbeat_stream(
    customer_count: int,
    invalid_ratio: float = 0.0,
    dynamic_customers: bool = False,
    active_customers_min: int | None = None,
    active_customers_max: int | None = None,
    active_set_refresh_probability: float = 0.03,
) -> Iterator[HeartbeatEvent]:
    """
    Infinite generator that yields synthetic ``HeartbeatEvent`` objects.

    Each iteration picks a random customer, samples a physiologically realistic
    heart-rate reading, and occasionally injects deliberate out-of-range values
    (probability = ``invalid_ratio``) to test the consumer's validation path.

    A small fraction of events (5%) receive a slightly back-dated timestamp to
    simulate the realistic scenario of minor clock skew between devices.

    Parameters
    ----------
    customer_count:
        Number of simulated customers to draw from.
    invalid_ratio:
        Fraction of events that should carry out-of-range heart-rate values.
        Set to ``0.0`` in tests to ensure clean data; ``0.02`` in production.
    dynamic_customers:
        If ``True``, emit events from a changing active subset of customers.
        If ``False``, all customers are always active (legacy behaviour).
    active_customers_min:
        Minimum size of active subset in dynamic mode.
    active_customers_max:
        Maximum size of active subset in dynamic mode.
    active_set_refresh_probability:
        Probability per event to resample the active subset in dynamic mode.

    Yields
    ------
    HeartbeatEvent
        An unserialized domain event ready to be JSON-encoded and published.
    """
    if not (0.0 <= active_set_refresh_probability <= 1.0):
        raise ValueError("active_set_refresh_probability must be in [0.0, 1.0]")

    # Pre-build customer pool and stable baselines once, then keep reusing them
    customers = customer_id_pool(customer_count)
    baselines = _build_customer_baselines(customer_count)

    min_active, max_active = _resolve_active_customer_window(
        customer_count=customer_count,
        dynamic_customers=dynamic_customers,
        active_customers_min=(
            active_customers_min if active_customers_min is not None else max(1, customer_count // 5)
        ),
        active_customers_max=(
            active_customers_max if active_customers_max is not None else customer_count
        ),
    )

    active_customers = _sample_active_customers(customers, min_active, max_active)

    while True:
        if (
            dynamic_customers
            and random.random() < active_set_refresh_probability
        ):
            active_customers = _sample_active_customers(customers, min_active, max_active)

        customer_id = random.choice(active_customers)
        resting = baselines[customer_id]
        timestamp = datetime.now(timezone.utc)

        # Simulate minor clock skew on 5% of events (1–8 seconds back)
        if random.random() < 0.05:
            timestamp = timestamp - timedelta(seconds=random.randint(1, 8))

        if random.random() < invalid_ratio:
            # Inject a deliberately bad value to exercise the consumer's
            # validation and quarantine logic
            heart_rate = random.choice([28, 222])  # Hard out-of-bounds
        else:
            heart_rate = _sample_heart_rate(resting)

        yield HeartbeatEvent(
            customer_id=customer_id,
            timestamp=timestamp,
            heart_rate=heart_rate,
        )
