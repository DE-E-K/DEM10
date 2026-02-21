"""
anomaly_rules.py – Pure, stateless anomaly detection logic.

Why extract this?
-----------------
In the original implementation, anomaly detection logic was embedded directly
inside the Kafka consumer loop.  That made it impossible to unit-test without
spinning up a Kafka broker and a mock DB connection.

This module contains a single class, ``AnomalyRules``, whose ``evaluate()``
method is a **pure function**: given a ``HeartbeatEvent`` and a short history of
recent rates for that customer, it returns either an ``AnomalyEvent`` or ``None``.
It has no I/O dependencies and can be tested completely offline.

Rule set
--------
The three production anomaly rules (in priority order):

1. **LOW_HEART_RATE** (severity: ``high``)
   Heart rate ≤ ``anomaly_low_threshold`` (default 50 bpm).
   Indicates possible bradycardia or sensor detachment.

2. **HIGH_HEART_RATE** (severity: ``high``)
   Heart rate ≥ ``anomaly_high_threshold`` (default 140 bpm).
   Indicates possible tachycardia or strenuous exertion.

3. **SPIKE** (severity: ``medium``)
   Change from the customer's most recent reading ≥ ``anomaly_spike_delta``
   (default 30 bpm).  Indicates a sudden unexplained jump — potentially
   artefactual (sensor slip) or clinically significant.

Rules 1 and 2 take priority over rule 3: if the absolute threshold is already
exceeded we report that, not the delta.
"""

from services.common.config import settings
from services.common.models import AnomalyEvent, HeartbeatEvent


class AnomalyRules:
    """
    Stateless anomaly detection rule engine.

    All configuration is read from the ``settings`` singleton, meaning
    thresholds can be tuned via environment variables without code changes.

    Usage
    -----
    >>> rules = AnomalyRules()
    >>> history = [72, 75, 70]  # Last N heart-rate readings for this customer
    >>> anomaly = rules.evaluate(event, history)
    >>> if anomaly:
    ...     print(anomaly.anomaly_type, anomaly.severity)
    """

    def evaluate(
        self,
        event: HeartbeatEvent,
        recent_rates: list[int],
    ) -> AnomalyEvent | None:
        """
        Apply the rule set to a single heartbeat event.

        Evaluation order: LOW → HIGH → SPIKE.  The first matching rule wins.
        If no rule matches, returns ``None`` (the event is normal).

        Parameters
        ----------
        event:
            The ``HeartbeatEvent`` to evaluate.
        recent_rates:
            Ordered list of the customer's last N heart-rate readings (most
            recent last).  Used to compute the delta for SPIKE detection.
            May be empty for the customer's first ever event.

        Returns
        -------
        AnomalyEvent | None
            The anomaly record if any rule fired, otherwise ``None``.
        """
        rate = event.heart_rate

        # Rule 1: Absolute low threshold
        if rate <= settings.anomaly_low_threshold:
            return AnomalyEvent(
                event_id=event.event_id,
                customer_id=event.customer_id,
                timestamp=event.timestamp,
                heart_rate=rate,
                anomaly_type="LOW_HEART_RATE",
                severity="high",
                details={
                    "threshold": settings.anomaly_low_threshold,
                    "measured": rate,
                },
            )

        # Rule 2: Absolute high threshold
        if rate >= settings.anomaly_high_threshold:
            return AnomalyEvent(
                event_id=event.event_id,
                customer_id=event.customer_id,
                timestamp=event.timestamp,
                heart_rate=rate,
                anomaly_type="HIGH_HEART_RATE",
                severity="high",
                details={
                    "threshold": settings.anomaly_high_threshold,
                    "measured": rate,
                },
            )

        # Rule 3: Sudden spike relative to last reading
        if recent_rates:
            previous = recent_rates[-1]
            delta = abs(rate - previous)
            if delta >= settings.anomaly_spike_delta:
                return AnomalyEvent(
                    event_id=event.event_id,
                    customer_id=event.customer_id,
                    timestamp=event.timestamp,
                    heart_rate=rate,
                    anomaly_type="SPIKE",
                    severity="medium",
                    details={
                        "delta": delta,
                        "threshold": settings.anomaly_spike_delta,
                        "previous": previous,
                        "measured": rate,
                    },
                )

        # No rule fired — event is within normal range
        return None
