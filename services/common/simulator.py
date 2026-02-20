import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

from services.common.config import settings
from services.common.models import HeartbeatEvent


def customer_id_pool(customer_count: int) -> list[str]:
    return [f"cust_{i:05d}" for i in range(1, customer_count + 1)]


def _sample_heart_rate() -> int:
    baseline = random.randint(58, 95)
    if random.random() < 0.08:
        baseline += random.randint(15, 70)
    if random.random() < 0.03:
        baseline -= random.randint(10, 20)
    return max(settings.heart_rate_min, min(settings.heart_rate_max, baseline))


def heartbeat_stream(customer_count: int, invalid_ratio: float = 0.0) -> Iterator[HeartbeatEvent]:
    customers = customer_id_pool(customer_count)
    while True:
        customer_id = random.choice(customers)
        timestamp = datetime.now(timezone.utc)

        if random.random() < 0.05:
            timestamp = timestamp - timedelta(seconds=random.randint(1, 8))

        if random.random() < invalid_ratio:
            heart_rate = random.choice([-5, 260])
        else:
            heart_rate = _sample_heart_rate()

        yield HeartbeatEvent(customer_id=customer_id, timestamp=timestamp, heart_rate=heart_rate)
