import json, sys

# Simulate what consumer.py does
msg_str = '{"event_id": "39b8e5cf-2642-4c17-b2ad-251e4db1637a", "customer_id": "cust_00358", "timestamp": "2026-02-22T09:07:09.697930Z", "heart_rate": 70}'
msg_str2 = '{"event_id": "359ed2e7-c1d9-4a56-9075-7f965f344bb8", "customer_id": "cust_00184", "timestamp": "2026-02-22T09:45:36.829693Z", "heart_rate": 138}'

from services.common.models import HeartbeatEvent
from services.common.config import settings

print(f"HEART_RATE_MIN={settings.heart_rate_min}, HEART_RATE_MAX={settings.heart_rate_max}")

for raw in [msg_str, msg_str2]:
    try:
        payload = json.loads(raw)
        event = HeartbeatEvent.model_validate(payload)
        if not (settings.heart_rate_min <= event.heart_rate <= settings.heart_rate_max):
            raise ValueError(f"heart_rate {event.heart_rate} outside bounds [{settings.heart_rate_min}, {settings.heart_rate_max}]")
        print(f"VALID: customer={event.customer_id} heart_rate={event.heart_rate}")
    except Exception as exc:
        print(f"INVALID: {type(exc).__name__}: {exc}")
