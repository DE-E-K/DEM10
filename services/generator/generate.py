import json
import itertools

from services.common.config import settings
from services.common.simulator import heartbeat_stream


def main() -> None:
    stream = heartbeat_stream(
        customer_count=settings.sim_customer_count,
        invalid_ratio=settings.sim_invalid_ratio,
        dynamic_customers=settings.sim_dynamic_customers,
        active_customers_min=settings.sim_active_customers_min,
        active_customers_max=settings.sim_active_customers_max,
        active_set_refresh_probability=settings.sim_active_customers_refresh_probability,
    )
    for event in itertools.islice(stream, 100):
        print(json.dumps(event.model_dump(mode="json")))


if __name__ == "__main__":
    main()
