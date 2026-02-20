import json
import itertools

from services.common.config import settings
from services.common.simulator import heartbeat_stream


def main() -> None:
    stream = heartbeat_stream(settings.sim_customer_count, settings.sim_invalid_ratio)
    for event in itertools.islice(stream, 100):
        print(json.dumps(event.model_dump(mode="json")))


if __name__ == "__main__":
    main()
