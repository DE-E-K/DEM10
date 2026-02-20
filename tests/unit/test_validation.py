import pytest
from pydantic import ValidationError
from services.common.models import HeartbeatEvent


def test_empty_customer_id_rejected() -> None:
    with pytest.raises(ValidationError):
        HeartbeatEvent(customer_id="", heart_rate=70)


def test_heart_rate_bounds_enforced() -> None:
    with pytest.raises(ValidationError):
        HeartbeatEvent(customer_id="cust_001", heart_rate=300)
