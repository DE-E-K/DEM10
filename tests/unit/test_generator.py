from services.common.simulator import customer_id_pool, heartbeat_stream


def test_customer_pool_size() -> None:
    pool = customer_id_pool(10)
    assert len(pool) == 10
    assert pool[0] == "cust_00001"


def test_heartbeat_stream_shape() -> None:
    stream = heartbeat_stream(customer_count=5, invalid_ratio=0.0)
    event = next(stream)
    assert event.customer_id.startswith("cust_")
    assert 0 <= event.heart_rate <= 250
