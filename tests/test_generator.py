from faker import Faker

from producer.generator import DataPools, generate_bad_payload, generate_valid_event
from producer.schemas import DeviceType, EventType, UserGender


def test_generate_valid_event_fields():
    fake = Faker()
    pools = DataPools(fake, user_count=5, item_count=3)
    event = generate_valid_event(fake, pools)

    required_keys = {
        "event_id",
        "timestamp",
        "event_type",
        "user_id",
        "user_gender",
        "user_age_group",
        "device_type",
        "item_id",
        "item_category",
        "item_price",
        "session_id",
        "page_url",
        "referrer",
    }
    assert required_keys.issubset(event.keys())  # nosec B101 - test assertion
    assert event["event_type"] in {e.value for e in EventType}  # nosec B101 - test assertion
    assert event["user_gender"] in {g.value for g in UserGender}  # nosec B101 - test assertion
    assert event["device_type"] in {d.value for d in DeviceType}  # nosec B101 - test assertion
    assert isinstance(event["user_id"], int)  # nosec B101 - test assertion
    assert isinstance(event["timestamp"], str)  # nosec B101 - test assertion


def test_generate_bad_payload_not_empty():
    fake = Faker()
    pools = DataPools(fake, user_count=5, item_count=3)
    payload = generate_bad_payload(fake, pools)
    assert payload is not None  # nosec B101 - test assertion
