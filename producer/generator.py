from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from faker import Faker
from kafka import KafkaProducer  # type: ignore[attr-defined]
from prometheus_client import Counter, Histogram, start_http_server

from producer.schemas import DeviceType, EventType, LogEvent, UserGender


JsonPayload = Union[Dict[str, Any], str, bytes]

AGE_GROUPS: List[str] = ["10s", "20s", "30s", "40s", "50s", "60s_plus"]
ITEM_CATEGORIES: List[str] = [
    "fashion",
    "electronics",
    "food",
    "beauty",
    "sports",
    "home",
    "books",
]
REFERRERS: List[str] = ["google", "naver", "kakao", "direct", "email", "social"]
DEVICE_TYPES: List[DeviceType] = list(DeviceType)
EVENT_TYPES: List[EventType] = list(EventType)


class DataPools:
    """Faker 호출을 줄이기 위한 캐시 풀."""

    def __init__(self, fake: Faker, user_count: int = 500, item_count: int = 200):
        self.users = [
            fake.unique.random_int(min=1, max=10_000_000) for _ in range(user_count)
        ]
        self.sessions = [fake.uuid4() for _ in range(user_count)]
        self.items = [
            {
                "item_id": fake.unique.bothify(text="item_##??"),
                "item_category": random.choice(ITEM_CATEGORIES),  # nosec B311 - synthetic data
                "item_price": float(random.randint(1_000, 500_000)),  # nosec B311 - synthetic data
            }
            for _ in range(item_count)
        ]
        fake.unique.clear()


def _default_serializer(value: JsonPayload) -> bytes:
    """Serialize dict/str/bytes to bytes for Kafka."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


@dataclass
class ProducerConfig:
    bootstrap_servers: str
    topic: str
    rate_per_sec: int
    bad_ratio: float
    linger_ms: int
    batch_size: int
    acks: str
    client_id: str
    log_every: int
    max_messages: Optional[int]
    log_eps: bool
    metrics_port: int
    metrics_host: str

    @classmethod
    def from_env_and_args(cls, args: argparse.Namespace) -> "ProducerConfig":
        return cls(
            bootstrap_servers=args.bootstrap
            or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
            topic=args.topic or os.getenv("KAFKA_TOPIC", "user_logs"),
            rate_per_sec=args.rate or int(os.getenv("PRODUCER_RATE", "100")),
            bad_ratio=args.bad_ratio
            if args.bad_ratio is not None
            else float(os.getenv("PRODUCER_BAD_RATIO", "0.01")),
            linger_ms=int(os.getenv("PRODUCER_LINGER_MS", "5")),
            batch_size=int(os.getenv("PRODUCER_BATCH_SIZE", "32768")),
            acks=os.getenv("PRODUCER_ACKS", "all"),
            client_id=os.getenv("PRODUCER_CLIENT_ID", "log-generator"),
            log_every=int(os.getenv("PRODUCER_LOG_EVERY", "1000")),
            max_messages=args.max_messages,
            log_eps=bool(int(os.getenv("PRODUCER_LOG_EPS", "0"))) or bool(args.log_eps),
            metrics_port=int(os.getenv("PRODUCER_METRICS_PORT", "8000")),
            metrics_host=os.getenv("PRODUCER_METRICS_HOST", "127.0.0.1"),
        )


def build_producer(config: ProducerConfig) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        value_serializer=_default_serializer,
        linger_ms=config.linger_ms,
        batch_size=config.batch_size,
        acks=config.acks,
        client_id=config.client_id,
        retries=5,
        max_in_flight_requests_per_connection=5,
        request_timeout_ms=30000,
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def generate_valid_event(fake: Faker, pools: DataPools) -> Dict[str, Any]:
    item = random.choice(pools.items)  # nosec B311 - synthetic data
    event = LogEvent(
        event_id=fake.uuid4(),
        timestamp=_now_utc(),
        event_type=random.choice(EVENT_TYPES),  # nosec B311 - synthetic data
        user_id=random.choice(pools.users),  # nosec B311 - synthetic data
        user_gender=random.choice(list(UserGender)),  # nosec B311 - synthetic data
        user_age_group=random.choice(AGE_GROUPS),  # nosec B311 - synthetic data
        device_type=random.choice(DEVICE_TYPES),  # nosec B311 - synthetic data
        item_id=item["item_id"],
        item_category=item["item_category"],
        item_price=item["item_price"],
        session_id=random.choice(pools.sessions),  # nosec B311 - synthetic data
        page_url=f"/product/{item['item_id']}",
        referrer=random.choice(REFERRERS),  # nosec B311 - synthetic data
    )
    record = event.model_dump(mode="json")
    # ClickHouse DateTime는 "YYYY-MM-DD HH:MM:SS" 형태를 선호하므로 포맷 변경
    ts = record.get("timestamp")
    if isinstance(ts, str):
        record["timestamp"] = ts.replace("T", " ").split("Z")[0].split("+")[0]
    return record


def generate_bad_payload(fake: Faker, pools: DataPools) -> JsonPayload:
    mode = random.choice(["missing_field", "malformed_json", "wrong_type"])  # nosec B311 - synthetic data
    base_event = generate_valid_event(fake, pools)

    if mode == "missing_field":
        base_event.pop("event_id", None)
        base_event.pop("timestamp", None)
        return base_event

    if mode == "wrong_type":
        base_event["item_price"] = "not_a_number"
        base_event["event_type"] = 999
        return base_event

    # malformed_json: 의도적으로 JSON을 깨뜨려서 파싱 실패 유도
    return '{"event_id": "broken", "event_type": "view"'  # missing closing brace


def _install_signal_handlers(stop_event: threading.Event) -> None:
    def _handle_signal(signum: int, _: Optional[object]) -> None:
        logging.info("Received signal %s, flushing producer and stopping...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)


PRODUCER_MESSAGES_TOTAL = Counter(
    "log_beaver_producer_messages_total",
    "총 전송 메시지 수",
    ["status"],
)
PRODUCER_BYTES_TOTAL = Counter(
    "log_beaver_producer_bytes_total",
    "전송된 페이로드 바이트 수",
    ["topic"],
)
PRODUCER_SEND_LATENCY_MS = Histogram(
    "log_beaver_producer_send_latency_ms",
    "send()~acks까지 지연 (ms)",
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000],
)


def _record_send_success(
    metadata: Any, start_time: float, topic: str, payload_size: int
) -> None:
    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    PRODUCER_MESSAGES_TOTAL.labels(status="sent").inc()
    PRODUCER_BYTES_TOTAL.labels(topic=topic).inc(payload_size)
    PRODUCER_SEND_LATENCY_MS.observe(elapsed_ms)


def _record_send_error(exc: BaseException) -> None:
    PRODUCER_MESSAGES_TOTAL.labels(status="error").inc()
    logging.error("Failed to send message (async callback)", exc_info=exc)


def run(config: ProducerConfig, fake: Faker) -> None:
    producer = build_producer(config)
    pools = DataPools(fake)
    stop_event = threading.Event()
    _install_signal_handlers(stop_event)

    interval = 1.0 / config.rate_per_sec if config.rate_per_sec > 0 else 0.0
    sent = 0

    logging.info(
        "Starting producer to %s topic=%s rate=%s/s bad_ratio=%.4f",
        config.bootstrap_servers,
        config.topic,
        config.rate_per_sec,
        config.bad_ratio,
    )

    eps_start = time.perf_counter()

    try:
        while not stop_event.is_set():
            if config.max_messages is not None and sent >= config.max_messages:
                logging.info("Reached max_messages=%s, stopping.", config.max_messages)
                break

            payload_obj = (
                generate_bad_payload(fake, pools)
                if random.random() < config.bad_ratio  # nosec B311 - synthetic data
                else generate_valid_event(fake, pools)
            )
            serialized = _default_serializer(payload_obj)

            try:
                send_start = time.perf_counter()
                future = producer.send(config.topic, value=serialized)
                future.add_callback(
                    lambda metadata,
                    start=send_start,
                    topic=config.topic,
                    size=len(serialized): _record_send_success(  # noqa: E501
                        metadata, start, topic, size
                    )
                )
                future.add_errback(_record_send_error)
                sent += 1
            except Exception as exc:  # noqa: BLE001
                PRODUCER_MESSAGES_TOTAL.labels(status="error").inc()
                logging.exception("Failed to send message: %s", exc)

            if sent % config.log_every == 0:
                logging.info("Produced %s messages so far", sent)
                if config.log_eps:
                    elapsed = time.perf_counter() - eps_start
                    eps = sent / elapsed if elapsed > 0 else 0.0
                    logging.info("Approx EPS=%.1f", eps)

            if interval > 0:
                time.sleep(interval)
    finally:
        logging.info("Flushing producer before exit...")  # 종료 전에 반드시 플러시
        producer.flush(timeout=30)
        producer.close()
        logging.info("Producer stopped. Total sent=%s", sent)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka log generator with chaos events."
    )
    parser.add_argument("--bootstrap", help="Kafka bootstrap servers (host:port).")
    parser.add_argument("--topic", help="Kafka topic to publish to.")
    parser.add_argument(
        "--rate",
        type=int,
        help="Messages per second (default env PRODUCER_RATE or 100).",
    )
    parser.add_argument(
        "--bad-ratio",
        type=float,
        dest="bad_ratio",
        help="Fraction of bad messages (0.0-1.0).",
    )
    parser.add_argument(
        "--max-messages", type=int, help="Stop after sending this many messages."
    )
    parser.add_argument(
        "--locale", default="en_US", help="Faker locale (default en_US)."
    )
    parser.add_argument(
        "--log-eps", action="store_true", help="Log approximate EPS periodically."
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    fake = Faker(locale=args.locale)
    config = ProducerConfig.from_env_and_args(args)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logging.info(
        "Starting Prometheus metrics server on %s:%s",
        config.metrics_host,
        config.metrics_port,
    )
    start_http_server(config.metrics_port, addr=config.metrics_host)

    try:
        run(config, fake)
        return 0
    except Exception as exc:  # noqa: BLE001
        logging.exception("Producer failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
