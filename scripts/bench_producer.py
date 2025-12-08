from __future__ import annotations

import time

from faker import Faker

from producer.generator import DataPools, generate_valid_event


def main() -> None:
    fake = Faker()
    pools = DataPools(fake)
    total = 100_000

    start = time.perf_counter()
    for _ in range(total):
        generate_valid_event(fake, pools)
    elapsed = time.perf_counter() - start
    eps = total / elapsed if elapsed > 0 else 0.0
    print(f"Generated {total} events in {elapsed:.2f}s -> {eps:,.1f} events/sec")


if __name__ == "__main__":
    main()
