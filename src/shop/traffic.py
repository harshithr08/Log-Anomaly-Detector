"""
traffic.py — fake customers sending requests to the shop.

Requests arrive at random moments (Poisson arrivals) around --rate per second,
and each picks a request type from a realistic mix: lots of browsing, some
cart actions, fewer checkouts.

Usage:
    python -m shop.traffic --rate 10                # run until stopped
    python -m shop.traffic --rate 20 --duration 60  # one minute
"""

import argparse
import asyncio
import contextlib
import os
import random
import signal
import time
from collections import Counter

import httpx

from shop.catalog import CATEGORIES, PRODUCTS
from shop.settings import ShopSettings

SEARCH_WORDS = ["mouse", "pan", "shoes", "lamp", "pen", "coffee", "bag", "unicorn", "laptop"]
TEST_CARDS = ["4242424242424242", "5555555555554444", "378282246310005"]
MAX_IN_FLIGHT = 200
REPORT_EVERY_S = 10


def browse(rng):
    params = {"category": rng.choice(CATEGORIES)} if rng.random() < 0.5 else {}
    return "GET", "/products", {"params": params}


def search(rng):
    return "GET", "/search", {"params": {"q": rng.choice(SEARCH_WORDS)}}


def view_product(rng):
    product_id = rng.choice(list(PRODUCTS)) if rng.random() < 0.97 else "p99"  # rare bad link
    return "GET", f"/products/{product_id}", {}


def add_to_cart(rng):
    item = {"product_id": rng.choice(list(PRODUCTS)), "quantity": rng.choice([1, 1, 1, 2, 3])}
    return "POST", "/cart/items", {"json": item}


def checkout(rng):
    products = rng.sample(list(PRODUCTS), k=rng.choice([1, 1, 2, 3]))
    items = [{"product_id": pid, "quantity": rng.choice([1, 1, 2])} for pid in products]
    return "POST", "/checkout", {"json": {"items": items, "card_number": rng.choice(TEST_CARDS)}}


REQUEST_MIX = [(browse, 30), (search, 15), (view_product, 25), (add_to_cart, 12), (checkout, 18)]


class Stats:
    def __init__(self):
        self.statuses = Counter()

    def report(self) -> str:
        total = sum(self.statuses.values())
        parts = ", ".join(f"{status}: {n}" for status, n in sorted(self.statuses.items()))
        self.statuses.clear()
        return f"{total} requests ({parts})"


async def send(client, rng, stats, in_flight):
    makers, weights = zip(*REQUEST_MIX, strict=True)
    method, path, kwargs = rng.choices(makers, weights)[0](rng)
    try:
        response = await client.request(method, path, **kwargs)
        stats.statuses[response.status_code] += 1
    except httpx.HTTPError as error:
        stats.statuses[type(error).__name__] += 1
    finally:
        in_flight.release()


async def run(url: str, rate: float, duration: float, seed: int | None = None):
    rng = random.Random(seed)
    stats = Stats()
    in_flight = asyncio.Semaphore(MAX_IN_FLIGHT)
    tasks = set()
    started = last_report = time.monotonic()
    print(f"Sending ~{rate}/s to {url}" + (f" for {duration}s" if duration else ""), flush=True)

    # Stop cleanly on `docker stop`: as process 1 in a container, Python would
    # otherwise ignore SIGTERM until Docker gives up and kills it.
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):  # not available on Windows
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)

    async with httpx.AsyncClient(base_url=url, timeout=10) as client:
        while not stop.is_set() and (not duration or time.monotonic() - started < duration):
            await asyncio.sleep(rng.expovariate(rate))
            await in_flight.acquire()  # never more than MAX_IN_FLIGHT at once
            task = asyncio.create_task(send(client, rng, stats, in_flight))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            if time.monotonic() - last_report >= REPORT_EVERY_S:
                print(stats.report(), flush=True)
                last_report = time.monotonic()
        await asyncio.gather(*tasks)
    print(stats.report(), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--rate", type=float, default=float(os.getenv("TRAFFIC_RATE", "10")))
    parser.add_argument("--duration", type=float, default=0, help="seconds; 0 = forever")
    parser.add_argument("--url", default=ShopSettings().shop_url)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    asyncio.run(run(args.url, args.rate, args.duration, args.seed))


if __name__ == "__main__":
    main()
