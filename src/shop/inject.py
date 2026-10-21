"""
inject.py — break and fix shop services from the command line.

Usage:
    python -m shop.inject status
    python -m shop.inject break payment slow --delay-ms 1500
    python -m shop.inject break inventory skip --probability 0.5
    python -m shop.inject break shipping error
    python -m shop.inject break payment crash
    python -m shop.inject fix payment
    python -m shop.inject fix all
"""

import argparse

import httpx

from shop.settings import SERVICES, ShopSettings


def main():
    parser = argparse.ArgumentParser(description="Break and fix shop services.")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("status", help="show active faults")

    broken = commands.add_parser("break", help="start a fault")
    broken.add_argument("service", choices=SERVICES)
    broken.add_argument("kind", choices=["slow", "error", "skip", "crash"])
    broken.add_argument("--probability", type=float, default=1.0)
    broken.add_argument("--delay-ms", type=int, default=2000)

    fixed = commands.add_parser("fix", help="stop a fault")
    fixed.add_argument("service", choices=[*SERVICES, "all"])

    args = parser.parse_args()
    settings = ShopSettings()

    with httpx.Client(timeout=5) as client:
        if args.command == "break":
            spec = {"kind": args.kind, "probability": args.probability, "delay_ms": args.delay_ms}
            url = settings.url_for(args.service) + "/admin/fault"
            print(client.post(url, json=spec).raise_for_status().json())
            return

        targets = SERVICES if args.command == "status" or args.service == "all" else [args.service]
        for service in targets:
            url = settings.url_for(service) + "/admin/fault"
            try:
                response = client.delete(url) if args.command == "fix" else client.get(url)
                fault = response.raise_for_status().json()["fault"]
                print(f"{service:<10} {fault or 'healthy'}")
            except httpx.HTTPError as error:
                print(f"{service:<10} unreachable ({type(error).__name__})")


if __name__ == "__main__":
    main()
