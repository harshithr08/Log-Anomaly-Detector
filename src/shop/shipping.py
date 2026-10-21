"""
shipping.py — create a shipment for a paid order.

Normal: validate the address, pick a carrier, create the shipment.
Skip fault: create the shipment without validating the address.

Run: uvicorn shop.shipping:create_app --factory --port 8003
"""

import uuid

from pydantic import BaseModel

from shop.service import Service

CARRIERS = {"ups": (2, 4), "fedex": (1, 3), "dhl": (3, 6)}  # carrier -> eta range in days
ZONES = ("north", "south", "east", "west")


class ShipmentRequest(BaseModel):
    order_id: str
    items: int


def create_app(**service_kwargs):
    svc = Service(
        "shipping",
        error_messages=(
            "Carrier API rate limit exceeded",
            "Label printer service unavailable",
        ),
        **service_kwargs,
    )
    log, app = svc.log, svc.app

    @app.post("/shipments")
    async def create_shipment(body: ShipmentRequest):
        log.info("Creating shipment order=%s items=%d", body.order_id, body.items)

        if not svc.faults.hits("skip"):
            await svc.work(3, 15)
            log.info("Validated address order=%s zone=%s", body.order_id, svc.rng.choice(ZONES))

        carrier = svc.rng.choice(list(CARRIERS))
        eta_days = svc.rng.randint(*CARRIERS[carrier])
        await svc.work(10, 40)
        log.info("Selected carrier=%s eta_days=%d", carrier, eta_days)

        tracking = "TRK" + uuid.uuid4().hex[:10].upper()
        log.info("Shipment created order=%s tracking=%s", body.order_id, tracking)
        return {"tracking": tracking, "carrier": carrier, "eta_days": eta_days}

    return app
