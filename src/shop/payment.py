"""
payment.py — charge a card.

Normal: fraud check, then authorize with the card processor. Sometimes the
processor times out and we retry; sometimes the card is declined.
Skip fault: charge without running the fraud check.

Run: uvicorn shop.payment:create_app --factory --port 8002
"""

import uuid

from fastapi import HTTPException
from pydantic import BaseModel

from shop.service import Service

PROCESSOR_TIMEOUT_RATE = 0.05
DECLINE_RATE = 0.03
FRAUD_BLOCK_SCORE = 0.97
MAX_ATTEMPTS = 3


class ChargeRequest(BaseModel):
    order_id: str
    amount: float
    card_number: str


def create_app(**service_kwargs):
    svc = Service(
        "payment",
        error_messages=(
            "Payment processor returned HTTP 502",
            "TLS handshake with payment processor failed",
        ),
        **service_kwargs,
    )
    log, app = svc.log, svc.app

    @app.post("/charges")
    async def charge(body: ChargeRequest):
        log.info(
            "Charging order=%s amount=%.2f card=****%s",
            body.order_id,
            body.amount,
            body.card_number[-4:],
        )

        if not svc.faults.hits("skip"):
            await svc.work(5, 20)
            score = svc.rng.random()
            if score > FRAUD_BLOCK_SCORE:
                log.warning("Fraud check failed order=%s score=%.2f", body.order_id, score)
                raise HTTPException(402, "fraud check failed")
            log.info("Fraud check passed order=%s score=%.2f", body.order_id, score)

        for attempt in range(1, MAX_ATTEMPTS + 1):
            await svc.work(20, 80)
            if svc.rng.random() >= PROCESSOR_TIMEOUT_RATE:
                break
            log.warning("Processor timeout order=%s attempt=%d, retrying", body.order_id, attempt)
        else:
            log.error("Processor unreachable order=%s after %d attempts", body.order_id, attempt)
            raise HTTPException(503, "processor unreachable")

        if svc.rng.random() < DECLINE_RATE:
            log.warning("Card declined order=%s reason=insufficient_funds", body.order_id)
            raise HTTPException(402, "card declined")

        charge_id = "ch_" + uuid.uuid4().hex[:12]
        log.info("Payment authorized order=%s charge=%s", body.order_id, charge_id)
        return {"charge_id": charge_id}

    return app
