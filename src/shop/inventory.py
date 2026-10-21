"""
inventory.py — stock levels and reservations.

Normal: look up stock, reserve stock for an order (or refuse: out of stock),
release or commit a reservation. A background job restocks low items.
Skip fault: reserve without checking stock levels (can oversell).

Run: uvicorn shop.inventory:create_app --factory --port 8001
"""

import asyncio
import uuid

from fastapi import HTTPException
from pydantic import BaseModel

from shop.catalog import PRODUCTS
from shop.service import Service

RESTOCK_EVERY_S = 30
LOW_STOCK = 3


class Item(BaseModel):
    product_id: str
    quantity: int


class ReservationRequest(BaseModel):
    order_id: str
    items: list[Item]


def create_app(**service_kwargs):
    svc = Service(
        "inventory",
        error_messages=(
            "Database connection lost while reading stock",
            "Deadlock detected while updating stock table",
        ),
        **service_kwargs,
    )
    log, app = svc.log, svc.app
    # Some items start with little stock, so "out of stock" happens naturally.
    stock = {pid: svc.rng.choice([2, 4, 15, 30, 50]) for pid in PRODUCTS}
    reservations: dict[str, list[Item]] = {}

    @svc.background
    async def restock_forever():
        while True:
            await asyncio.sleep(RESTOCK_EVERY_S)
            for pid, level in stock.items():
                if level <= LOW_STOCK:
                    stock[pid] = level + 20
                    log.info("Restocked product=%s new_level=%d", pid, stock[pid])

    @app.get("/stock")
    async def stock_levels(ids: str):
        product_ids = [pid for pid in ids.split(",") if pid in stock]
        log.info("Looking up stock for %d products", len(product_ids))
        await svc.work(2, 10)
        return {pid: stock[pid] for pid in product_ids}

    @app.get("/stock/{product_id}")
    async def stock_level(product_id: str):
        if product_id not in stock:
            raise HTTPException(404, "unknown product")
        await svc.work(1, 5)
        log.info("Stock level product=%s available=%d", product_id, stock[product_id])
        return {"product_id": product_id, "available": stock[product_id]}

    @app.post("/reservations")
    async def reserve(body: ReservationRequest):
        if not svc.faults.hits("skip"):
            log.info("Checking stock levels order=%s items=%d", body.order_id, len(body.items))
            await svc.work(3, 12)
            for item in body.items:
                if stock.get(item.product_id, 0) < item.quantity:
                    log.warning(
                        "Insufficient stock product=%s requested=%d available=%d",
                        item.product_id,
                        item.quantity,
                        stock.get(item.product_id, 0),
                    )
                    raise HTTPException(409, "out of stock")
        for item in body.items:
            stock[item.product_id] = stock.get(item.product_id, 0) - item.quantity
        reservation_id = "r" + uuid.uuid4().hex[:10]
        reservations[reservation_id] = body.items
        await svc.work(2, 8)
        log.info("Reserved stock order=%s reservation=%s", body.order_id, reservation_id)
        return {"reservation_id": reservation_id}

    @app.delete("/reservations/{reservation_id}")
    async def release(reservation_id: str):
        items = reservations.pop(reservation_id, None)
        if items is None:
            raise HTTPException(404, "unknown reservation")
        for item in items:
            stock[item.product_id] += item.quantity
        log.info("Released reservation=%s items=%d", reservation_id, len(items))
        return {"released": reservation_id}

    @app.post("/reservations/{reservation_id}/commit")
    async def commit(reservation_id: str):
        if reservations.pop(reservation_id, None) is None:
            raise HTTPException(404, "unknown reservation")
        await svc.work(2, 6)
        log.info("Committed reservation=%s", reservation_id)
        return {"committed": reservation_id}

    return app
