"""
gateway.py — the "shop" service: the front door customers talk to.

Request types:
    GET  /products            list products (cached availability)
    GET  /search?q=...        search by name
    GET  /products/{id}       one product (cached)
    POST /cart/items          add to cart, if in stock
    POST /checkout            reserve stock -> charge -> ship -> commit

Skip fault: checkout forgets to reserve stock and goes straight to payment.

Run: uvicorn shop.gateway:create_app --factory --port 8000
"""

import time
import uuid

from fastapi import HTTPException
from pydantic import BaseModel

from shop.catalog import PRODUCTS
from shop.service import Service

CACHE_TTL_S = 5.0


class CartItem(BaseModel):
    product_id: str
    quantity: int = 1


class CheckoutRequest(BaseModel):
    items: list[CartItem]
    card_number: str


def create_app(**service_kwargs):
    svc = Service(
        "shop",
        error_messages=(
            "Session store unavailable",
            "Template rendering failed",
        ),
        **service_kwargs,
    )
    log, app = svc.log, svc.app
    cache: dict[str, tuple[float, object]] = {}

    async def cached(key: str, fetch):
        """Return a cached value, or fetch and cache it."""
        hit = cache.get(key)
        if hit and hit[0] > time.monotonic():
            log.info("Cache hit key=%s", key)
            return hit[1]
        log.info("Cache miss key=%s", key)
        value = await fetch()
        cache[key] = (time.monotonic() + CACHE_TTL_S, value)
        return value

    async def stock_for(product_ids: list[str]) -> dict[str, int]:
        response = await svc.call(
            "inventory", "GET", "/stock", params={"ids": ",".join(product_ids)}
        )
        response.raise_for_status()
        return response.json()

    def failed(step: str, response) -> HTTPException:
        log.error("Checkout step failed step=%s status=%d", step, response.status_code)
        return HTTPException(502, f"{step} failed")

    @app.get("/products")
    async def list_products(category: str | None = None):
        log.info("Listing products category=%s", category or "all")
        ids = [pid for pid, p in PRODUCTS.items() if category in (None, p["category"])]
        stock = await cached(f"stock:{category or 'all'}", lambda: stock_for(ids))
        log.info("Returning products count=%d", len(ids))
        return [{"id": pid, **PRODUCTS[pid], "in_stock": stock.get(pid, 0) > 0} for pid in ids]

    @app.get("/search")
    async def search(q: str):
        log.info("Searching products query=%s", q)
        ids = [pid for pid, p in PRODUCTS.items() if q.lower() in p["name"].lower()]
        if not ids:
            log.info("Search returned no results")
            return []
        stock = await stock_for(ids)
        log.info("Search returned results count=%d", len(ids))
        return [{"id": pid, **PRODUCTS[pid], "in_stock": stock.get(pid, 0) > 0} for pid in ids]

    @app.get("/products/{product_id}")
    async def product(product_id: str):
        if product_id not in PRODUCTS:
            log.warning("Product not found product=%s", product_id)
            raise HTTPException(404, "product not found")

        async def fetch():
            response = await svc.call("inventory", "GET", f"/stock/{product_id}")
            response.raise_for_status()
            return response.json()["available"]

        available = await cached(f"product:{product_id}", fetch)
        log.info("Viewed product=%s available=%d", product_id, available)
        return {"id": product_id, **PRODUCTS[product_id], "available": available}

    @app.post("/cart/items")
    async def add_to_cart(item: CartItem):
        if item.product_id not in PRODUCTS:
            raise HTTPException(404, "product not found")
        log.info("Adding to cart product=%s quantity=%d", item.product_id, item.quantity)
        response = await svc.call("inventory", "GET", f"/stock/{item.product_id}")
        response.raise_for_status()
        available = response.json()["available"]
        if available < item.quantity:
            log.warning(
                "Cannot add to cart product=%s requested=%d available=%d",
                item.product_id,
                item.quantity,
                available,
            )
            raise HTTPException(409, "not enough stock")
        log.info("Added to cart product=%s quantity=%d", item.product_id, item.quantity)
        return {"added": item.product_id}

    @app.post("/checkout")
    async def checkout(body: CheckoutRequest):
        order_id = "o" + uuid.uuid4().hex[:10]
        items = [i.model_dump() for i in body.items]
        total = sum(PRODUCTS[i.product_id]["price"] * i.quantity for i in body.items)
        log.info("Starting checkout order=%s items=%d", order_id, len(items))

        reservation_id = None
        if not svc.faults.hits("skip"):
            response = await svc.call(
                "inventory", "POST", "/reservations", json={"order_id": order_id, "items": items}
            )
            if response.status_code == 409:
                log.warning("Checkout failed: out of stock order=%s", order_id)
                raise HTTPException(409, "out of stock")
            if response.status_code != 200:
                raise failed("reserve", response)
            reservation_id = response.json()["reservation_id"]
            log.info("Stock reserved order=%s reservation=%s", order_id, reservation_id)

        async def release():
            if reservation_id:
                await svc.call("inventory", "DELETE", f"/reservations/{reservation_id}")
                log.info("Released reservation=%s order=%s", reservation_id, order_id)

        log.info("Payment started order=%s amount=%.2f", order_id, total)
        response = await svc.call(
            "payment",
            "POST",
            "/charges",
            json={"order_id": order_id, "amount": total, "card_number": body.card_number},
        )
        if response.status_code == 402:
            log.warning("Payment declined order=%s", order_id)
            await release()
            raise HTTPException(402, "payment declined")
        if response.status_code != 200:
            await release()
            raise failed("payment", response)
        log.info("Payment captured order=%s charge=%s", order_id, response.json()["charge_id"])

        response = await svc.call(
            "shipping", "POST", "/shipments", json={"order_id": order_id, "items": len(items)}
        )
        if response.status_code != 200:
            raise failed("shipping", response)
        shipment = response.json()
        log.info("Shipment booked order=%s tracking=%s", order_id, shipment["tracking"])

        if reservation_id:
            await svc.call("inventory", "POST", f"/reservations/{reservation_id}/commit")
        log.info("Checkout complete order=%s total=%.2f", order_id, total)
        return {"order_id": order_id, "total": round(total, 2), **shipment}

    return app
