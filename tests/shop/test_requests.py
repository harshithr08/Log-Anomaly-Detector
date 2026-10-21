import re

LOG_LINE = re.compile(
    r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z (INFO|WARNING|ERROR) "
    r"(shop|inventory|payment|shipping) \[req=([0-9a-f]{16}|-)\] .+$"
)
CHECKOUT = {"items": [{"product_id": "p17", "quantity": 1}], "card_number": "4242424242424242"}


def test_every_log_line_has_the_standard_format(shop):
    shop.client.get("/products")
    shop.client.post("/checkout", json=CHECKOUT)
    for service in ("shop", "inventory", "payment", "shipping"):
        for line in shop.log_lines(service):
            assert LOG_LINE.match(line), line


def test_checkout_request_id_reaches_every_service(shop):
    response = shop.client.post("/checkout", json=CHECKOUT)
    request_id = response.headers["x-request-id"]

    assert response.status_code == 200  # services use a fixed seed, so no random decline
    for service in ("shop", "inventory", "payment", "shipping"):
        assert shop.lines_for(service, request_id), f"no lines from {service}"

    lines = shop.lines_for("shop", request_id)
    assert "Handling POST /checkout" in lines[0]
    assert "Completed POST /checkout" in lines[-1]


def test_cache_miss_then_hit(shop):
    first = shop.client.get("/products/p03").headers["x-request-id"]
    second = shop.client.get("/products/p03").headers["x-request-id"]
    assert any("Cache miss" in line for line in shop.lines_for("shop", first))
    assert any("Cache hit" in line for line in shop.lines_for("shop", second))
    assert not shop.lines_for("inventory", second)  # served from cache


def test_unknown_product_is_a_normal_404(shop):
    assert shop.client.get("/products/p99").status_code == 404


def test_out_of_stock_is_refused(shop):
    order = {"items": [{"product_id": "p01", "quantity": 1000}], "card_number": "4242"}
    response = shop.client.post("/checkout", json=order)
    assert response.status_code == 409
    assert any("Insufficient stock" in line for line in shop.log_lines("inventory"))
