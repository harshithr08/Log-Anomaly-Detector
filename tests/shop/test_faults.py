import json

from shop.faults import FaultInjector, FaultSpec, GroundTruth
from shop.logs import request_id_var

CHECKOUT = {"items": [{"product_id": "p17", "quantity": 1}], "card_number": "4242424242424242"}


def ground_truth(shop, service):
    path = shop.settings.ground_truth_dir / f"{service}.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


def affected_ids(shop, service):
    return {e["request_id"] for e in ground_truth(shop, service) if e["event"] == "affected"}


def test_skip_removes_the_step_and_is_recorded(shop):
    shop.admin("payment").post("/admin/fault", json={"kind": "skip"})
    request_id = shop.client.post("/checkout", json=CHECKOUT).headers["x-request-id"]

    payment_lines = shop.lines_for("payment", request_id)
    assert payment_lines, "payment was called"
    assert not any("Fraud check" in line for line in payment_lines)
    assert request_id in affected_ids(shop, "payment")


def test_gateway_skip_goes_straight_to_payment(shop):
    shop.admin("shop").post("/admin/fault", json={"kind": "skip"})
    request_id = shop.client.post("/checkout", json=CHECKOUT).headers["x-request-id"]
    assert not shop.lines_for("inventory", request_id)
    assert not any("Stock reserved" in line for line in shop.lines_for("shop", request_id))


def test_error_fault_fails_the_checkout(shop):
    shop.admin("shipping").post("/admin/fault", json={"kind": "error"})
    response = shop.client.post("/checkout", json=CHECKOUT)
    request_id = response.headers["x-request-id"]

    assert request_id in affected_ids(shop, "shipping")
    assert response.status_code == 502
    assert any(" ERROR shipping " in line for line in shop.lines_for("shipping", request_id))


def test_slow_fault_adds_delay(shop):
    shop.admin("inventory").post("/admin/fault", json={"kind": "slow", "delay_ms": 300})
    response = shop.client.get("/search", params={"q": "pen"})
    completed = shop.lines_for("inventory", response.headers["x-request-id"])[-1]
    duration_ms = int(completed.rsplit("duration_ms=", 1)[1])
    assert duration_ms >= 240  # 300ms, minus up to 20% jitter


def test_probability_controls_share_of_requests(shop):
    shop.admin("shop").post(
        "/admin/fault", json={"kind": "slow", "delay_ms": 0, "probability": 0.3}
    )
    for _ in range(200):
        shop.client.get("/products/p05")
    assert 30 <= len(affected_ids(shop, "shop")) <= 90


def test_fix_records_fault_end(shop):
    admin = shop.admin("payment")
    fault_id = admin.post("/admin/fault", json={"kind": "error"}).json()["fault_id"]
    assert admin.delete("/admin/fault").json()["fault"] is None
    events = [(e["event"], e["fault_id"]) for e in ground_truth(shop, "payment")]
    assert events == [("fault_start", fault_id), ("fault_end", fault_id)]


def test_restart_closes_faults_left_open(tmp_path):
    truth = GroundTruth(tmp_path / "payment.jsonl")
    before = FaultInjector("payment", truth)
    fault_id = before.start(FaultSpec(kind="crash"))
    # process dies here; a new process starts with a fresh injector
    FaultInjector("payment", truth).close_open_faults()
    last = truth.events()[-1]
    assert (last["event"], last["fault_id"], last["reason"]) == ("fault_end", fault_id, "restarted")


def test_hits_records_the_current_request(tmp_path):
    injector = FaultInjector("shop", GroundTruth(tmp_path / "shop.jsonl"))
    injector.start(FaultSpec(kind="skip"))
    token = request_id_var.set("abc123")
    try:
        assert injector.hits("skip")
        assert not injector.hits("slow")  # different kind
    finally:
        request_id_var.reset(token)
    assert injector.ground_truth.events()[-1]["request_id"] == "abc123"


def test_a_fault_with_a_duration_fixes_itself(tmp_path):
    now = [100.0]
    injector = FaultInjector("payment", GroundTruth(tmp_path / "p.jsonl"), clock=lambda: now[0])
    fault_id = injector.start(FaultSpec(kind="error", duration_s=120))
    now[0] = 219.0
    assert injector.hits("error") and injector.fixes_in_s() == 1.0
    now[0] = 220.0
    assert not injector.hits("error") and injector.active is None
    last = injector.ground_truth.events()[-1]
    assert (last["event"], last["fault_id"], last["reason"]) == ("fault_end", fault_id, "expired")
