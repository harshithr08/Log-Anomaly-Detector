from logad.parser.parse import TemplateFinder, mask, parse_line

LINE = (
    "2026-10-21T02:50:38.384Z INFO payment [req=90d78a6ef11a42ed] "
    "Charging order=o7f2a91c3d4 amount=42.10 card=****4242"
)


def test_parse_splits_header_and_params():
    parsed = parse_line(LINE)
    assert (parsed.level, parsed.service, parsed.request_id) == (
        "INFO",
        "payment",
        "90d78a6ef11a42ed",
    )
    assert parsed.ts.isoformat() == "2026-10-21T02:50:38.384000+00:00"
    assert parsed.params == {"order": "o7f2a91c3d4", "amount": 42.1, "card": "****4242"}


def test_lines_outside_a_request():
    parsed = parse_line("2026-10-21T02:50:38.384Z INFO inventory [req=-] Restocked product=p03")
    assert parsed.request_id == "-"


def test_garbage_is_rejected():
    assert parse_line("not a log line") is None
    assert parse_line("yesterday INFO shop [req=-] bad timestamp") is None


def test_mask_hides_variable_parts():
    assert mask("Completed GET /products/p03 status=200 duration_ms=12") == (
        "Completed GET /products/<*> status=<*> duration_ms=<*>"
    )
    assert (
        mask("Processor unreachable after 3 attempts") == "Processor unreachable after <*> attempts"
    )
    assert mask("Handling POST /reservations/r3f9a2b1c0d/commit") == (
        "Handling POST /reservations/<*>/commit"
    )


def test_same_event_type_gets_same_template():
    finder = TemplateFinder()
    other = LINE.replace("o7f2a91c3d4", "o1111111111").replace("42.10", "9.99")
    assert finder.template(parse_line(LINE)) == finder.template(parse_line(other))
    assert (
        finder.template(parse_line(LINE)) == "payment INFO Charging order=<*> amount=<*> card=<*>"
    )


def test_different_routes_stay_different():
    finder = TemplateFinder()
    a = "2026-10-21T00:00:00.000Z INFO shop [req=-] Handling GET /products"
    b = "2026-10-21T00:00:00.000Z INFO shop [req=-] Handling GET /search"
    assert finder.template(parse_line(a)) != finder.template(parse_line(b))
