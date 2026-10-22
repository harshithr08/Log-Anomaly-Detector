import json
import logging

from logad.common.logs import JsonFormatter


def test_log_lines_are_json():
    record = logging.LogRecord(
        "logad.parser", logging.WARNING, "x.py", 1, "New %s", ("thing",), None
    )
    entry = json.loads(JsonFormatter().format(record))
    assert entry["level"] == "WARNING" and entry["logger"] == "logad.parser"
    assert entry["message"] == "New thing" and entry["ts"].endswith("Z")


def test_errors_stay_on_one_line():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord("x", logging.ERROR, "x.py", 1, "failed", (), sys.exc_info())
    line = JsonFormatter().format(record)
    assert "\n" not in line and "ValueError: boom" in json.loads(line)["error"]
