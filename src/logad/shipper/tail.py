"""
tail.py — read new, complete lines from a growing log file.

Position is tracked as (file identity, byte offset). The identity is the
file's (device, inode) pair, so we notice when the app rotates its log:
"shop.log" is renamed to "shop.log.1" and a new "shop.log" appears. When that
happens we first finish the old file (now "shop.log.1"), then start the new one.

Reading never moves the position by itself: the caller commits a position only
after the lines were delivered. If delivery fails, the same lines are read again.
"""

import json
import os
from dataclasses import dataclass
from pathlib import Path

MAX_READ_BYTES = 1024 * 1024


@dataclass(frozen=True)
class Position:
    file_id: tuple[int, int] | None  # (st_dev, st_ino); None = not opened yet
    offset: int = 0


def file_id(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return (stat.st_dev, stat.st_ino)


def _read_complete_lines(
    path: Path, offset: int, max_lines: int
) -> tuple[list[tuple[int, str]], int]:
    """Up to max_lines lines that end with a newline, starting at offset. A
    half-written last line is left for next time. Returns ([(line offset, line)],
    offset just after the last line returned)."""
    with path.open("rb") as f:
        f.seek(offset)
        chunk = f.read(MAX_READ_BYTES)
    lines, end = [], 0
    while len(lines) < max_lines:
        newline = chunk.find(b"\n", end)
        if newline == -1:
            break
        raw = chunk[end:newline].rstrip(b"\r")
        if raw:
            lines.append((offset + end, raw.decode("utf-8", errors="replace")))
        end = newline + 1
    return lines, offset + end


def _with_ids(lines: list[tuple[int, str]], fid: tuple[int, int]) -> list[tuple[str, str]]:
    """Give each line a permanent ID: <device>.<file id>.<byte offset>. A line
    that is sent again (after a crash or a failed send) keeps the same ID."""
    return [(f"{fid[0]}.{fid[1]}.{at}", text) for at, text in lines]


class FileTailer:
    def __init__(self, path: Path, position: Position | None = None):
        self.path = path
        self.position = position or Position(file_id=None)

    def read(self, max_lines: int = 10_000) -> tuple[list[tuple[str, str]], Position]:
        """New complete lines as (line ID, line), and the position to commit once
        they are delivered."""
        current_id = file_id(self.path)
        if current_id is None:
            return [], self.position
        pos = self.position

        if pos.file_id is not None and pos.file_id != current_id:
            # Rotated: finish the old file first (it was renamed to <name>.1).
            rotated = self.path.with_name(self.path.name + ".1")
            if file_id(rotated) == pos.file_id:
                lines, offset = _read_complete_lines(rotated, pos.offset, max_lines)
                if lines:
                    return _with_ids(lines, pos.file_id), Position(pos.file_id, offset)
            pos = Position(current_id, 0)  # old file done (or gone): start the new one

        if pos.file_id is None:
            pos = Position(current_id, 0)
        if self.path.stat().st_size < pos.offset:  # truncated in place
            pos = Position(current_id, 0)

        lines, offset = _read_complete_lines(self.path, pos.offset, max_lines)
        return _with_ids(lines, current_id), Position(current_id, offset)

    def commit(self, position: Position) -> None:
        self.position = position


class OffsetStore:
    """Positions saved to a small JSON file, so a restarted shipper resumes."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> dict[str, Position]:
        if not self.path.exists():
            return {}
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return {
            name: Position(tuple(p["file_id"]) if p["file_id"] else None, p["offset"])
            for name, p in data.items()
        }

    def save(self, positions: dict[str, Position]) -> None:
        data = {name: {"file_id": p.file_id, "offset": p.offset} for name, p in positions.items()}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        os.replace(tmp, self.path)  # atomic: never leaves a half-written file
