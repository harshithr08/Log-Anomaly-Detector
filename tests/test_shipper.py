import pytest

from logad.shipper.ship import Shipper
from logad.shipper.tail import OffsetStore


class Collector:
    """A fake ingest API that records what it receives; can be told to fail."""

    def __init__(self):
        self.lines: list[str] = []
        self.sources: set[str] = set()
        self.fail = False

    async def __call__(self, batch):
        if self.fail:
            raise OSError("ingest down")
        self.lines += [item["line"] for item in batch]
        self.sources |= {item["source"] for item in batch}


@pytest.fixture
def setup(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    store = OffsetStore(tmp_path / "offsets.json")
    collector = Collector()

    def new_shipper(batch_lines=500):
        return Shipper(log_dir, store, collector, batch_lines=batch_lines)

    def write(name, text):
        with (log_dir / name).open("a", encoding="utf-8", newline="") as f:
            f.write(text)

    return log_dir, new_shipper, write, collector


async def drain(shipper):
    while await shipper.ship_once():
        pass


async def test_sends_only_complete_lines(setup):
    _, new_shipper, write, got = setup
    shipper = new_shipper()
    write("shop.log", "a\nb\nhal")
    await drain(shipper)
    assert got.lines == ["a", "b"]

    write("shop.log", "f\n")
    await drain(shipper)
    assert got.lines == ["a", "b", "half"]
    assert got.sources == {"shop"}


async def test_restart_resumes_without_duplicates(setup):
    _, new_shipper, write, got = setup
    write("shop.log", "a\nb\n")
    await drain(new_shipper())

    write("shop.log", "c\n")
    await drain(new_shipper())  # a new process, same offsets file
    assert got.lines == ["a", "b", "c"]


async def test_failed_send_is_retried_not_lost(setup):
    _, new_shipper, write, got = setup
    shipper = new_shipper()
    write("payment.log", "a\nb\n")

    got.fail = True
    with pytest.raises(OSError):
        await shipper.ship_once()
    got.fail = False
    await drain(shipper)
    assert got.lines == ["a", "b"]


async def test_rotation_finishes_old_file_then_starts_new(setup):
    log_dir, new_shipper, write, got = setup
    shipper = new_shipper()
    write("shop.log", "a\nb\n")
    await drain(shipper)

    write("shop.log", "c\n")  # written just before rotation, not shipped yet
    (log_dir / "shop.log").rename(log_dir / "shop.log.1")
    write("shop.log", "d\n")
    await drain(shipper)
    assert got.lines == ["a", "b", "c", "d"]


async def test_backlog_larger_than_a_batch(setup):
    _, new_shipper, write, got = setup
    write("shop.log", "".join(f"line{i}\n" for i in range(1200)))
    shipper = new_shipper(batch_lines=500)
    sizes = []
    while sent := await shipper.ship_once():
        sizes.append(sent)
    assert sizes == [500, 500, 200]
    assert got.lines == [f"line{i}" for i in range(1200)]


async def test_files_are_merged_in_time_order(setup):
    """With full batches, no file may run ahead of the others."""
    _, new_shipper, write, got = setup
    stamp = "2026-10-21T10:00:{:02d}.000Z"
    write("inventory.log", "".join(f"{stamp.format(s)} inventory {s}\n" for s in range(0, 40, 2)))
    write("shop.log", "".join(f"{stamp.format(s)} shop {s}\n" for s in range(1, 40, 2)))
    shipper = new_shipper(batch_lines=5)
    while await shipper.ship_once():
        pass
    seconds = [int(line.split()[-1]) for line in got.lines]
    assert seconds == list(range(40))


async def test_reads_every_log_file(setup):
    _, new_shipper, write, got = setup
    for service in ("shop", "inventory", "payment"):
        write(f"{service}.log", f"{service} line\n")
    write("notes.txt", "not a log\n")
    await drain(new_shipper())
    assert got.sources == {"shop", "inventory", "payment"}
    assert len(got.lines) == 3
