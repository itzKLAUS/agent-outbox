import sqlite3

import pytest
from test_outbox import put

from agent_outbox import Outbox


def test_backup_preserves_business_write_and_idempotency(box, tmp_path):
    intent = put(box)
    with box.connect() as db:
        db.execute("CREATE TABLE reports (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO reports VALUES (17)")
    backup = box.backup(tmp_path / "snapshot.sqlite")
    restored = Outbox(backup, clock=box.clock)
    assert restored.inspect(intent) == box.inspect(intent)
    assert restored.events() == box.events()
    assert put(restored) == intent
    with restored.connect() as db:
        assert db.execute("SELECT id FROM reports").fetchone()[0] == 17
    assert restored.counts()["pending"] == 1  # no adapter executed


def test_backup_refuses_overwrite_and_source(box, tmp_path):
    target = tmp_path / "existing"
    target.write_bytes(b"preserve")
    for destination in (target, box.path):
        with pytest.raises(FileExistsError):
            box.backup(destination)
    assert target.read_bytes() == b"preserve"


def test_backup_failure_cleans_only_new_snapshot(box, tmp_path, monkeypatch):
    target = tmp_path / "snapshot.sqlite"

    def unavailable(*args, **kwargs):
        raise sqlite3.OperationalError("disk unavailable")

    monkeypatch.setattr(box, "connect", unavailable)
    with pytest.raises(sqlite3.OperationalError):
        box.backup(target)
    assert not target.exists()


def test_memory_database_rejected():
    with pytest.raises(ValueError, match="durable"):
        Outbox(":memory:")
