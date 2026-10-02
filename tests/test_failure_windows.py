import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_outbox import put

from agent_outbox import Outbox, StaleLease, Worker


def test_duplicate_enqueue_race(tmp_path):
    box = Outbox(tmp_path / "race.sqlite", clock=lambda: 1000)
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(lambda _: put(box), range(8)))
    assert len(set(ids)) == 1
    assert len(box.events()) == 1


def test_recovery_is_idempotent(tmp_path):
    now = [1000]
    box = Outbox(tmp_path / "recovery.sqlite", clock=lambda: now[0])
    put(box)
    box.claim("w", lease_ms=1)
    now[0] += 1
    box.recover()
    events = box.events()
    box.recover()
    assert box.events() == events


def test_action_finishes_after_lease_expiry(tmp_path):
    now = [1000]
    box = Outbox(tmp_path / "slow.sqlite", clock=lambda: now[0])
    intent = put(box)
    writes = []

    def adapter(lease):
        writes.append(lease.intent_id)
        now[0] += 10
        return {"receipt": "synthetic"}

    with pytest.raises(StaleLease):
        Worker(box, "w", adapter).step(lease_ms=10)
    assert writes == [intent]
    box.recover()
    assert box.inspect(intent)["state"] == "uncertain"
    assert not Worker(box, "w2", adapter).step()
    assert writes == [intent]


def test_transition_and_event_rollback_on_storage_error(tmp_path):
    box = Outbox(tmp_path / "failure.sqlite", clock=lambda: 1000)
    intent = put(box)
    with box.connect() as conn:
        conn.execute("""
            CREATE TRIGGER reject_events BEFORE INSERT ON agent_outbox_events
            BEGIN SELECT RAISE(ABORT, 'synthetic storage error'); END
        """)
    with pytest.raises(sqlite3.IntegrityError, match="synthetic storage error"):
        box.claim("w")
    assert box.inspect(intent)["state"] == "pending"
    assert box.inspect(intent)["attempts"] == 0
    assert len(box.events()) == 1


def test_invalid_retry_config_prevents_invocation(tmp_path):
    box = Outbox(tmp_path / "config.sqlite", clock=lambda: 1000)
    intent = put(box)
    seen = []
    with pytest.raises(ValueError):
        Worker(box, "w", lambda lease: seen.append(lease)).step(delay_ms=-1)
    assert seen == []
    assert box.inspect(intent)["state"] == "pending"
