import json
import multiprocessing
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from agent_outbox import Conflict, InvalidState, Outbox, RetryableError, StaleLease, Worker


@pytest.fixture
def clock():
    return [1000]


@pytest.fixture
def box(tmp_path, clock):
    return Outbox(tmp_path / "outbox.sqlite", clock=lambda: clock[0])


def put(box, *, key="job", mode="once", expires_ms=100_000, **kwargs):
    with box.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        intent_id = box.enqueue(
            conn,
            scope="tenant",
            key=key,
            operation="report.write",
            target="reports",
            payload={"number": 1},
            mode=mode,
            expires_ms=expires_ms,
            **kwargs,
        )
        conn.commit()
        return intent_id


def test_business_and_enqueue_commit_together(box):
    with box.connect() as conn:
        conn.execute("CREATE TABLE business(id INTEGER PRIMARY KEY)")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO business VALUES(1)")
        intent = box.enqueue(
            conn, scope="a", key="1", operation="write", target="b", payload={}, expires_ms=2000
        )
        conn.commit()
    assert box.inspect(intent)["state"] == "pending"
    with box.connect() as conn:
        assert conn.execute("SELECT count(*) FROM business").fetchone()[0] == 1


def test_business_and_enqueue_rollback_together(box):
    with box.connect() as conn:
        conn.execute("CREATE TABLE business(id INTEGER PRIMARY KEY)")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO business VALUES(1)")
        intent = box.enqueue(
            conn, scope="a", key="1", operation="write", target="b", payload={}, expires_ms=2000
        )
        # Connection close rolls back both writes.
    assert box.inspect(intent) is None
    assert box.events() == []
    with box.connect() as conn:
        assert conn.execute("SELECT count(*) FROM business").fetchone()[0] == 0


def test_requires_transaction(box):
    with box.connect() as conn, pytest.raises(InvalidState, match="transaction"):
        box.enqueue(
            conn, scope="a", key="k", operation="w", target="b", payload={}, expires_ms=2000
        )


def test_rejects_other_database(box, tmp_path):
    other = Outbox(tmp_path / "other.sqlite")
    with other.connect() as conn, pytest.raises(InvalidState, match="another database"):
        conn.execute("BEGIN IMMEDIATE")
        box.enqueue(
            conn, scope="a", key="k", operation="w", target="b", payload={}, expires_ms=2000
        )


def test_idempotency_survives_completion_and_expiry(box, clock):
    intent = put(box)
    lease = box.claim("worker")
    box.succeed(lease, {"ok": True})
    clock[0] = 100_001
    assert put(box) == intent
    assert len(box.events()) == 3


@pytest.mark.parametrize(
    "change",
    [
        {"operation": "other"},
        {"target": "other"},
        {"payload": {"number": 2}},
        {"mode": "retry_safe"},
        {"expires_ms": 99_999},
        {"max_attempts": 4},
    ],
)
def test_changed_intent_conflicts(box, change):
    put(box)
    arguments = dict(
        scope="tenant",
        key="job",
        operation="report.write",
        target="reports",
        payload={"number": 1},
        mode="once",
        expires_ms=100_000,
        max_attempts=3,
    )
    arguments.update(change)
    with box.connect() as conn, pytest.raises(Conflict):
        conn.execute("BEGIN IMMEDIATE")
        box.enqueue(conn, **arguments)


def test_canonical_payload_and_scope(box):
    with box.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        arguments = dict(key="k", operation="w", target="b", expires_ms=2000)
        first = box.enqueue(conn, scope="a", payload={"x": 1, "y": [2]}, **arguments)
        assert box.enqueue(conn, scope="a", payload={"y": [2], "x": 1}, **arguments) == first
        assert box.enqueue(conn, scope="b", payload={"y": [2], "x": 1}, **arguments) != first
        conn.commit()


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {1: "bad"},
        {"x": float("nan")},
        {"x": float("inf")},
        {"x": (1, 2)},
        {"x": {1, 2}},
        {"x": object()},
        {"x": "a" * 65_537},
    ],
)
def test_invalid_payload(box, payload):
    with box.connect() as conn, pytest.raises(ValueError):
        conn.execute("BEGIN IMMEDIATE")
        box.enqueue(
            conn, scope="a", key="k", operation="w", target="b", payload=payload, expires_ms=2000
        )
    assert box.events() == []


def test_cyclic_payload(box):
    payload = {}
    payload["cycle"] = payload
    with box.connect() as conn, pytest.raises(ValueError):
        conn.execute("BEGIN IMMEDIATE")
        box.enqueue(
            conn, scope="a", key="k", operation="w", target="b", payload=payload, expires_ms=2000
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("scope", ""),
        ("key", " "),
        ("operation", "x" * 257),
        ("target", 1),
        ("mode", "anything"),
        ("expires_ms", True),
        ("expires_ms", 1000),
        ("expires_ms", 1 << 63),
        ("max_attempts", 0),
        ("max_attempts", 1.5),
    ],
)
def test_invalid_intent_arguments(box, field, value):
    arguments = dict(scope="a", key="k", operation="w", target="b", payload={}, expires_ms=2000)
    arguments[field] = value
    with box.connect() as conn, pytest.raises(ValueError):
        conn.execute("BEGIN IMMEDIATE")
        box.enqueue(conn, **arguments)


def test_claim_completion_and_no_payload_leak(box):
    intent = put(box)
    lease = box.claim("worker")
    assert lease.intent_id == intent
    assert lease.idempotency_key == intent
    assert lease.fence == 1
    assert lease.payload == {"number": 1}
    assert box.claim("worker2") is None
    digest = box.succeed(lease, {"private_result": "synthetic"})
    assert len(digest) == 64
    assert box.inspect(intent)["result_digest"] == digest
    assert box.claim("worker") is None
    report = json.dumps([box.inspect(intent), box.events()])
    assert lease.token not in report
    assert "private_result" not in report
    assert '"payload"' not in report
    assert '"idem"' not in report


def test_unknown_intent(box):
    assert box.inspect("missing") is None
    assert box.claim("w") is None
    with pytest.raises(InvalidState):
        box.cancel("missing")


def test_once_crash_is_quarantined(box, clock):
    intent = put(box)
    lease = box.claim("w", lease_ms=10)
    clock[0] += 10
    assert box.claim("other") is None
    assert box.inspect(intent)["state"] == "uncertain"
    with pytest.raises(StaleLease):
        box.succeed(lease, {})


def test_retry_safe_crash_reclaims_with_stable_key(box, clock):
    intent = put(box, mode="retry_safe")
    first = box.claim("w", lease_ms=10)
    clock[0] += 10
    second = box.claim("other")
    assert second.fence == 2
    assert first.idempotency_key == second.idempotency_key == intent
    for action in (
        lambda: box.succeed(first, {}),
        lambda: box.failed(first),
        lambda: box.retry(first),
        lambda: box.renew(first),
    ):
        with pytest.raises(StaleLease):
            action()
    box.succeed(second, {"ok": True})


def test_forged_token_and_worker_rejected(box):
    put(box)
    lease = box.claim("w")
    for changed in (
        replace(lease, token="synthetic-invalid-token"),  # noqa: S106
        replace(lease, worker="other"),
        replace(lease, fence=2),
        replace(lease, intent_id="missing"),
    ):
        with pytest.raises(StaleLease):
            box.succeed(changed, {})


def test_retry_delay_and_exhaustion(box, clock):
    intent = put(box, mode="retry_safe", max_attempts=2)
    first = box.claim("w")
    box.retry(first, delay_ms=50)
    assert box.claim("w") is None
    clock[0] += 50
    second = box.claim("w")
    box.retry(second)
    assert box.inspect(intent)["state"] == "uncertain"
    assert box.claim("w") is None


def test_lease_exhaustion_and_deadline(box, clock):
    intent = put(box, mode="retry_safe", max_attempts=1, expires_ms=1010)
    lease = box.claim("w", lease_ms=999)
    assert lease.deadline_ms == 1010
    clock[0] = 1010
    box.recover()
    assert box.inspect(intent)["state"] == "uncertain"


def test_pending_expiry_never_executes(box, clock):
    intent = put(box, expires_ms=1010)
    clock[0] = 1010
    assert box.claim("w") is None
    assert box.inspect(intent)["state"] == "failed"
    assert box.inspect(intent)["attempts"] == 0


def test_delay_past_deadline_is_uncertain(box):
    intent = put(box, mode="retry_safe", expires_ms=1010)
    box.retry(box.claim("w"), delay_ms=10)
    assert box.inspect(intent)["state"] == "uncertain"


def test_renew_never_shortens_or_exceeds_deadline(box, clock):
    put(box, expires_ms=2000)
    lease = box.claim("w", lease_ms=100)
    assert box.renew(lease, lease_ms=10) == 1100
    clock[0] = 1050
    assert box.renew(lease, lease_ms=5000) == 2000
    clock[0] = 1200
    box.succeed(lease, {})  # original lease object remains valid after renewal


def test_definitive_failure(box):
    intent = put(box, mode="retry_safe")
    lease = box.claim("w")
    box.failed(lease)
    assert box.inspect(intent)["state"] == "failed"
    assert box.claim("w") is None


def test_cancellation_cannot_revoke_execution(box):
    cancelled = put(box, key="cancel")
    box.cancel(cancelled)
    assert box.inspect(cancelled)["state"] == "cancelled"
    leased = put(box, key="leased")
    box.claim("w")
    with pytest.raises(InvalidState):
        box.cancel(leased)


@pytest.mark.parametrize("success", [True, False])
def test_resolution_requires_current_fence(box, success):
    intent = put(box)
    lease = box.claim("w")
    box.retry(lease)
    with pytest.raises(InvalidState):
        box.resolve(intent, fence=2, succeeded=success, evidence={"receipt": "r"})
    box.resolve(intent, fence=1, succeeded=success, evidence={"receipt": "r"})
    assert box.inspect(intent)["state"] == ("succeeded" if success else "failed")
    with pytest.raises(InvalidState):
        box.resolve(intent, fence=1, succeeded=success, evidence={})


def test_resolution_boolean_validation(box):
    with pytest.raises(ValueError):
        box.resolve("missing", fence=1, succeeded=1, evidence={})


def test_event_pagination(box):
    intent = put(box)
    box.succeed(box.claim("w"), {})
    first = box.events(limit=2)
    last = box.events(after=first[-1]["seq"])
    assert [e["event"] for e in first + last] == ["enqueued", "claimed", "succeeded"]
    assert all(e["intent_id"] == intent for e in first + last)
    for arguments in ({"after": -1}, {"limit": 0}, {"limit": 1001}, {"limit": True}):
        with pytest.raises(ValueError):
            box.events(**arguments)


def test_thread_claim_race(box):
    put(box)
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda i: box.claim(f"w{i}"), range(8)))
    assert sum(lease is not None for lease in claims) == 1


def _process_claim(path):
    box = Outbox(path, clock=lambda: 1000)
    lease = box.claim("process")
    return lease.intent_id if lease else None


def test_process_claim_race(box):
    put(box)
    with multiprocessing.get_context("spawn").Pool(4) as pool:
        claims = pool.map(_process_claim, [box.path] * 8)
    assert sum(value is not None for value in claims) == 1


def test_restart_recovers_persisted_lease(box, clock):
    intent = put(box, mode="retry_safe")
    first = box.claim("w", lease_ms=10)
    clock[0] += 10
    reopened = Outbox(box.path, clock=lambda: clock[0])
    second = reopened.claim("new")
    assert second.intent_id == intent
    assert second.fence == first.fence + 1


def test_schema_mismatch(tmp_path):
    path = tmp_path / "new.sqlite"
    Outbox(path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE agent_outbox_meta SET version=99")
    with pytest.raises(InvalidState, match="schema"):
        Outbox(path)


def test_sql_metacharacters_are_bound(box):
    intent = put(box, key="'; DROP TABLE agent_outbox_intents;--")
    assert box.inspect(intent)["state"] == "pending"


def test_clock_validation(tmp_path):
    box = Outbox(tmp_path / "bad.sqlite", clock=lambda: True)
    with pytest.raises(ValueError):
        box.claim("w")


@pytest.mark.parametrize("value", [0, -1, True, 1.5, 1 << 63])
def test_invalid_lease_duration(box, value):
    with pytest.raises(ValueError):
        box.claim("w", lease_ms=value)


def test_worker_success(box):
    intent = put(box)
    seen = []
    worker = Worker(box, "w", lambda lease: seen.append(lease.intent_id))
    assert worker.step()
    assert not worker.step()
    assert seen == [intent]
    assert box.inspect(intent)["state"] == "succeeded"


@pytest.mark.parametrize("mode,state", [("once", "uncertain"), ("retry_safe", "pending")])
def test_worker_retryable_failure(box, mode, state):
    intent = put(box, mode=mode)

    def adapter(lease):
        raise RetryableError("synthetic timeout")

    assert Worker(box, "w", adapter).step()
    assert box.inspect(intent)["state"] == state


def test_unknown_adapter_errors_remain_observable(box):
    intent = put(box)

    def adapter(lease):
        raise RuntimeError("synthetic bug")

    with pytest.raises(RuntimeError, match="synthetic bug"):
        Worker(box, "w", adapter).step()
    assert box.inspect(intent)["state"] == "uncertain"


def test_process_exit_leaves_lease(box):
    intent = put(box)

    def adapter(lease):
        raise SystemExit(1)

    with pytest.raises(SystemExit):
        Worker(box, "w", adapter).step()
    assert box.inspect(intent)["state"] == "leased"


def test_bad_result_remains_ambiguous(box, clock):
    intent = put(box)
    with pytest.raises(ValueError):
        Worker(box, "w", lambda lease: {"bad": object()}).step(lease_ms=10)
    assert box.inspect(intent)["state"] == "leased"
    clock[0] += 10
    box.recover()
    assert box.inspect(intent)["state"] == "uncertain"


def test_downstream_deduplication_after_crash(box, clock):
    intent = put(box, mode="retry_safe")
    downstream = {}
    # Simulate a tool that commits externally, then the worker crashes before ack.
    first = box.claim("crashing", lease_ms=10)
    downstream.setdefault(first.idempotency_key, {"receipt": "one"})
    clock[0] += 10
    second = box.claim("recovering")
    result = downstream.setdefault(second.idempotency_key, {"receipt": "two"})
    box.succeed(second, result)
    assert list(downstream.values()) == [{"receipt": "one"}]
    assert box.inspect(intent)["state"] == "succeeded"
