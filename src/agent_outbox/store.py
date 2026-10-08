"""SQLite outbox with caller-owned enqueue transactions and fenced worker leases."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

Mode = Literal["retry_safe", "once"]
MAX_INT = (1 << 63) - 1
MAX_PAYLOAD = 65_536
STATES = ("pending", "leased", "succeeded", "failed", "uncertain", "cancelled")


class Conflict(ValueError):
    """An idempotency key was reused for a different intent."""


class StaleLease(RuntimeError):
    """A lease expired, was replaced, or no longer owns the intent."""


class InvalidState(RuntimeError):
    """A transition is invalid for the current state."""


def _integer(value: int, label: str, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= MAX_INT:
        raise ValueError(f"{label} must be an integer in [{minimum}, {MAX_INT}]")
    return value


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValueError(f"{label} must be nonblank text of at most 256 characters")
    return value


def _json(value: Any) -> str:
    # Reject tuples, nonstring keys and custom containers instead of silently
    # normalizing them into a different tool payload.
    def check(item: Any) -> None:
        if item is None or type(item) in (bool, int, float, str):
            return
        if type(item) is list:
            for child in item:
                check(child)
            return
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise ValueError("JSON object keys must be strings")
                check(child)
            return
        raise ValueError("Payload must contain only JSON values")

    try:
        check(value)
        result = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, RecursionError, ValueError) as exc:
        raise ValueError("Payload must be finite, acyclic JSON") from exc
    if len(result.encode("utf-8")) > MAX_PAYLOAD:
        raise ValueError("Payload exceeds 64 KiB")
    return result


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class Lease:
    intent_id: str
    fence: int
    token: str
    worker: str
    operation: str
    target: str
    payload: dict[str, Any]
    mode: Mode
    deadline_ms: int

    @property
    def idempotency_key(self) -> str:
        """Stable key to pass to the actual downstream operation on every retry."""
        return self.intent_id


class Outbox:
    """One durable local SQLite database, shared by trusted dispatch workers.

    Use ``enqueue(connection, ...)`` inside the SAME transaction as the business
    write. All other methods own short, independent transactions. This is not an
    authentication, sandbox or downstream exactly-once implementation.
    """

    def __init__(self, path: str | Path, *, clock: Callable[[], int] | None = None):
        if str(path) == ":memory:":
            raise ValueError("a durable database file is required")
        self.path = str(Path(path).resolve())
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        with self.connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS agent_outbox_meta(version INTEGER NOT NULL);
                INSERT INTO agent_outbox_meta SELECT 1
                  WHERE NOT EXISTS (SELECT 1 FROM agent_outbox_meta);
            """)
            versions = conn.execute("SELECT version FROM agent_outbox_meta").fetchall()
            if [row[0] for row in versions] != [1]:
                raise InvalidState("Unsupported outbox schema version")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS agent_outbox_intents (
                    id TEXT PRIMARY KEY, scope TEXT NOT NULL, idem TEXT NOT NULL,
                    spec TEXT NOT NULL, operation TEXT NOT NULL, target TEXT NOT NULL,
                    payload TEXT NOT NULL, mode TEXT NOT NULL,
                    state TEXT NOT NULL, available INTEGER NOT NULL,
                    expires INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, fence INTEGER NOT NULL DEFAULT 0,
                    worker TEXT, token TEXT, lease_until INTEGER,
                    result_digest TEXT, UNIQUE(scope, idem)
                );
                CREATE INDEX IF NOT EXISTS agent_outbox_ready
                  ON agent_outbox_intents(state, available, id);
                CREATE TABLE IF NOT EXISTS agent_outbox_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, intent_id TEXT NOT NULL,
                    at_ms INTEGER NOT NULL, event TEXT NOT NULL, fence INTEGER NOT NULL,
                    details TEXT NOT NULL
                );
            """)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Open a connection; caller explicitly commits its business transaction."""
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
        finally:
            conn.close()  # rolls back a caller's uncommitted transaction

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def _now(self) -> int:
        return _integer(self.clock(), "clock")

    def _event(
        self,
        conn: sqlite3.Connection,
        intent_id: str,
        event: str,
        fence: int,
        now: int,
        **details: Any,
    ) -> None:
        conn.execute(
            "INSERT INTO agent_outbox_events(intent_id,at_ms,event,fence,details) "
            "VALUES(?,?,?,?,?)",
            (intent_id, now, event, fence, _json(details)),
        )

    def enqueue(
        self,
        conn: sqlite3.Connection,
        *,
        scope: str,
        key: str,
        operation: str,
        target: str,
        payload: dict[str, Any],
        mode: Mode = "once",
        expires_ms: int,
        max_attempts: int = 3,
    ) -> str:
        """Insert an intent atomically with a business write. Never commits conn."""
        if not conn.in_transaction:
            raise InvalidState("enqueue requires an explicit caller-owned transaction")
        database = conn.execute("PRAGMA database_list").fetchone()[2]
        if Path(database).resolve() != Path(self.path):
            raise InvalidState("enqueue connection belongs to another database")
        for label, value in (
            ("scope", scope),
            ("key", key),
            ("operation", operation),
            ("target", target),
        ):
            _text(value, label)
        if mode not in ("retry_safe", "once"):
            raise ValueError("Unknown delivery mode")
        if type(payload) is not dict:
            raise ValueError("Payload must be a JSON object")
        expires_ms = _integer(expires_ms, "expires_ms", 1)
        _integer(max_attempts, "max_attempts", 1)
        now = self._now()
        body = _json(payload)
        spec = _digest([operation, target, payload, mode, expires_ms, max_attempts])
        existing = conn.execute(
            "SELECT id,spec FROM agent_outbox_intents WHERE scope=? AND idem=?", (scope, key)
        ).fetchone()
        if existing is not None:
            if existing[1] != spec:
                raise Conflict("Idempotency key reused for changed intent")
            return str(existing[0])
        if expires_ms <= now:
            raise ValueError("New intent must expire in the future")
        intent_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO agent_outbox_intents "
            "(id,scope,idem,spec,operation,target,payload,mode,state,available,"
            "expires,max_attempts) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                intent_id,
                scope,
                key,
                spec,
                operation,
                target,
                body,
                mode,
                "pending",
                now,
                expires_ms,
                max_attempts,
            ),
        )
        self._event(conn, intent_id, "enqueued", 0, now, mode=mode)
        return intent_id

    def _recover(self, conn: sqlite3.Connection, now: int) -> None:
        for row in conn.execute(
            "SELECT * FROM agent_outbox_intents WHERE "
            "(state='leased' AND lease_until<=?) OR (state='pending' AND expires<=?)",
            (now, now),
        ).fetchall():
            if row["state"] == "pending":
                state = "failed"
            elif (
                row["mode"] == "retry_safe"
                and row["attempts"] < row["max_attempts"]
                and now < row["expires"]
            ):
                state = "pending"
            else:
                state = "uncertain"
            conn.execute(
                "UPDATE agent_outbox_intents SET state=?,available=?,token=NULL,"
                "worker=NULL,lease_until=NULL WHERE id=?",
                (state, now, row["id"]),
            )
            self._event(conn, row["id"], "recovered", row["fence"], now, state=state)

    def recover(self) -> None:
        """Recover expired leases without claiming or invoking an adapter."""
        with self._write() as conn:
            self._recover(conn, self._now())

    def claim(self, worker: str, *, lease_ms: int = 30_000) -> Lease | None:
        _text(worker, "worker")
        _integer(lease_ms, "lease_ms", 1)
        with self._write() as conn:
            now = self._now()
            self._recover(conn, now)
            row = conn.execute(
                "SELECT * FROM agent_outbox_intents WHERE state='pending' AND available<=? "
                "ORDER BY available,id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            deadline = min(now + lease_ms, row["expires"])
            fence = row["fence"] + 1
            token = secrets.token_urlsafe(32)
            conn.execute(
                "UPDATE agent_outbox_intents SET state='leased',attempts=attempts+1,"
                "fence=?,token=?,worker=?,lease_until=? WHERE id=?",
                (fence, token, worker, deadline, row["id"]),
            )
            self._event(conn, row["id"], "claimed", fence, now, worker=worker)
            return Lease(
                row["id"],
                fence,
                token,
                worker,
                row["operation"],
                row["target"],
                json.loads(row["payload"]),
                row["mode"],
                deadline,
            )

    def _owned(self, conn: sqlite3.Connection, lease: Lease, now: int) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM agent_outbox_intents WHERE id=?", (lease.intent_id,)
        ).fetchone()
        if (
            row is None
            or row["state"] != "leased"
            or row["fence"] != lease.fence
            or row["token"] != lease.token
            or row["worker"] != lease.worker
            or row["lease_until"] <= now
        ):
            raise StaleLease("Lease is no longer current")
        return cast(sqlite3.Row, row)

    def renew(self, lease: Lease, *, lease_ms: int = 30_000) -> int:
        _integer(lease_ms, "lease_ms", 1)
        with self._write() as conn:
            now = self._now()
            row = self._owned(conn, lease, now)
            deadline = min(max(row["lease_until"], now + lease_ms), row["expires"])
            conn.execute(
                "UPDATE agent_outbox_intents SET lease_until=? WHERE id=?",
                (deadline, lease.intent_id),
            )
            self._event(conn, lease.intent_id, "renewed", lease.fence, now, until=deadline)
            return int(deadline)

    def succeed(self, lease: Lease, result: Any) -> str:
        digest = _digest(result)
        with self._write() as conn:
            now = self._now()
            self._owned(conn, lease, now)
            conn.execute(
                "UPDATE agent_outbox_intents SET state='succeeded',result_digest=?,"
                "token=NULL,worker=NULL,lease_until=NULL WHERE id=?",
                (digest, lease.intent_id),
            )
            self._event(conn, lease.intent_id, "succeeded", lease.fence, now, digest=digest)
        return digest

    def failed(self, lease: Lease) -> None:
        """Record a DEFINITIVELY unsuccessful operation (no ambiguous side effect)."""
        self._finish_error(lease, ambiguous=False, delay_ms=0)

    def retry(self, lease: Lease, *, delay_ms: int = 1_000) -> None:
        """Ambiguous failure: retry only if upstream deduplicates this intent ID."""
        self._finish_error(lease, ambiguous=True, delay_ms=delay_ms)

    def _finish_error(self, lease: Lease, *, ambiguous: bool, delay_ms: int) -> None:
        _integer(delay_ms, "delay_ms")
        with self._write() as conn:
            now = self._now()
            row = self._owned(conn, lease, now)
            available = min(now + delay_ms, MAX_INT)
            if not ambiguous:
                state = "failed"
            elif (
                row["mode"] == "retry_safe"
                and row["attempts"] < row["max_attempts"]
                and available < row["expires"]
            ):
                state = "pending"
            else:
                state = "uncertain"
            conn.execute(
                "UPDATE agent_outbox_intents SET state=?,available=?,token=NULL,"
                "worker=NULL,lease_until=NULL WHERE id=?",
                (state, available, lease.intent_id),
            )
            self._event(conn, lease.intent_id, "attempt_ended", lease.fence, now, state=state)

    def cancel(self, intent_id: str) -> None:
        with self._write() as conn:
            row = conn.execute(
                "SELECT state,fence FROM agent_outbox_intents WHERE id=?", (intent_id,)
            ).fetchone()
            if row is None or row["state"] != "pending":
                raise InvalidState("Only pending intents can be cancelled")
            conn.execute(
                "UPDATE agent_outbox_intents SET state='cancelled' WHERE id=?", (intent_id,)
            )
            self._event(conn, intent_id, "cancelled", row["fence"], self._now())

    def resolve(self, intent_id: str, *, fence: int, succeeded: bool, evidence: Any) -> None:
        """Trusted operator reconciles uncertainty against downstream evidence."""
        if type(succeeded) is not bool:
            raise ValueError("succeeded must be a boolean")
        _integer(fence, "fence", 1)
        digest = _digest(evidence)
        with self._write() as conn:
            row = conn.execute(
                "SELECT state,fence FROM agent_outbox_intents WHERE id=?", (intent_id,)
            ).fetchone()
            if row is None or row["state"] != "uncertain" or row["fence"] != fence:
                raise InvalidState("Resolution requires current uncertain fence")
            state = "succeeded" if succeeded else "failed"
            conn.execute(
                "UPDATE agent_outbox_intents SET state=?,result_digest=? WHERE id=?",
                (state, digest, intent_id),
            )
            self._event(conn, intent_id, "resolved", fence, self._now(), state=state, digest=digest)

    def inspect(self, intent_id: str) -> dict[str, Any] | None:
        """Return metadata only; omit payload, lease token and client idempotency key."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT id,scope,operation,target,mode,state,available,expires,attempts,"
                "max_attempts,fence,worker,lease_until,result_digest "
                "FROM agent_outbox_intents WHERE id=?",
                (intent_id,),
            ).fetchone()
            return None if row is None else dict(row)

    def counts(self) -> dict[str, int]:
        """Operational state counts; no automatic recovery or adapter execution."""
        result = dict.fromkeys(STATES, 0)
        with self.connect() as conn:
            for row in conn.execute(
                "SELECT state,count(*) FROM agent_outbox_intents GROUP BY state"
            ):
                result[row[0]] = row[1]
        return result

    def backup(self, destination: str | Path) -> Path:
        """Create an online, consistent SQLite snapshot without overwriting files.

        The snapshot contains payloads and credentials stored by the application.
        Restore with dispatch disabled and reconcile downstream effects first.
        """
        target = Path(destination).resolve()
        # Exclusive creation also protects the live database and existing backups.
        descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        try:
            with self.connect() as source:
                snapshot = sqlite3.connect(target)
                try:
                    source.backup(snapshot)
                    if snapshot.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise InvalidState("Backup integrity check failed")
                finally:
                    snapshot.close()
        except BaseException:
            target.unlink()
            raise
        return target

    def find(self, state: str, *, after: str = "", limit: int = 100) -> list[dict[str, Any]]:
        """Find metadata by state, paginated by intent ID (not a snapshot)."""
        if state not in STATES:
            raise ValueError("Unknown state")
        _integer(limit, "limit", 1)
        if limit > 1000:
            raise ValueError("At most 1000 intents per page")
        with self.connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT id,scope,operation,target,mode,state,available,expires,attempts,"
                    "max_attempts,fence,worker,lease_until,result_digest "
                    "FROM agent_outbox_intents "
                    "WHERE state=? AND id>? ORDER BY id LIMIT ?",
                    (state, after, limit),
                )
            ]

    def events(self, *, after: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        _integer(after, "after")
        _integer(limit, "limit", 1)
        if limit > 1000:
            raise ValueError("At most 1000 events per page")
        with self.connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM agent_outbox_events WHERE seq>? ORDER BY seq LIMIT ?",
                    (after, limit),
                )
            ]
