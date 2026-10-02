"""Synthetic crash recovery; no external tool, model or credentials are used."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_outbox import Outbox


def main() -> None:
    now = [1000]
    with TemporaryDirectory() as directory:
        box = Outbox(Path(directory) / "demo.sqlite", clock=lambda: now[0])
        with box.connect() as conn:
            conn.execute("CREATE TABLE reports(id TEXT PRIMARY KEY, state TEXT)")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO reports VALUES('synthetic-report', 'queued')")
            intent = box.enqueue(
                conn,
                scope="demo",
                key="report-1",
                operation="report.publish",
                target="synthetic-reports",
                payload={"report_id": "synthetic-report"},
                mode="retry_safe",
                expires_ms=60_000,
            )
            conn.commit()

        # A fake downstream service stores a receipt under the immutable intent ID.
        downstream: dict[str, dict[str, str]] = {}
        first = box.claim("worker-before-crash", lease_ms=10)
        assert first is not None
        downstream.setdefault(first.idempotency_key, {"receipt": "synthetic-receipt-1"})
        # This worker exits before acknowledging its already committed side effect.
        now[0] += 10
        second = box.claim("worker-after-restart")
        assert second is not None
        receipt = downstream.setdefault(second.idempotency_key, {"receipt": "unused"})
        box.succeed(second, receipt)
        print(
            json.dumps(
                {
                    "intent": box.inspect(intent),
                    "downstream_writes": len(downstream),
                    "events": box.events(),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
