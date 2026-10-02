# Verification

The Windows/Python 3.12.14 run passed 70 tests with 100% source line coverage. The same 70 tests passed against a clean installed wheel on Python 3.13.5, imported from that environment's site-packages. Tests cover atomic business-write/enqueue commit and rollback, changed idempotency specifications, finite JSON validation, metadata export, stale ownership, deadline boundaries, bounded retries, uncertainty, resolution, event pagination, eight concurrent thread claims, and four spawned-process claim races.

An external-commit/worker-crash test uses a synthetic deduplicating destination. It demonstrates that reusing the stable intent ID returns the first receipt; it does not prove a real provider implements that contract. No live model, payment, blockchain or production API was called.

Additional tests verify concurrent duplicate enqueue, repeated recovery, a tool completing after lease expiry, rollback when event storage fails, and rejection of invalid retry configuration before invoking an adapter.

Formatting, Ruff lint, strict mypy, the synthetic demo, and source/wheel builds passed locally. Hosted CI will be recorded after it is observed. Coverage is line coverage, not a claim of complete branch or failure-mode coverage. This is an initial private implementation, with no enterprise-readiness or novelty claim.
