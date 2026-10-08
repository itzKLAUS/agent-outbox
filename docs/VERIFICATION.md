# Verification

The Windows/Python 3.12.14 run passed 76 tests with 100% source line coverage. The same 76 tests passed against a clean installed wheel on Python 3.13.5, imported from that environment's site-packages. Tests cover atomic business-write/enqueue commit and rollback, changed idempotency specifications, finite JSON validation, metadata export, stale ownership, deadline boundaries, bounded retries, uncertainty, resolution, event pagination, eight concurrent thread claims, and four spawned-process claim races.

An external-commit/worker-crash test uses a synthetic deduplicating destination. It demonstrates that reusing the stable intent ID returns the first receipt; it does not prove a real provider implements that contract. No live model, payment, blockchain or production API was called.

Additional tests verify concurrent duplicate enqueue, repeated recovery, a tool completing after lease expiry, rollback when event storage fails, and rejection of invalid retry configuration before invoking an adapter.

Operational queries are covered for all state counts, intent-ID pagination, uncertainty discovery, query bounds and exclusion of payloads and tokens.

Formatting, Ruff lint, strict mypy, the synthetic demo, and source/wheel builds passed locally. Hosted CI will be recorded after it is observed. Coverage is line coverage, not a claim of complete branch or failure-mode coverage. This is an initial private implementation, with no enterprise-readiness or novelty claim.

## Public release 0.2.0 — 2026-10-08

Windows/Python 3.12.14: 80 tests pass, 99.65% source line coverage. Ruff lint/format and strict mypy pass. Wheel and source builds succeed; all 80 tests also pass against the installed wheel in an isolated environment. Synthetic demos pass. New tests exercise backup overwrite refusal, failure cleanup and isolated restore; restore does not execute adapters.

The Git history and tracked file list were reviewed before publication; a credential-pattern scan found no matches. This is not a guarantee that no secret or vulnerability exists. Review current cross-platform runs in [GitHub Actions](https://github.com/itzKLAUS/agent-outbox/actions/workflows/check.yml). Production workload benchmarks and independent security review remain open.
