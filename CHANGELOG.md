# Changelog

## 0.2.0

- MIT public release with install metadata, contributor and support guides, and original architecture artwork.
- Online SQLite backup with integrity verification, exclusive destination creation and restrictive POSIX file mode.
- Restore regression checks for business rows, outbox metadata, events and scoped idempotency.
- Explicit rejection of the SQLite `:memory:` sentinel.

## 0.1.0

Transactional enqueue, immutable idempotency, fenced worker leases, retry-safe dispatch, uncertain-outcome reconciliation, bounded operational queries and failure-window tests.
