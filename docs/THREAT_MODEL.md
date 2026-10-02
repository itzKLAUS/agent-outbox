# Trust boundaries

Trusted components: the application establishing scope, keys and authorization; local workers and their adapters; operators resolving ambiguity; SQLite and the host filesystem; the wall clock; and the destination's advertised deduplication contract for retry-safe actions.

The intended fault model includes worker crashes, restarts, duplicate enqueue requests, lost acknowledgements, overlapping local workers, lease expiry and ordinary adapter errors. It excludes a hostile database owner, malicious trusted worker, forged downstream evidence, unreliable network-filesystem locking or a compromised host.

Random tokens and monotonic local fences reject stale ledger transitions. A token is ownership evidence inside the trusted worker environment, not a signed credential or authentication system. Protect connections and tokens; never expose them to an untrusted agent or public API. Fences cannot cancel an external request or keep a malicious worker from bypassing the outbox.

Enqueue validates finite acyclic JSON and bound parameters. SQL strings use parameter binding for caller values. Limits reduce accidental resource use but do not establish a denial-of-service boundary against untrusted traffic. Apply request size, rate and tenant limits at the application edge. The library does not evaluate permissions, authenticate tenants or prevent cross-scope database access.

Adapter selection must be explicit and trusted. Payloads are data, never instructions to execute arbitrary code. No subprocess, remote fetch or model call occurs in the library or synthetic demo.

An uncertain record is evidence of an unresolved outcome, not evidence that the tool failed or ran. Independent downstream receipts are needed for reconciliation. A receipt digest supports matching retained evidence; it does not establish authenticity or make the audit table tamper-proof.

This implementation has not had an independent security review, load test, filesystem power-loss test or production deployment assessment. Tests verify the implemented scenarios, not every operating-system or storage failure mode.
