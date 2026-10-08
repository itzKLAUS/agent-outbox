# Problem fit and related work

These projects are focused implementations of established systems ideas. No exhaustive novelty search or claim that equivalent open-source software does not exist is made.

- [Temporal](https://docs.temporal.io/) provides durable execution workflows. Choose it when a distributed workflow platform is the right operational fit.
- [Celery](https://docs.celeryq.dev/en/stable/) provides distributed task queues. A task queue alone does not decide whether a timed-out external side effect should be repeated.
- [Biscuit](https://github.com/eclipse-biscuit/biscuit) provides delegated, attenuable capability authorization. Delegation Gate focuses on exact permissions with a shared local quota ledger and action-bound invocation proofs; it is not a Biscuit implementation.

Agent Outbox is useful for a single-host SQLite application that needs its business write and dispatch intent in one transaction. Delegation Gate is useful inside a trusted gateway with one authoritative local ledger. Action Ledger is useful for a team that needs independent review and declared budget reservations before trusted automation executes. None is a sandbox, a universal payment verifier, or an exactly-once external execution guarantee.

Do not combine the projects by assuming their receipts prove the same thing: a Delegation Gate receipt proves admission; an Action Ledger outcome records a cooperating client's result; an Outbox terminal state records the dispatch/reconciliation decision.
