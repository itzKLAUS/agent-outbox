# State transitions

| State | Trigger | Result |
| --- | --- | --- |
| pending | claim before intent expiry | leased, attempts + 1, fence + 1 |
| pending | cancellation | cancelled |
| pending | intent expiry | failed, no adapter invoked |
| leased | completion under current, unexpired lease | succeeded |
| leased | definitive unsuccessful result | failed |
| leased | ambiguous failure, retry-safe, attempts and deadline permit | pending after delay |
| leased | ambiguous failure without safe retry | uncertain |
| leased | lease expires, retry-safe, attempts and deadline permit | pending immediately |
| leased | lease expires without safe retry | uncertain |
| uncertain | operator supplies current fence and downstream evidence | succeeded or failed |

Succeeded, failed and cancelled intents are terminal. Uncertain intents are never claimed. The next claim performs expiry recovery inside its transaction; `recover()` does so without claiming anything. Recovery at the deadline uses `<=`: an acknowledgement exactly at expiry is stale. Renewals never shorten the current lease or extend it beyond the intent expiry.

`failed(lease)` asserts that the operation definitively failed without an ambiguous effect. A transport timeout does not satisfy that contract. `retry(lease)` handles ambiguity: it retries only in retry-safe mode, otherwise quarantining the intent. Exhausted attempts become uncertain because an earlier attempt might have committed externally.

Reconciliation requires the current fence to prevent two operators from racing a stale decision. Evidence is canonically hashed; the operator must retain the actual evidence elsewhere. The library authenticates neither operators nor workers.

Idempotency lookup precedes new-intent expiry validation: a client can retrieve the original ID after completion or expiry by presenting the same immutable intent specification. Reusing a key with changed arguments fails even when the original intent is terminal.
