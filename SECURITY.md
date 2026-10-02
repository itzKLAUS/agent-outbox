# Reporting and handling security issues

This repository is private. Report a suspected vulnerability privately to the repository owner; do not post exploit details, customer payloads or credentials in public issues.

Use synthetic data for reproductions. Preserve the affected package version, operation and transition sequence, operating system, clock behavior and downstream idempotency assumptions. Do not attach a live database or lease token.

If a database or token is exposed, stop dispatch, restrict access, investigate actual downstream effects, and reconcile outstanding leases before restarting. The library has no central revocation service or authentication layer. A fence or token does not authorize an external tool call.
