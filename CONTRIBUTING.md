# Contributing to agent-outbox

Start with a small reproducible issue, the intended behavior, and the integration context. Search existing issues and pull requests first. Use synthetic data; never attach keys, production ledgers, customer payloads or authorization headers.

Run the locked checks in the README. Changes to replay protection, budgets, leases, signatures or recovery need regression tests for the failure path as well as the successful path. Document schema compatibility and the trust boundary. A test passing is not a security certification.

Keep patches focused. Include what changed, why, checks run, and material limitations. Preserve relevant development provenance and comply with dependency licenses. AI-assisted contributions are welcome when the submitter has reviewed and verified the work and takes responsibility for it.

Report security concerns using [SECURITY.md](SECURITY.md), rather than a public exploit issue. Treat other contributors respectfully; harassment and disclosure of private information are not welcome.

Original project code and documentation are licensed under [MIT](LICENSE). Dependencies retain their own licenses. Sponsorship is optional and does not affect access to the software or issue prioritization guarantees.
