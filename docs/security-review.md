# Security review

Review date: 2026-08-15.

## Trust boundaries

Treat task payloads, results, errors, signals, trace baggage, queue names, task names, and administrator actor names as untrusted data. Treat the schema owner, migration credentials, worker code, and the reverse proxy that authenticates administrator requests as trusted.

The engine is not a sandbox. A handler runs with the operating-system and network permissions of its worker process. Run handlers from different trust domains in separate worker Deployments with separate service accounts and database roles.

## PostgreSQL roles

Use one login per capability:

| Role | Capability |
| --- | --- |
| Owner | Apply migrations and configure grants |
| Producer | Enqueue tasks, emit signals, and inspect task results |
| Worker | Claim and transition tasks, renew leases, and run durable primitives |
| Observer | Read only the observer views |
| Administrator | Read observer views and invoke administrative operations |

The schema revokes public schema, table, and function access. Protocol functions use `SECURITY DEFINER` with a fixed `search_path`. Runtime-role integration tests prove allowed operations and rejected table or function access through real PostgreSQL.

Do not reuse a database role for a less privileged capability after calling `configure_grants`. PostgreSQL grants are additive. Create a new role when reducing privileges.

## Administrator service

Administrator mode is disabled by default. When you enable it, place the service behind an authenticating reverse proxy. The proxy must remove the configured actor header from client requests and inject the authenticated identity itself.

Every successful retry, cancel, pause, and resume operation writes an immutable audit row in the same transaction as the mutation. Missing actor identity is rejected. Keep the administrator service private even when observer pages are public.

## Data exposure

Payloads, results, errors, checkpoints, signals, idempotency keys, and retained audit target identifiers can contain sensitive data. The database stores them as plaintext. PostgreSQL encryption, backup encryption, retention, and access auditing remain deployment responsibilities.

Telemetry excludes payloads, results, errors, task identifiers, and idempotency keys from metric attributes. Trace context and baggage are persisted, so producers must not place secrets in baggage.

## Resource limits

The database rejects values above the configured payload, result, error, and checkpoint limits. Claims, recovery,
schedule catch-up, lease renewal, and retention use bounded batches. Optional queue admission limits bound nonterminal
task growth. These limits reduce accidental amplification but do not replace PostgreSQL statement timeouts, connection
limits, workload isolation, or Kubernetes resource limits.

## Supply chain

`cargo deny check` is the dependency-policy gate. It rejects advisories, unapproved licenses, wildcard dependencies, unknown Git sources, and unknown registries. Duplicate transitive versions are warnings because upstream packages can migrate independently.

Release artifacts require immutable version tags. Container images and the Helm OCI artifact must be signed and verified by the release workflow before a release is accepted.

## Review result

No known critical or high-severity issue remains in the reviewed 0.1.0 implementation. The following risks are explicit deployment responsibilities:

- Handler code is trusted and unsandboxed.
- Database content is not application-level encrypted.
- Administrator identity depends on a correctly configured trusted proxy.
- PostgreSQL denial-of-service protection depends on deployment limits and capacity.
- Dependency and artifact checks must run again for every release candidate.

Run the role integration suite on PostgreSQL 17 and 18, `cargo deny check`, Clippy with warnings denied, the Python type and coverage suite, and Helm validation before accepting a release candidate.
