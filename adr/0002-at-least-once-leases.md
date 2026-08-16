# 0002: Use at-least-once delivery with fenced leases

Status: accepted

## Decision

Workers claim tasks with expiring leases. Every claim receives a new attempt number and lease token. Mutations from a handler require both values.

## Consequences

The engine recovers automatically from worker loss and rejects stale database writes. Handlers may overlap briefly after a lease expires and must make external side effects idempotent.
