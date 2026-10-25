---
title: Leases and fencing - safe competing consumers in relaybus
description: How relaybus workers claim deliveries with SKIP LOCKED or compare-and-set leases, renew them while handlers run, and fence every state change with the lease token.
---

# Leases and fencing

Several processes can run the dispatcher at once. They compete for due deliveries using **leases**.

## Claiming

- **PostgreSQL**: one statement chooses candidates with `FOR UPDATE SKIP LOCKED` and leases them (`processing`, `attempts + 1`, a new lease token).
- **MongoDB**: compare-and-set updates claim a delivery atomically.

A lease lasts 30 seconds by default (`dispatcher.leaseDuration`) and is renewed every third of that while the handler runs.

## Crashes and takeover

A worker that crashes leaves an expired lease. Any other worker claims the delivery after it expires. If the handler outlives its lease, the handler's `context.signal` is aborted; the retry waits for the abandoned run to settle, so runs never overlap.

## Fencing

`complete`, `reschedule`, `bury`, `release` and `extendLease` apply only when the delivery is `processing` **and** the lease token matches. A worker that lost its lease changes nothing, so a stalled worker cannot overwrite the worker that took over.

## Transactional handlers

With `transactional: true` the handler runs in a store transaction that also completes the delivery. Its writes through `context.transaction` and the completion commit together, which gives effectively-once processing for database effects. Such a handler must finish within the lease duration or its `timeout`.

## Shutdown

On `stop`, the dispatcher stops claiming and waits for running handlers for four fifths of the stop timeout (`stop({ timeout })`, default `dispatcher.shutdownTimeout`, 30 seconds), then aborts the rest and releases them without consuming an attempt. The whole stop, closing transports and the store included, ends at the timeout: deliveries still in flight then (their release did not answer) are abandoned with their lease, which expires after `leaseDuration`, and another process runs them again. At-least-once holds either way.

## Related

- [Deliveries](/concepts/deliveries)
- [Guarantees](/guarantees)
