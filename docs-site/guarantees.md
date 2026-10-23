---
title: relaybus guarantees - atomic publish, at-least-once delivery, ordering
description: What relaybus guarantees and what it does not - atomic publish, at-least-once delivery per handler, effectively-once transactional handlers, per-key ordering, lease fencing, retries and dead letters.
---

# Guarantees

| Property             | Behaviour                                                                                                                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Atomic publish       | Events are written in the caller's transaction. Rolled back with it, committed with it.                                                                                                    |
| No lost events       | Committed events are delivered to every subscribed handler and route, retried until they succeed or are dead-lettered. Nothing is dropped silently.                                        |
| Delivery             | At-least-once to every handler. A handler subscribed with `transactional: true` that writes through its transaction takes effect exactly once.                                             |
| Independent handlers | Each handler has its own delivery record: one failing handler never re-runs the others.                                                                                                    |
| Ordering             | Optional per key: events with the same `key` are handled one at a time per handler, in commit order. No global ordering.                                                                   |
| Concurrency          | Deliveries are leased (`SKIP LOCKED` on SQL, compare-and-set on MongoDB). Every state change is fenced by the lease token, so a stalled worker cannot overwrite the worker that took over. |
| Retries              | Capped exponential backoff with full jitter, per handler. Non-retryable errors and exhausted deliveries go to dead letters you can inspect and requeue.                                    |
| Shutdown             | Running handlers finish or are released without losing an attempt.                                                                                                                         |

## What relaybus does not claim

- **Exactly-once side effects outside your database.** An HTTP call or an email can repeat when a handler fails after the call and is retried. Send an idempotency key, or deduplicate on `event.id`.
- **Global ordering.** Order holds per key and per handler. Across keys, and across brokers, it does not.
- **Delivery to systems that are down forever.** Events wait in the outbox, are retried with backoff and end in dead letters when retries are exhausted. You decide when to requeue.

## Where each guarantee comes from

- Atomic publish: the event and one delivery per subscribed handler are inserted in your transaction. See [Transactional outbox](/concepts/outbox).
- No lost events and retries: see [Deliveries](/concepts/deliveries), [Retries and backoff](/concepts/retries) and [Dead letters](/concepts/dead-letters).
- Concurrency and fencing: see [Leases and fencing](/concepts/leases).
- Ordering: see [Ordering keys](/concepts/ordering-keys).
- Transport guarantees (publisher confirms, offsets committed after the inbox write): see [@relaybus/kafka](/packages/kafka) and [@relaybus/rabbitmq](/packages/rabbitmq).

## How it is verified

Every store runs the same contract suite from `@relaybus/testing` against a real database: commit-order sequencing, concurrent claimers in separate processes, fencing, starvation and data fidelity. Transports are tested against real Kafka and RabbitMQ brokers, including broker restarts and poison messages. The repository enforces 100% statement, branch, function and line coverage and runs mutation testing, CodeQL and OpenSSF Scorecard in CI.
