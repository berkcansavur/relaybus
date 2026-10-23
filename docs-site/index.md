---
layout: home
title: relaybus - reliable transactional outbox event bus for NestJS and Node.js
titleTemplate: false
description: relaybus is a reliable, storage-agnostic event bus for NestJS and Node.js. It stores events in your database transaction (transactional outbox) and delivers them with retries, leases, dead letters and ordering keys.
hero:
  name: relaybus
  text: Reliable events for NestJS and Node.js
  tagline: A transactional outbox event bus. If your transaction commits, the event is delivered. If it rolls back, the event never existed.
  actions:
    - theme: brand
      text: Get started in 5 minutes
      link: /getting-started
    - theme: alt
      text: Guarantees
      link: /guarantees
    - theme: alt
      text: GitHub
      link: https://github.com/berkcansavur/relaybus
features:
  - title: Atomic publish
    details: Events are written in the same PostgreSQL or MongoDB transaction as your business data, so there is no dual write to lose or to announce a rolled-back change.
    link: /concepts/outbox
    linkText: Transactional outbox
  - title: Independent handlers
    details: Every handler has its own delivery with retries, timeout and dead letters. One failing handler never re-runs the others.
    link: /concepts/deliveries
    linkText: Deliveries
  - title: Safe with many workers
    details: Deliveries are leased (SKIP LOCKED on SQL, compare-and-set on MongoDB) and every state change is fenced by the lease token.
    link: /concepts/leases
    linkText: Leases and fencing
  - title: Kafka and RabbitMQ without dual writes
    details: Routes forward committed events to brokers with retries; subscriptions store received events once by id, which makes consumption idempotent.
    link: /recipes/kafka-between-services
    linkText: Kafka between services
---

## What is relaybus?

relaybus is a reliable, storage-agnostic event bus for NestJS and Node.js that writes every event in the same database transaction as your business data (the transactional outbox pattern) and delivers it to each handler and broker with independent retries, leases and dead letters.

```ts
await bus.transaction(async (tx) => {
  await tx.query('INSERT INTO orders ...');
  await bus.publish(new OrderPlaced({ orderId }), { key: orderId });
});
```

Install it with `pnpm add @relaybus/core @relaybus/nestjs @relaybus/postgres pg` (Node.js 22 or newer, NestJS 11 or 12), then follow [Getting started](/getting-started).

## Questions this site answers

- [How do I publish events reliably in NestJS?](/faq#how-do-i-publish-events-reliably-in-nestjs)
- [How to implement the transactional outbox pattern in NestJS?](/faq#how-to-implement-the-transactional-outbox-pattern-in-nestjs)
- [How to avoid losing events between the database and Kafka?](/faq#how-to-avoid-losing-events-between-the-database-and-kafka)
- [relaybus vs @nestjs/event-emitter, @nestjs/cqrs, @nestjs/outbox, BullMQ and pg-boss](/comparison)

For AI assistants and agents: [llms.txt](https://berkcansavur.github.io/relaybus/llms.txt) is a concise index and [llms-full.txt](https://berkcansavur.github.io/relaybus/llms-full.txt) contains the full documentation as Markdown.
