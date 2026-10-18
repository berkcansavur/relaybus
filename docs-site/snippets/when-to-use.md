**When to use relaybus**

- A NestJS or Node.js service writes to PostgreSQL or MongoDB and must announce those changes without ever losing an event or announcing a change that rolled back.
- You want several handlers to react to one event, each with its own retries, timeout and dead letters, without one failing handler re-running the others.
- You need to forward events to Kafka or RabbitMQ without a dual write between the database and the broker.
- You want per-key ordering (for example per order or per account) and operational tools: dead letters you can list and requeue, statistics and a health check.

**When not to use relaybus**

- You only need in-process notifications and losing an event on restart is acceptable: [`@nestjs/event-emitter`](https://docs.nestjs.com/techniques/events) is simpler.
- Your stack has no PostgreSQL or MongoDB database to hold the outbox (for example Redis only, or MySQL): look at BullMQ, or [`@nestjs/outbox`](https://github.com/nestjs/outbox) for MySQL, or implement the `EventStore` interface yourself.
- You need global ordering, or exactly-once side effects outside your own database (an HTTP call can repeat; send an idempotency key). relaybus does not claim either.
- You need cron-style recurring jobs or a general work queue: a job queue such as BullMQ or pg-boss fits that model better.
