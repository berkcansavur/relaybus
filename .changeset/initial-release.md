---
'@relaybus/core': minor
'@relaybus/nestjs': minor
'@relaybus/postgres': minor
'@relaybus/mongodb': minor
'@relaybus/kafka': minor
'@relaybus/rabbitmq': minor
'@relaybus/testing': minor
---

Initial release: transactional outbox event bus with per-handler deliveries, leased dispatching,
retries with jitter, dead letters and ordering keys; PostgreSQL and MongoDB stores; Kafka and
RabbitMQ transports; NestJS module; store contract suite.
