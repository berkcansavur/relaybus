# @relaybus/otel

@relaybus/otel is the OpenTelemetry integration of relaybus, a reliable, storage-agnostic event bus for NestJS and Node.js: messaging metrics after the OpenTelemetry semantic conventions, outbox backlog gauges, and one trace from `publish` in one service to the handler in another.

```sh
pnpm add @relaybus/otel @opentelemetry/api
```

It depends only on `@opentelemetry/api` (peer, `^1.3.0`); the SDK, exporters and collector are yours.

```ts
import { relayBusOpenTelemetry } from '@relaybus/otel';

const otel = relayBusOpenTelemetry(); // global meter and tracer providers, W3C propagation

const bus = new RelayBus({
  store,
  observers: [otel.observer], // metrics
  tracer: otel.tracer, // spans + traceparent/tracestate in event headers
  backlogInterval: '30s', // feeds the backlog gauges
});
```

In NestJS pass the same three options to `RelayBusModule.forRoot`.

| Option           | Default                      | What it does                                                                                              |
| ---------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------- |
| `meterProvider`  | `metrics.getMeterProvider()` | Where the metrics are created.                                                                            |
| `tracerProvider` | `trace.getTracerProvider()`  | Where the spans are created.                                                                              |
| `propagator`     | W3C Trace Context (built in) | Writes and reads the trace context of events. Pass `propagation` from `@opentelemetry/api` for the SDK's. |

## Traces

| Span                   | Kind     | Parent                                               | In which process                     |
| ---------------------- | -------- | ---------------------------------------------------- | ------------------------------------ |
| `publish <event type>` | producer | the caller's active span                             | the publisher, inside `publish`      |
| `send <destination>`   | producer | the publish span (from the stored event), linked     | the publisher, when a route sends it |
| `process <event type>` | consumer | the context the event arrived with (send or publish) | the consumer, around the handler     |

The publish span's context is stored with the event (`traceparent`, `tracestate` headers), so the
trace survives the outbox, retries and restarts. A route sends the event with the send span's
context; the consumer's `process` span continues from it, and whatever the handler does or
publishes joins that span. Without a tracer the headers are still readable with `traceContextOf`
from `@relaybus/core`.

## Metrics

Durations are in seconds. `error.type` is the relaybus error code (`RELAYBUS_*`) when there is one,
else the error's class name; it is present only on failures.

| Metric                                | Type      | Unit         | Attributes                                                                                                                                                     |
| ------------------------------------- | --------- | ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `messaging.client.operation.duration` | histogram | `s`          | `messaging.system`, `messaging.operation.name` (`publish`, `claim`, `send`, `receive`), `messaging.operation.type`, `messaging.destination.name`, `error.type` |
| `messaging.client.sent.messages`      | counter   | `{message}`  | `messaging.system`, `messaging.destination.name`, `messaging.operation.name`, `messaging.operation.type`, `error.type`                                         |
| `messaging.client.consumed.messages`  | counter   | `{message}`  | as above, plus `relaybus.receive.outcome` (`accepted`, `duplicate`, `ignored`, `invalid`, `error`)                                                             |
| `messaging.process.duration`          | histogram | `s`          | `messaging.system`, `messaging.destination.name` (event type), `messaging.consumer.group.name` (handler id), `messaging.operation.*`, `error.type`             |
| `relaybus.messages.invalid`           | counter   | `{message}`  | `messaging.system`, `messaging.destination.name`, `error.type` (inbound messages a transport could not decode; they never reach `receive`)                     |
| `relaybus.events.published`           | counter   | `{event}`    | `messaging.system`, `messaging.destination.name` (event type), `error.type`                                                                                    |
| `relaybus.claim.deliveries`           | histogram | `{delivery}` | `messaging.system`, `messaging.operation.*`                                                                                                                    |
| `relaybus.claim.lanes`                | histogram | `{lane}`     | `messaging.system`, `messaging.operation.*`                                                                                                                    |
| `relaybus.end_to_end.latency`         | histogram | `s`          | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `relaybus.delivery.kind` (`handler`, `route`)                               |
| `relaybus.deliveries.retried`         | counter   | `{delivery}` | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `error.type`                                                                |
| `relaybus.deliveries.dead_lettered`   | counter   | `{delivery}` | as above                                                                                                                                                       |
| `relaybus.lease.lost`                 | counter   | `{delivery}` | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`                                                                              |
| `relaybus.deliveries.pending`         | gauge     | `{delivery}` | none (needs `backlogInterval`)                                                                                                                                 |
| `relaybus.deliveries.oldest_age`      | gauge     | `s`          | none (needs `backlogInterval`)                                                                                                                                 |
| `relaybus.failures`                   | counter   | `{failure}`  | `relaybus.failure.stage` (`claim`, `complete`, `purge`, `backlog`, ...), `error.type`                                                                          |

`messaging.system` is `relaybus` for work on the outbox (publish, claim, process) and the
transport's system (`kafka`, `rabbitmq`) for sends and receives. The full guide, with a collector,
Prometheus and alert rules, is the [Observability](https://berkcansavur.github.io/relaybus/concepts/observability)
page.

## License

MIT
