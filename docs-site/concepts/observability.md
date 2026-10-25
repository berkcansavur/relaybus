---
title: Observability - OpenTelemetry metrics and traces for relaybus
description: Messaging metrics after the OpenTelemetry semantic conventions, outbox backlog gauges and one trace from publish in one service to the handler in another, with a collector, Prometheus and alert rules.
---

# Observability

relaybus reports what it does through observer hooks and a tracing hook in `@relaybus/core`. Core
depends on no telemetry library; `@relaybus/otel` maps both to OpenTelemetry.

```sh
pnpm add @relaybus/otel @opentelemetry/api
```

```ts
import { relayBusOpenTelemetry } from '@relaybus/otel';

const otel = relayBusOpenTelemetry(); // global meter and tracer providers

const bus = new RelayBus({
  store,
  observers: [otel.observer], // metrics
  tracer: otel.tracer, // spans, traceparent/tracestate in event headers
  backlogInterval: '30s', // pending and oldest-age gauges
});
```

`RelayBusModule.forRoot` takes the same three options. Register the OpenTelemetry SDK (for example
`@opentelemetry/sdk-node`) before the bus is created, or pass `meterProvider` and `tracerProvider`.

## Traces

One trace runs from the request that publishes an event to the handler in the service that
consumes it, through the outbox and the broker:

```
POST /orders                      (orders service, your span)
└─ publish order.placed.v1        producer   stored with the event as traceparent
   └─ send orders.placed          producer   linked to the publish span; its context goes on the wire
      └─ process order.placed.v1  consumer   billing service, around the handler
         └─ publish invoice.created.v1        what the handler publishes joins the trace
```

| Span                   | Kind     | Parent                                                          |
| ---------------------- | -------- | --------------------------------------------------------------- |
| `publish <event type>` | producer | the caller's active span                                        |
| `send <destination>`   | producer | the publish span, from the stored event's headers (also a link) |
| `process <event type>` | consumer | the context the event arrived with: the send span, else publish |

- The publish span's context is written into the event's `traceparent` and `tracestate` headers
  inside the publishing transaction, so it survives the outbox, retries and restarts. Headers you
  pass to `publish` yourself win.
- A route sends the event with the send span's context instead; the stored event is not changed.
- A local handler runs inside its `process` span, so database calls, HTTP calls and events it
  publishes become its children. A retry is a new `process` span in the same trace
  (`relaybus.delivery.attempt`).
- Without `@relaybus/otel`, `traceContextOf(event.headers)` from `@relaybus/core` returns the
  W3C context an event carries, for any other tracer.

Span attributes: `messaging.system` (`relaybus` for the outbox, the transport's system such as
`kafka` or `rabbitmq` for sends), `messaging.destination.name`, `messaging.operation.name`,
`messaging.operation.type`, `messaging.message.id`, `messaging.message.conversation_id` (the
correlation id), `messaging.consumer.group.name` (the handler id), `messaging.batch.message_count`,
`relaybus.event.type`, `relaybus.delivery.attempt`, and on failure `error.type` with the
exception recorded and the status set to error.

The default propagator is W3C Trace Context, built in. Pass `propagator: propagation` (from
`@opentelemetry/api`) to use the one the SDK configured, for example to add baggage.

## Metrics

Names follow the [OpenTelemetry semantic conventions for messaging](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
where one exists. Durations are in seconds. `error.type` is the relaybus error code
(`RELAYBUS_LEASE_LOST`, see [Error codes](/concepts/error-codes)) or the error's class name, and
is present only on failures.

| Metric                                | Type      | Unit         | Recorded                                      | Attributes                                                                                                               |
| ------------------------------------- | --------- | ------------ | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `messaging.client.operation.duration` | histogram | `s`          | each publish, claim, send and receive         | `messaging.system`, `messaging.operation.name`, `messaging.operation.type`, `messaging.destination.name`, `error.type`   |
| `messaging.client.sent.messages`      | counter   | `{message}`  | each send attempt of a route                  | `messaging.system`, `messaging.destination.name`, `messaging.operation.name`, `messaging.operation.type`, `error.type`   |
| `messaging.client.consumed.messages`  | counter   | `{message}`  | each event received from a broker             | as above, plus `relaybus.receive.outcome`                                                                                |
| `messaging.process.duration`          | histogram | `s`          | each local handler run                        | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `messaging.operation.*`, `error.type` |
| `relaybus.events.published`           | counter   | `{event}`    | each event passed to `publish`                | `messaging.system`, `messaging.destination.name`, `error.type`                                                           |
| `relaybus.claim.deliveries`           | histogram | `{delivery}` | deliveries per claim query                    | `messaging.system`, `messaging.operation.*`                                                                              |
| `relaybus.claim.lanes`                | histogram | `{lane}`     | distinct leases (lanes) per claim query       | `messaging.system`, `messaging.operation.*`                                                                              |
| `relaybus.end_to_end.latency`         | histogram | `s`          | from `occurredAt` to a handler or route start | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `relaybus.delivery.kind`              |
| `relaybus.deliveries.retried`         | counter   | `{delivery}` | each failed attempt rescheduled               | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `error.type`                          |
| `relaybus.deliveries.dead_lettered`   | counter   | `{delivery}` | each delivery moved to dead                   | as above                                                                                                                 |
| `relaybus.lease.lost`                 | counter   | `{delivery}` | each lease lost while a delivery ran          | `messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`                                        |
| `relaybus.deliveries.pending`         | gauge     | `{delivery}` | last backlog reading (`backlogInterval`)      | none                                                                                                                     |
| `relaybus.deliveries.oldest_age`      | gauge     | `s`          | last backlog reading (`backlogInterval`)      | none                                                                                                                     |
| `relaybus.failures`                   | counter   | `{failure}`  | each error relaybus caught and reported       | `relaybus.failure.stage`, `error.type`                                                                                   |

| Attribute                       | Values                                                                                                                         |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `messaging.system`              | `relaybus` for publish, claim and handlers; the transport's `system` (`kafka`, `rabbitmq`) or name                             |
| `messaging.destination.name`    | the event type; for sends and receives the topic, routing key or destination                                                   |
| `messaging.operation.name`      | `publish`, `claim`, `send`, `receive`, `process`                                                                               |
| `messaging.operation.type`      | `create`, `receive`, `send`, `process`                                                                                         |
| `messaging.consumer.group.name` | the handler id (a route's id is `relaybus.transport:<transport>:<destination>`)                                                |
| `relaybus.delivery.kind`        | `handler` or `route`                                                                                                           |
| `relaybus.receive.outcome`      | `accepted`, `duplicate`, `ignored`, `invalid`, `error`                                                                         |
| `relaybus.failure.stage`        | `claim`, `dispatch`, `complete`, `reschedule`, `bury`, `release`, `extend-lease`, `purge`, `watch`, `backlog`, `observer`, ... |
| `error.type`                    | a relaybus error code, else the error class name                                                                               |

The backlog gauges need `backlogInterval`: each reading is one `statistics()` query, so it is off
by default. Read it in one process per database if many share it, or accept the duplicate series.

The metrics come from observer hooks you can also implement yourself: `onPublishFinished`,
`onClaimFinished`, `onHandlerStarted`, `onHandlerFinished`, `onTransportSent`,
`onReceiveFinished`, `onInvalidMessage`, `onBacklog`, next to the lifecycle hooks (`onDeliveryRetryScheduled`,
`onDeliveryDead`, `onLeaseLost`, `onFailure`, ...). A hook that no observer implements costs
nothing, not even a clock read.

## Prometheus and Grafana through the OpenTelemetry Collector

Export OTLP from the service and let the collector expose Prometheus metrics and forward traces:

```ts
// telemetry.ts, loaded before the application
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';

new NodeSDK({
  serviceName: 'orders',
  traceExporter: new OTLPTraceExporter(),
  metricReader: new PeriodicExportingMetricReader({ exporter: new OTLPMetricExporter() }),
}).start();
```

```yaml
# otel-collector.yaml
receivers:
  otlp:
    protocols:
      http:
exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
  otlp/tempo:
    endpoint: tempo:4317
    tls:
      insecure: true
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
    traces:
      receivers: [otlp]
      exporters: [otlp/tempo]
```

Prometheus scrapes `otel-collector:8889`. The collector's Prometheus exporter turns dots into
underscores and adds unit and counter suffixes, so the series are named
`relaybus_deliveries_oldest_age_seconds`, `relaybus_deliveries_dead_lettered_total`,
`messaging_process_duration_seconds_bucket` and so on, with `service_name` from the resource.

Useful Grafana panels:

```promql
# Handler p95 by handler
histogram_quantile(0.95, sum by (le, messaging_consumer_group_name) (rate(messaging_process_duration_seconds_bucket[5m])))

# End-to-end latency p99 across services
histogram_quantile(0.99, sum by (le, messaging_destination_name) (rate(relaybus_end_to_end_latency_seconds_bucket[5m])))

# Backlog
max by (service_name) (relaybus_deliveries_pending)

# Send failures by broker destination
sum by (messaging_destination_name, error_type) (rate(messaging_client_sent_messages_total{error_type!=""}[5m]))
```

## Alerts

Start with these two; they catch a stuck dispatcher, a dead broker and a failing handler.

```yaml
groups:
  - name: relaybus
    rules:
      - alert: RelayBusBacklogStale
        # The oldest due delivery has waited 5 minutes: nothing is draining the outbox
        # (dispatcher down, broker unreachable for a route, a key blocked by a retrying event).
        expr: max by (service_name) (relaybus_deliveries_oldest_age_seconds) > 300
        for: 5m
        labels: { severity: page }
      - alert: RelayBusDeadLetters
        # Deliveries are giving up. Inspect with bus.listDeliveries({ status: 'dead' }) and requeue.
        expr: sum by (service_name, messaging_consumer_group_name, error_type) (increase(relaybus_deliveries_dead_lettered_total[15m])) > 0
        labels: { severity: ticket }
```

Further candidates, tuned to your traffic: a rising `rate(relaybus_deliveries_retried_total[5m])`
(a dependency is failing before deliveries die), `relaybus_lease_lost_total` above zero (handlers
outliving `dispatcher.leaseDuration`, or clock skew), `relaybus_failures_total` by
`relaybus_failure_stage` (store errors), and
`relaybus_end_to_end_latency_seconds` p99 against your latency objective.
