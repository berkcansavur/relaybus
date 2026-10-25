---
title: Interoperating with services that do not use relaybus - Kafka and RabbitMQ codecs
description: Exchange events between relaybus and services in Java, Go, .NET, Spring, MassTransit or plain kafkajs/amqplib. One codec option sets the wire format both ways - CloudEvents binary or structured mode, a mapped JSON envelope, or your own - with exactly-once handling and dead-lettering of malformed input.
---

# Interoperating with non-relaybus services

relaybus only needs to run on one side of a topic or queue. The `codec` option of the Kafka and RabbitMQ transports sets the wire format in both directions: how events are written when relaybus publishes, and how messages are read when it consumes. Messages written by other services get the same guarantees as relaybus's own: stored once by id, handled once, malformed input dead-lettered while the consumer keeps running.

| Other side writes or reads                       | `codec`                                             |
| ------------------------------------------------ | --------------------------------------------------- |
| CloudEvents binary mode, or any client           | `cloudEventsBinary` (default, nothing to configure) |
| CloudEvents structured mode (`cloudevents+json`) | `cloudEventsStructured`                             |
| Its own JSON envelope around the payload         | `jsonEnvelope({ type, id, payload, ... })`          |
| The bare JSON payload, metadata in headers       | `jsonEnvelope({ type, id, payload: 'whole-body' })` |
| Avro, Protobuf, anything else                    | `{ encode, decode }`, your own codec                |

```ts
import { KafkaTransport, cloudEventsStructured } from '@relaybus/kafka';

new KafkaTransport({ kafka, groupId: 'billing', codec: cloudEventsStructured });
```

The built-ins have the same names in `@relaybus/kafka` and `@relaybus/rabbitmq`. One transport is one subscription, so a service that speaks two formats uses two transports (each with its own `name`), or one codec whose `decode` switches on `record.topic` (Kafka) or `message.fields.routingKey` (RabbitMQ).

## CloudEvents binary mode, the default

The payload is the JSON body, UTF-8. The attributes travel as headers.

| Attribute            | Kafka header                                            | RabbitMQ header (and property)                |
| -------------------- | ------------------------------------------------------- | --------------------------------------------- |
| spec version         | `ce_specversion: 1.0`                                   | `cloudEvents:specversion: 1.0`                |
| event id             | `ce_id`                                                 | `cloudEvents:id` (`messageId`)                |
| event type           | `ce_type`                                               | `cloudEvents:type` (`type`)                   |
| source service       | `ce_source`                                             | `cloudEvents:source` (`appId`)                |
| time (RFC 3339, UTC) | `ce_time`                                               | `cloudEvents:time` (`timestamp`, seconds)     |
| ordering key         | `ce_subject` and the record key                         | `cloudEvents:subject`                         |
| content type         | `content-type: application/json`                        | `cloudEvents:datacontenttype` (`contentType`) |
| correlation id       | `ce_correlationid`                                      | `cloudEvents:correlationid` (`correlationId`) |
| causation id         | `ce_causationid`                                        | `cloudEvents:causationid`                     |
| your event headers   | as is (`x-relaybus-h-` prefix only on a name collision) | `relaybus:<name>`                             |

Reading is strict: anything that is not binary mode is malformed. Reading it without relaybus:

- **Plain kafkajs, Go, Python**: `JSON.parse(record.value)`, type and id from the `ce_type` and `ce_id` headers.
- **Java**: `io.cloudevents:cloudevents-kafka` `CloudEventDeserializer` reads the record as a `CloudEvent`. Spring Cloud Function and Spring Cloud Stream recognise the `ce_` headers.
- **Go**: `cloudevents/sdk-go` with `protocol/kafka_sarama`.
- **RabbitMQ (any AMQP 0-9-1 client, Spring AMQP)**: bind a queue to the exchange with the routing key, parse the body, read `messageId` and `type` from the properties or the `cloudEvents:*` headers.

```java
// Spring Kafka consumer of a relaybus topic, no relaybus on this side.
@KafkaListener(topics = "orders.events")
void onOrder(@Payload String json, @Header("ce_type") String type, @Header("ce_id") String id) {
  if (processedIds.add(id) && type.equals("order.placed.v1")) {
    handle(objectMapper.readValue(json, OrderPlaced.class));
  }
}
```

## CloudEvents structured mode

CloudEvents SDKs often send one JSON document with the attributes and `data`, with `content-type: application/cloudevents+json` (Kafka header, RabbitMQ `contentType` property):

```json
{
  "specversion": "1.0",
  "id": "ce-5",
  "type": "order.placed.v1",
  "source": "//go/orders",
  "time": "2026-05-01T10:00:00Z",
  "subject": "order-5",
  "datacontenttype": "application/json",
  "data": { "orderId": "5" }
}
```

`codec: cloudEventsStructured` writes this document (the key as `subject`, correlation and causation ids as the `correlationid` and `causationid` extension attributes, event headers as message headers) and reads both structured and binary mode, so a topic can move from one mode to the other while both are in flight. `data` must be JSON (`data_base64` is refused); string extension attributes such as `traceparent` become event headers. On RabbitMQ the AMQP properties are those of binary mode, and event header names starting with `x-death`, `x-first-death-`, `x-last-death-` or `x-relaybus-` are refused, because readers drop them.

## A JSON envelope of your own

`jsonEnvelope(mapping)` declares where each value lives: a dotted body path (`{ body: 'meta.correlationId' }`, a whole number indexes an array: `'messageType.0'`) or a header (`{ header: 'event-type' }`). The codec writes and reads the same mapping, so relaybus and the other services can both produce and consume.

| Field           | Required | Where                                                 | When not mapped                                                                                |
| --------------- | -------- | ----------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `type`          | yes      | body, header, or `{ fixed: 'order.placed.v1' }`       |                                                                                                |
| `id`            | yes      | body, header, or the transport's own id (below)       |                                                                                                |
| `payload`       | yes      | body path, or `'whole-body'`                          |                                                                                                |
| `key`           | no       | body or header                                        | Kafka: the record key (written unless `recordKey` says otherwise). RabbitMQ: none.             |
| `source`        | no       | body or header                                        | Kafka: the topic. RabbitMQ: the `appId` property (always written), else the routing key.       |
| `occurredAt`    | no       | body or header, RFC 3339 (or `format`, below)         | Kafka: the record timestamp (written from `occurredAt`). RabbitMQ: `timestamp`, whole seconds. |
| `correlationId` | no       | body or header                                        | null                                                                                           |
| `causationId`   | no       | body or header                                        | null                                                                                           |
| `headers`       | no       | body path of an object, or `'message-headers'`        | `'message-headers'`: every message header no other field maps                                  |
| `contentType`   | no       | written as the content type                           | `application/json`                                                                             |
| `recordKey`     | no       | Kafka: `'event-key'`, `'none'` or `(event) => key`    | `'event-key'`; `contentTypeHeader: false` also drops the `content-type` header                 |
| `extra`         | no       | `{ body: { path: value }, headers: { name: value } }` | nothing extra; values are constants or `(event) => value`, written only, never read            |
| `bodyOrder`     | no       | top-level body field names                            | fields in the order they are written; set it when the other side compares bytes                |

The types make an incomplete mapping a compile error: `type`, `id` and `payload` are required, and with `payload: 'whole-body'` every other field must be a header, because the body is the payload. The id strategy is always explicit, because it is what the inbox deduplicates on:

| Transport id                     | Written                       | Read                                                                                                                                  |
| -------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Kafka `'record-position'`        | a `relaybus-event-id` header  | that header when present (records relaybus wrote), else `kafka:<topic>:<partition>:<offset>`, stable for every redelivery of a record |
| RabbitMQ `'message-id-property'` | the `messageId` property      | the `messageId` property, kept on redeliveries; a message without one is malformed                                                    |
| `{ body }` or `{ header }`       | the event id at that location | the value there; a message without one is malformed                                                                                   |

A producer that may send one event twice writes it at two Kafka offsets, so only a producer id deduplicates that; prefer `{ body }` or `{ header }` when the other side sets an id. relaybus itself writes the event id in `relaybus-event-id` under `'record-position'`, so its own outbox retries stay deduplicated.

A value that is neither mapped nor carried by the transport (see the table) does not survive the trip: on RabbitMQ an unmapped key reads as none and an unmapped `occurredAt` keeps whole seconds. Map every value both sides need.

`occurredAt: { body: 'timestamp', format: 'epoch-millis' }` writes whole epoch milliseconds (a number in the body, decimal text in a header) and reads them back; `'epoch-seconds'` drops the milliseconds. `extra` writes what in-house formats carry besides the event: a constant `provider: 'orders-api'`, a second copy of the time (`createdDate: (event) => event.occurredAt.toISOString()`) or a schema-version header. Extra values are never read, extra headers are left out of the event headers on read, and an extra value at a mapped location is a configuration error. A payload-only mapping takes extra headers only.

On write, event headers that travel as message headers keep their names, so a name the mapping uses for another field is refused with a non-retryable error (`UnencodableEventError` on Kafka, `RabbitMqUnencodableEventError` on RabbitMQ), as is `content-type` on Kafka. A `{ fixed }` type refuses events of any other type the same way: such a route carries one type only. On read, a body field holding a number is read as its decimal text when it is a safe integer, so numeric ids work.

### A body envelope, as service monorepos use it

```json
{
  "eventName": "order.placed.v1",
  "messageId": "0190a000-0000-7000-8000-0000000000aa",
  "data": { "orderId": "42" },
  "meta": { "correlationId": "flow-1", "occurredAt": "2026-05-01T10:00:00.000Z", "headers": {} }
}
```

```ts
import { KafkaTransport, jsonEnvelope } from '@relaybus/kafka';

const transport = new KafkaTransport({
  kafka,
  groupId: 'billing',
  codec: jsonEnvelope({
    type: { body: 'eventName' },
    id: { body: 'messageId' },
    payload: { body: 'data' },
    correlationId: { body: 'meta.correlationId' },
    causationId: { body: 'meta.causationId' },
    occurredAt: { body: 'meta.occurredAt' },
    headers: { body: 'meta.headers' },
  }),
});
```

### Schema validation

Payloads read by any codec go through the event's schema exactly like relaybus's own: the handler of an event defined with `defineEvent(type, { schema })` gets the validated value, and a payload that fails the schema makes the delivery dead (`EventValidationError`, code `RELAYBUS_EVENT_VALIDATION`, visible through `onDeliveryDead` and the store's dead deliveries) while the consumer keeps running. Any Standard Schema library works, for example zod:

```ts
import { z } from 'zod';
import { defineEvent } from '@relaybus/core';

export class OrderPlaced extends defineEvent('order.placed.v1', {
  schema: z.object({ orderId: z.string(), total: z.number() }),
}) {}

// The `data` of { "eventName": "order.placed.v1", "messageId": ..., "data": { "orderId": "42", "total": 12.5 } }
// reaches the handler only when it matches the schema.
bus.subscribe(OrderPlaced, async (event) => charge(event.payload.orderId, event.payload.total), {
  id: 'billing.charge',
});
```

Publishing validates before the codec runs, so an invalid payload is rejected by `publish` and never written.

### MassTransit

MassTransit's JSON envelope carries the type as a list of message URNs and the payload under `message`, with content type `application/vnd.masstransit+json`. Subscribe to the URN as the event type (`urn:message:Orders.Contracts:OrderPlaced` is a valid relaybus type), or map it in your own codec:

```ts
import { RabbitMqTransport, jsonEnvelope } from '@relaybus/rabbitmq';

const transport = new RabbitMqTransport({
  url,
  consumerGroup: 'billing',
  codec: jsonEnvelope({
    type: { body: 'messageType.0' },
    id: { body: 'messageId' },
    payload: { body: 'message' },
    correlationId: { body: 'correlationId' },
    causationId: { body: 'initiatorId' },
    occurredAt: { body: 'sentTime' },
    headers: { body: 'headers' },
    contentType: 'application/vnd.masstransit+json',
  }),
});
```

relaybus writes only the mapped fields (`messageType` as a one-element list); `conversationId`, `sourceAddress`, `host` and the other MassTransit fields are not written and ignored on read. MassTransit publishes to one fanout exchange per message type and relaybus to one topic exchange: connect them with an exchange-to-exchange binding in your broker setup.

### The bare JSON payload

A plain kafkajs, Spring `JsonSerializer` or Go producer that writes only the payload, with the type in a header:

```ts
import { KafkaTransport, jsonEnvelope } from '@relaybus/kafka';

const transport = new KafkaTransport({
  kafka,
  groupId: 'billing',
  codec: jsonEnvelope({
    type: { header: 'event-type' }, // or { fixed: 'order.placed.v1' } for a one-type topic
    id: 'record-position', // or { header: 'message-id' } when the producer sets one
    payload: 'whole-body',
  }),
});
```

On RabbitMQ, use `id: 'message-id-property'` (Spring AMQP sets `messageId` with `setCreateMessageIds(true)`). The remaining message headers become event headers.

## Your own codec

```ts
import type { KafkaCodec } from '@relaybus/kafka';

const legacyOrders: KafkaCodec = {
  encode: (event) => ({ key: event.key, value: encodeLegacyOrder(event.payload), headers: {} }),
  decode: (record) => {
    const order = decodeLegacyOrder(record.value); // synchronous and deterministic
    return { id: order.reference, type: 'order.placed.v1', payload: order };
  },
};
```

`decode` may leave out `source`, `key`, `occurredAt`, `headers`, `correlationId` and `causationId`; the transport fills them as in the table above. Both functions must be synchronous and free of side effects: `decode` runs again on every redelivery and must return the same id. On RabbitMQ, `encode` returns `{ content, options }` and should set `options.messageId`, which the transport uses to match unroutable returns. What `encode` throws fails the publish, and the outbox retries it unless it is a `NonRetryableError`.

### A legacy flat envelope without keys or headers

Some topics carry many event types as one flat JSON object, written without a record key and without headers, with the event headers spread over the top level:

```json
{
  "id": "evt-1",
  "requestId": "req-1",
  "provider": "billing-api",
  "timestamp": 1792936800123,
  "createdDate": "2026-10-25T14:00:00.123Z",
  "type": "order.created",
  "data": { "orderId": 1 },
  "userId": "u-7"
}
```

```ts
codec: jsonEnvelope({
  type: { body: 'type' },
  id: { body: 'id', whenMissing: 'record-position' }, // older records have no id
  payload: { body: 'data' },
  source: { body: 'provider' },
  occurredAt: { body: 'timestamp', format: 'epoch-millis' },
  correlationId: { body: 'requestId' },
  headers: 'top-level-fields', // event headers spread over the top level, read back from it
  extra: { body: { createdDate: (event) => event.occurredAt.toISOString() } },
  bodyOrder: ['id', 'requestId', 'provider', 'timestamp', 'createdDate', 'type', 'data'],
  recordKey: 'none',
  contentTypeHeader: false,
}),
```

`headers: 'top-level-fields'` reads every top-level text field that no other value uses as an event header; other unknown fields (numbers, objects) are accepted and left out. A record without a key reads as an event without a key. relaybus still keeps the outbox order of one event key, but keyless records are spread over partitions by the producer, so consumers see events of one key in order only when the key reaches the wire.

## Prove the format with captured messages

Before relaybus joins a topic or queue other services already use, capture a few real messages and check, in a unit test, that relaybus writes exactly those messages and reads back exactly those events. `@relaybus/testing` registers one test per captured message:

```ts
import { kafkaCodecContract } from '@relaybus/testing';
import { decodeKafkaRecord, jsonEnvelope } from '@relaybus/kafka';

const codec = jsonEnvelope({
  type: { header: 'x-event-type' },
  id: { header: 'x-message-id' },
  payload: { body: 'payload' },
  source: { body: 'meta.producer' },
  occurredAt: { body: 'meta.sentAt', format: 'epoch-millis' },
  extra: { body: { 'meta.schema': 'orders/v1' }, headers: { 'x-schema-version': '1' } },
  bodyOrder: ['meta', 'payload'],
});

kafkaCodecContract('orders format', {
  encode: codec.encode,
  decode: (record) => decodeKafkaRecord(codec, record),
  compare: 'bytes', // or 'json': same JSON regardless of key order and spacing
  ignoreHeaders: ['content-type'], // headers relaybus adds that the other consumers ignore
  cases: [
    {
      name: 'order placed',
      envelope: { id: 'm-1', type: 'order.placed.v1', key: 'order-1' /* ... the whole event */ },
      record: {
        topic: 'orders',
        key: 'order-1',
        headers: {
          'x-event-type': 'order.placed.v1',
          'x-message-id': 'm-1',
          'x-schema-version': '1',
        },
        value: '{"meta":{...},"payload":{...}}',
      },
    },
  ],
});
```

Each case checks both directions: `encode(envelope)` gives the captured key, value and headers (and the timestamp, when the codec sets one), and decoding the captured record gives `envelope`, filled in the way the transport fills it (`decodeKafkaRecord`). When the wire does not carry all of the event, for example a record without a key, give the event decoding yields as `decoded`; `only: 'decode'` checks a message only other producers write (one without an id, say), `only: 'encode'` one only other consumers read. `encode` may also be a transport's publish captured by an injected producer, so the test covers what the transport sends. `rabbitMqCodecContract` does the same for RabbitMQ with `decodeRabbitMqMessage`: the captured message has `properties` (as publish options name them, such as `persistent`), `headers` and `body`; `ignoreProperties` leaves out a per-send `timestamp`. The routing key is the route's destination, not part of the codec. `kafkaCodecMismatches` and `rabbitMqCodecMismatches` return the differences as lines, for other test runners.

## Malformed input

Under every codec, a record or message that cannot become an event (not JSON, no type, an invalid type, a missing mapped id, a missing payload, an exception in your `decode`) goes to the transport's invalid-message handling: Kafka dead-letters it to `<topic>.dead` with the reason in the `relaybus-invalid-reason` header (or applies your `onInvalidMessage` policy), RabbitMQ dead-letters it to `<queue>.dead`. The consumer never crashes and moves on. Records behind it keep their per-key order.

## Migrating from `decode`

`decode` still works and is deprecated: it reads with your function and writes CloudEvents binary mode. `decode: fn` is `codec: { encode: encodeEnvelope, decode: fn }`; giving both options is a configuration error. `decodeCloudEvent` and `plainJsonDecoder` remain available for such a codec, for example to read plain JSON and CloudEvents from one topic while writing binary mode:

```ts
import { encodeEnvelope, fromHeader, plainJsonDecoder } from '@relaybus/kafka';

codec: { encode: encodeEnvelope, decode: plainJsonDecoder({ type: fromHeader('event-type') }) }
```
