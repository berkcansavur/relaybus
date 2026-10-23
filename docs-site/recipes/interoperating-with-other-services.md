---
title: Interoperating with services that do not use relaybus - Kafka and RabbitMQ wire format
description: Exchange events between relaybus and services in Java, Go, Spring or plain kafkajs/amqplib that do not use relaybus. The Kafka and RabbitMQ wire format, and decoding plain JSON and structured CloudEvents with exactly-once handling.
---

# Interoperating with non-relaybus services

relaybus only needs to run on one side of a topic or queue. Events it publishes are CloudEvents 1.0 in binary content mode, readable by any client. Events written by services that do not use relaybus are accepted once you say how to read them, with the same guarantees: stored once by id, handled once, malformed input dead-lettered while the consumer keeps running.

| Direction                         | Default                                                           | What to configure                                                     |
| --------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------- |
| relaybus producer, other consumer | Works as is: JSON payload plus CloudEvents headers.               | Nothing. Deduplicate on the event id, delivery is at-least-once.      |
| CloudEvents binary producer       | Accepted.                                                         | Nothing.                                                              |
| CloudEvents structured producer   | Dead-lettered (the default is strict binary mode).                | `decode: decodeCloudEvent`                                            |
| Plain JSON producer               | Dead-lettered, so a misconfigured producer cannot slip events in. | `decode: plainJsonDecoder({ type, id?, key?, source?, occurredAt? })` |
| Avro, Protobuf, anything else     | Dead-lettered.                                                    | `decode: (record) => ({ id, type, payload })`, your own decoder.      |

## Wire format written by relaybus

The payload is always the JSON body, UTF-8. The attributes travel as headers.

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

Reading it without relaybus:

- **Plain kafkajs, Go, Python**: `JSON.parse(record.value)`, type and id from the `ce_type` and `ce_id` headers.
- **Java**: `io.cloudevents:cloudevents-kafka` `CloudEventDeserializer` reads the record as a `CloudEvent` directly. Spring Cloud Function and Spring Cloud Stream recognise the `ce_` headers of the Kafka binding.
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

## Consuming events from a service without relaybus

Choose the decoder on the transport that consumes the topic or queue. One transport is one subscription, so different decoders need different transports, or one decoder function that switches on `record.topic` (Kafka) or `message.fields.routingKey` (RabbitMQ).

### Plain JSON

```ts
import { KafkaTransport, fromHeader, fromPayloadField, plainJsonDecoder } from '@relaybus/kafka';

const transport = new KafkaTransport({
  kafka,
  groupId: 'billing',
  decode: plainJsonDecoder({
    type: fromHeader('event-type'), // or 'order.placed.v1', or fromPayloadField('eventType')
    id: fromHeader('message-id'), // optional, see below
    key: fromPayloadField('orderId'), // optional, default: the record key
  }),
});
```

```ts
import { RabbitMqTransport, plainJsonDecoder } from '@relaybus/rabbitmq';

const transport = new RabbitMqTransport({
  url,
  consumerGroup: 'billing',
  decode: plainJsonDecoder({ type: (message) => message.properties.type as string | undefined }),
});
```

The body becomes the payload and the headers become event headers. Handlers subscribe to the type as usual, `bus.subscribe('order.placed.v1', handler, { id })`. A record or message that is a CloudEvent anyway (binary or structured) is decoded as one, so relaybus and other producers can share a topic or queue.

**Ids and exactly-once handling.** The inbox stores an event once per id, so the id must be the same on every redelivery:

| Transport | Default id                                                                    | Without a usable id                       |
| --------- | ----------------------------------------------------------------------------- | ----------------------------------------- |
| Kafka     | `kafka:<topic>:<partition>:<offset>`, stable for every redelivery of a record | not possible, there is always an offset   |
| RabbitMQ  | the `messageId` property, kept on redeliveries and retry-queue round trips    | the message is dead-lettered as malformed |

A producer that may send one event twice writes it twice, at two offsets: only an id set by the producer, read with `id: fromHeader(...)` or `fromPayloadField(...)`, deduplicates that. On Kafka, deleting and recreating a topic restarts its offsets, so prefer a producer id there too.

### Structured CloudEvents

CloudEvents SDKs often send structured mode, one JSON document with the attributes and `data`:

```json
{
  "specversion": "1.0",
  "id": "ce-5",
  "type": "order.placed.v1",
  "source": "//go/orders",
  "time": "2026-05-01T10:00:00Z",
  "data": { "orderId": "5" }
}
```

with `content-type: application/cloudevents+json` (Kafka header, RabbitMQ `contentType` property). `decode: decodeCloudEvent` accepts it and binary mode. `data` must be JSON (`data_base64` is refused); string extension attributes such as `traceparent` become event headers.

### Your own decoder

```ts
const decode: KafkaMessageDecoder = (record) => {
  const order = decodeLegacyOrder(record.value); // must be synchronous and deterministic
  return { id: order.reference, type: 'order.placed.v1', payload: order };
};
```

Fields you leave out are filled from the record: `source`, `key`, `occurredAt`, `headers`. Anything the decoder throws marks the record as malformed.

## Malformed input

Under every decoder, a record or message that cannot become an event (not JSON, no type, an invalid type, a missing id on RabbitMQ, an exception in your decoder) goes to the transport's invalid-message handling: Kafka dead-letters it to `<topic>.dead` with the reason in the `relaybus-invalid-reason` header (or applies your `onInvalidMessage` policy), RabbitMQ dead-letters it to `<queue>.dead`. The consumer never crashes and moves on to the next record. Records behind it keep their per-key order.
