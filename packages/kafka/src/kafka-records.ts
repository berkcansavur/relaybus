import type { EventEnvelope } from '@relaybus/core';
import { decodeEnvelope, toOutgoingHeaders } from './cloudevents-codec.js';
import { cloudEventsBinary, type KafkaCodec } from './codecs.js';
import { completeDecodedEvent } from './inbound-decoders.js';
import type { KafkaEachMessagePayload, KafkaProducerRecord } from './kafka-client.js';

// What a consumed record becomes: an event, or a copy for its dead-letter topic.

/** The event of a consumed record, by the transport's codec; throws for a malformed record. */
export function decodeRecord(
  codec: KafkaCodec,
  { topic, partition, message }: KafkaEachMessagePayload,
): EventEnvelope {
  if (codec === cloudEventsBinary) {
    return decodeEnvelope(message);
  }
  const record = {
    topic,
    partition,
    offset: message.offset,
    key: message.key,
    value: message.value,
    headers: message.headers,
    timestamp: message.timestamp,
  };
  return completeDecodedEvent(codec.decode(record), record);
}

/** A copy of a record for its dead-letter topic, with why and where it came from in its headers. */
export function deadLetterRecord(
  { topic, partition, message }: KafkaEachMessagePayload,
  {
    reason,
    name,
    code,
    deadLetterTopic,
  }: {
    readonly reason: string;
    readonly name: string;
    readonly code: string | undefined;
    readonly deadLetterTopic: string;
  },
): KafkaProducerRecord {
  const headers: Record<string, Buffer | string | (Buffer | string)[]> = {
    ...toOutgoingHeaders(message.headers),
    'relaybus-invalid-reason': reason,
    'relaybus-source-topic': topic,
    'relaybus-source-partition': String(partition),
    'relaybus-source-offset': message.offset,
    'relaybus-error-name': name,
  };
  if (code === undefined) {
    delete headers['relaybus-error-code'];
  } else {
    headers['relaybus-error-code'] = code;
  }
  return {
    topic: deadLetterTopic,
    acks: -1,
    messages: [
      {
        key: message.key === null ? null : Buffer.from(message.key),
        value: message.value === null ? null : Buffer.from(message.value),
        headers,
      },
    ],
  };
}
