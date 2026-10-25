import { describe, expect, it } from 'vitest';
import type { EventEnvelope } from '@relaybus/core';
import { rabbitMqCodecContract } from '@relaybus/testing';
import {
  cloudEventsBinary,
  decodeRabbitMqMessage,
  jsonEnvelope,
  type RabbitMqCodec,
} from '../src/index.js';

/** An in-house format: the type and the payload in the body, the time in epoch seconds, a constant producer. */
const IN_HOUSE: RabbitMqCodec = jsonEnvelope({
  type: { body: 'eventName' },
  id: 'message-id-property',
  payload: { body: 'data' },
  occurredAt: { body: 'sentAt', format: 'epoch-seconds' },
  extra: { body: { producer: 'orders-api' }, headers: { 'x-schema': '1' } },
});

const EVENT: EventEnvelope = {
  id: '0190a000-0000-7000-8000-0000000000aa',
  type: 'order.placed.v1',
  source: 'orders',
  key: null,
  payload: { orderId: '1' },
  headers: {},
  correlationId: null,
  causationId: null,
  occurredAt: new Date('2026-01-01T00:00:00.000Z'),
};

rabbitMqCodecContract('an in-house JSON format', {
  encode: IN_HOUSE.encode,
  decode: (message) => decodeRabbitMqMessage(IN_HOUSE, message),
  cases: [
    {
      name: 'order placed',
      envelope: EVENT,
      message: {
        routingKey: 'orders.placed',
        properties: {
          persistent: true,
          messageId: '0190a000-0000-7000-8000-0000000000aa',
          timestamp: 1767225600,
          type: 'order.placed.v1',
          appId: 'orders',
          contentType: 'application/json',
        },
        headers: { 'x-schema': '1' },
        body: '{"eventName":"order.placed.v1","sentAt":1767225600,"data":{"orderId":"1"},"producer":"orders-api"}',
      },
    },
  ],
});

describe('decodeRabbitMqMessage', () => {
  it('reads CloudEvents binary mode the way the transport does', () => {
    const event = { ...EVENT, key: 'order-1', correlationId: 'flow-1' };
    const { content, options } = cloudEventsBinary.encode(event);
    const decoded = decodeRabbitMqMessage(cloudEventsBinary, {
      content,
      fields: { routingKey: 'orders.placed' },
      properties: options,
    });
    expect(decoded).toEqual(event);
  });
});
