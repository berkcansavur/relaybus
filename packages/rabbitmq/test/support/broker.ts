import { randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import amqplib from 'amqplib';
import { inject } from 'vitest';
import type { EventEnvelope } from '@relaybus/core';

const execFileAsync = promisify(execFile);

/** What the tests use of the broker the global setup started for this run. */
export interface TestBroker {
  getAmqpUrl(): string;
  /** Host port mapped to the AMQP port (5672). */
  getMappedPort(port: 5672): number;
  /** Runs a command inside the broker container, for example `rabbitmqctl`. */
  exec(command: string[]): Promise<{ output: string; exitCode: number }>;
  /** The broker is shared by every test file and stopped by the global setup. */
  stop(): Promise<void>;
}

/**
 * Connects to the broker shared by the whole run. Tests stay apart through `unique()`
 * exchange, queue and group names. A test that restarts the broker must use
 * `startDedicatedBroker()` instead.
 */
export function startBroker(): Promise<TestBroker> {
  const { amqpUrl, amqpPort, containerId } = inject('rabbitmqBroker');
  return Promise.resolve({
    getAmqpUrl: () => amqpUrl,
    getMappedPort: () => amqpPort,
    exec: async (command) => {
      try {
        const { stdout } = await execFileAsync('docker', ['exec', containerId, ...command]);
        return { output: stdout, exitCode: 0 };
      } catch (error) {
        const failure = error as { stdout?: string; code?: number };
        return { output: failure.stdout ?? '', exitCode: failure.code ?? 1 };
      }
    },
    stop: async () => {},
  });
}

/** Short random suffix so tests sharing a broker never share exchanges or queues. */
export function unique(prefix: string): string {
  return `${prefix}-${randomUUID().slice(0, 8)}`;
}

/** Reads a queue through a plain amqplib channel, bypassing the transport under test. */
export async function inspectQueue(
  amqpUrl: string,
  queue: string,
): Promise<{ messageCount: number; consumerCount: number }> {
  const connection = await amqplib.connect(amqpUrl);
  try {
    const channel = await connection.createChannel();
    return await channel.checkQueue(queue);
  } finally {
    await connection.close();
  }
}

/** Takes (and acks) one message from a queue, or returns undefined when it is empty. */
export async function takeMessage(
  amqpUrl: string,
  queue: string,
): Promise<{ headers: Record<string, unknown>; routingKey: string; body: string } | undefined> {
  const connection = await amqplib.connect(amqpUrl);
  try {
    const channel = await connection.createChannel();
    const message = await channel.get(queue, { noAck: true });
    return message === false
      ? undefined
      : {
          headers: message.properties.headers ?? {},
          routingKey: message.fields.routingKey,
          body: message.content.toString('utf8'),
        };
  } finally {
    await connection.close();
  }
}

/** Publishes raw bytes the way a foreign producer would. */
export async function publishRaw(
  amqpUrl: string,
  exchange: string,
  routingKey: string,
  content: Buffer,
  headers: Record<string, string> = {},
): Promise<void> {
  const connection = await amqplib.connect(amqpUrl);
  try {
    const channel = await connection.createConfirmChannel();
    await channel.assertExchange(exchange, 'topic', { durable: true });
    channel.publish(exchange, routingKey, content, { headers, persistent: true });
    await channel.waitForConfirms();
  } finally {
    await connection.close();
  }
}

export async function eventually(
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 60_000,
  description = 'condition',
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

export function remoteEnvelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
  return {
    id: randomUUID(),
    type: 'order.placed.v1',
    source: 'orders-service',
    key: 'order-1',
    payload: { orderId: '1' },
    headers: { traceparent: '00-abc-def-01' },
    correlationId: 'flow-1',
    causationId: 'cause-1',
    occurredAt: new Date('2026-01-01T10:20:30.456Z'),
    ...overrides,
  };
}
