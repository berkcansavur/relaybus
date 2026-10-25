import { systemClock } from '@relaybus/core';
import { schedule, type CancelTimer } from '@relaybus/core/internal';
import { PublishTimeoutError, TransportClosedError } from './errors.js';
import type { KafkaProducerLike, KafkaProducerRecord } from './kafka-client.js';
import type { KafkaSettings } from './kafka-settings.js';

/**
 * The idempotent producer of a transport: connected on first use (once, shared by concurrent
 * callers, again after a failed connect), and every send bounded by the publish timeout.
 */
export class KafkaProducerSession {
  readonly #settings: KafkaSettings;
  /** Whether the transport is closed: then nothing connects any more. */
  readonly #isClosed: () => boolean;
  #producer: Promise<KafkaProducerLike> | undefined;
  #connected = false;

  constructor(settings: KafkaSettings, isClosed: () => boolean) {
    this.#settings = settings;
    this.#isClosed = isClosed;
  }

  /** True once the producer connected, until the transport closes. */
  get isConnected(): boolean {
    return this.#connected;
  }

  /** The producer opened so far, if any: what closing must disconnect. */
  get pending(): Promise<KafkaProducerLike> | undefined {
    return this.#producer;
  }

  markClosed(): void {
    this.#connected = false;
  }

  connected(): Promise<KafkaProducerLike> {
    if (this.#isClosed()) {
      return Promise.reject(
        new TransportClosedError(`Transport '${this.#settings.name}' is closed`),
      );
    }
    this.#producer ??= this.#open();
    return this.#producer;
  }

  /** Sends with the publish timeout; the late outcome of a timed-out send is ignored. */
  async send(producer: KafkaProducerLike, record: KafkaProducerRecord): Promise<void> {
    const { name, timings } = this.#settings;
    let cancel: CancelTimer | undefined;
    const timeout = new Promise<never>((_resolve, reject) => {
      cancel = schedule(systemClock, {
        run: () => {
          reject(
            new PublishTimeoutError(
              `Transport '${name}' got no broker ack for topic '${record.topic}' within ${timings.publishTimeoutMs}ms`,
            ),
          );
        },
        delayMs: timings.publishTimeoutMs,
      });
    });
    const sending = producer.send(record);
    sending.catch(() => undefined);
    try {
      await Promise.race([sending, timeout]);
    } finally {
      cancel?.();
    }
  }

  async #open(): Promise<KafkaProducerLike> {
    const { kafka, producerConfig, maxInFlightRequests } = this.#settings;
    const producer = kafka.producer({ ...producerConfig, idempotent: true, maxInFlightRequests });
    try {
      await producer.connect();
    } catch (error) {
      this.#producer = undefined;
      throw error;
    }
    this.#connected = !this.#isClosed();
    return producer;
  }
}
