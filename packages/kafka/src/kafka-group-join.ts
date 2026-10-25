import { systemClock } from '@relaybus/core';
import { schedule, type CancelTimer } from '@relaybus/core/internal';
import { GroupJoinTimeoutError, TransportClosedError } from './errors.js';
import type { KafkaConsumerLike, KafkaEachMessagePayload } from './kafka-client.js';
import type { KafkaSettings } from './kafka-settings.js';

/** A wait for the consumer to join its group, and what ends it early. */
type GroupJoin = { readonly promise: Promise<void>; dispose(): void };

/**
 * Waits, within the startup timeout, for the consumer to join its group: a start-up that cannot
 * consume fails loudly instead of looking healthy. A crash or a close ends the wait early.
 */
export class GroupJoinWatch {
  readonly #settings: KafkaSettings;
  #waiter: { resolve(): void; reject(error: unknown): void } | undefined;

  constructor(settings: KafkaSettings) {
    this.#settings = settings;
  }

  /** The consumer joined its group. */
  joined(): void {
    this.#waiter?.resolve();
  }

  /** The consumer crashed while it was joining. */
  failed(error: unknown): void {
    this.#waiter?.reject(error);
  }

  /** The transport closed while the consumer was joining. */
  closed(): void {
    this.#waiter?.reject(new TransportClosedError(`Transport '${this.#settings.name}' is closed`));
  }

  /** Connects and subscribes the consumer, runs it and resolves once it joined its group. */
  async start(
    consumer: KafkaConsumerLike,
    {
      topics,
      fromBeginning,
      eachMessage,
    }: {
      readonly topics: string[];
      readonly fromBeginning: boolean;
      readonly eachMessage: (payload: KafkaEachMessagePayload) => Promise<void>;
    },
  ): Promise<void> {
    await consumer.connect();
    await consumer.subscribe(this.#settings.dialect.subscription(topics, fromBeginning));
    const joined = this.#wait(consumer);
    try {
      await consumer.run({ eachMessage });
      await joined.promise;
    } finally {
      joined.dispose();
    }
  }

  #wait(consumer: KafkaConsumerLike): GroupJoin {
    if (!this.#settings.dialect.tracksMembership(consumer)) {
      return { promise: Promise.resolve(), dispose: () => undefined };
    }
    const { name, groupId, timings } = this.#settings;
    let cancel: CancelTimer | undefined;
    const promise = new Promise<void>((resolve, reject) => {
      this.#waiter = { resolve, reject };
      cancel = schedule(systemClock, {
        run: () => {
          reject(
            new GroupJoinTimeoutError(
              `Consumer of transport '${name}' did not join group '${groupId}' within ${timings.startupTimeoutMs}ms`,
            ),
          );
        },
        delayMs: timings.startupTimeoutMs,
      });
    });
    // The rejection may land while `run` is still pending; the caller awaits it afterwards.
    promise.catch(() => undefined);
    return {
      promise,
      dispose: () => {
        cancel?.();
        this.#waiter = undefined;
      },
    };
  }
}
