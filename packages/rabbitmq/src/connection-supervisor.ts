import {
  computeRetryDelay,
  toErrorInfo,
  type RelayBusLogger,
  type RetryPolicy,
} from '@relaybus/core';
import { abortableSleep } from './abortable-sleep.js';
import type { AmqpConnection } from './amqp-types.js';
import { RabbitMqUnavailableError } from './errors.js';

export interface ConnectionSupervisorSettings {
  readonly connect: () => Promise<AmqpConnection>;
  /** Backoff between attempts, or undefined to never reconnect. */
  readonly reconnect: RetryPolicy | undefined;
  /** Creates channels and topology on a fresh connection. A rejection discards the connection. */
  readonly onConnected: (connection: AmqpConnection) => Promise<void>;
  /** The connection is gone or was discarded: drop everything that referenced it. */
  readonly onDisconnected: () => void;
  readonly logger: RelayBusLogger;
  readonly random: () => number;
}

/**
 * Owns the broker connection. amqplib does not reconnect, so after a connection
 * loss this class retries with capped, jittered backoff until `onConnected`
 * succeeds, and `close` stops it. It reports ready only once `onConnected` has
 * finished, so callers never use half-built channels.
 */
export class ConnectionSupervisor {
  readonly #settings: ConnectionSupervisorSettings;
  readonly #stop = new AbortController();
  #current: AmqpConnection | undefined;
  #ready = false;
  #started = false;
  #closed = false;
  #reconnecting: Promise<void> | undefined;

  constructor(settings: ConnectionSupervisorSettings) {
    this.#settings = settings;
  }

  /** True while a connection is open and `onConnected` has completed for it. */
  isConnected(): boolean {
    return this.#ready;
  }

  /**
   * Connects once. When that fails it rejects if `failOnError`, otherwise it
   * resolves and keeps trying in the background. Does nothing when already started.
   */
  async start(failOnError: boolean): Promise<void> {
    if (this.#started) {
      return;
    }
    if (this.#closed) {
      throw new RabbitMqUnavailableError('[ConnectionSupervisor](start) Transport is closed');
    }
    try {
      await this.#establish();
      this.#started = true;
    } catch (error) {
      if (failOnError) {
        throw error;
      }
      this.#settings.logger.warn(
        `[ConnectionSupervisor](start) Broker unreachable, retrying in the background | error=${toErrorInfo(error).message}`,
      );
      this.#started = true;
      this.#startReconnecting();
    }
  }

  /**
   * Drops the current connection because one of its channels failed, then
   * reconnects. Closing the channel alone would leave topology half-restored.
   */
  recover(reason: string): void {
    const connection = this.#current;
    if (connection === undefined) {
      return;
    }
    this.#handleLoss(connection, reason);
    connection.close().catch(() => undefined);
  }

  /**
   * Stops reconnecting, runs `beforeDisconnect` while the connection is still
   * open (to drain consumers) and closes the connection.
   */
  async close(beforeDisconnect: () => Promise<void>): Promise<void> {
    this.#closed = true;
    this.#stop.abort();
    await this.#reconnecting;
    try {
      await beforeDisconnect();
    } finally {
      const connection = this.#current;
      this.#current = undefined;
      this.#ready = false;
      await connection?.close().catch(() => undefined);
    }
  }

  async #establish(): Promise<void> {
    const connection = await this.#settings.connect();
    connection.on('error', (error: Error) => {
      this.#settings.logger.warn(
        `[ConnectionSupervisor](establish) Connection error | error=${error.message}`,
      );
    });
    connection.on('close', (error?: Error) => {
      this.#handleLoss(connection, error?.message ?? 'closed by peer');
    });
    this.#current = connection;
    try {
      await this.#settings.onConnected(connection);
      if (this.#current !== connection) {
        throw new RabbitMqUnavailableError(
          'Connection was lost while the topology was being set up',
        );
      }
    } catch (error) {
      await this.#discard(connection);
      throw error;
    }
    this.#ready = true;
  }

  async #discard(connection: AmqpConnection): Promise<void> {
    if (this.#current === connection) {
      this.#current = undefined;
    }
    this.#settings.onDisconnected();
    await connection.close().catch(() => undefined);
  }

  #handleLoss(connection: AmqpConnection, reason: string): void {
    if (connection !== this.#current) {
      return;
    }
    this.#current = undefined;
    this.#ready = false;
    this.#settings.logger.warn(
      `[ConnectionSupervisor](handleLoss) Connection lost | reason=${reason}`,
    );
    this.#settings.onDisconnected();
    this.#startReconnecting();
  }

  #startReconnecting(): void {
    if (
      this.#settings.reconnect === undefined ||
      this.#closed ||
      !this.#started ||
      this.#reconnecting !== undefined
    ) {
      return;
    }
    const policy = this.#settings.reconnect;
    this.#reconnecting = this.#reconnectUntilConnected(policy).finally(() => {
      this.#reconnecting = undefined;
    });
  }

  async #reconnectUntilConnected(policy: RetryPolicy): Promise<void> {
    for (let attempt = 1; !this.#closed; attempt += 1) {
      const delayMs = computeRetryDelay(policy, attempt, this.#settings.random);
      this.#settings.logger.log(
        `[ConnectionSupervisor](reconnectUntilConnected) Reconnecting | attempt=${attempt} delayMs=${delayMs}`,
      );
      await abortableSleep(delayMs, this.#stop.signal);
      if (this.#closed) {
        return;
      }
      try {
        await this.#establish();
        this.#settings.logger.log(
          `[ConnectionSupervisor](reconnectUntilConnected) Reconnected | attempt=${attempt}`,
        );
        return;
      } catch (error) {
        this.#settings.logger.warn(
          `[ConnectionSupervisor](reconnectUntilConnected) Attempt failed | attempt=${attempt} error=${toErrorInfo(error).message}`,
        );
      }
    }
  }
}
