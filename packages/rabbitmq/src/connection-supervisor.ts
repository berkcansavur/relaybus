import {
  computeRetryDelay,
  toErrorInfo,
  type RelayBusLogger,
  type RetryPolicy,
} from '@relaybus/core';
import { abortableSleep } from './abortable-sleep.js';
import type { AmqpConnection } from './amqp-types.js';
import { RabbitMqUnavailableError } from './errors.js';
import type { ConnectOptions } from './options.js';

export interface ConnectionSupervisorSettings {
  readonly connect: (options: ConnectOptions) => Promise<AmqpConnection>;
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
  /** Aborted by `forceClose`: destroys a socket that is still connecting. */
  readonly #force = new AbortController();
  /** Resolves on `forceClose`: closing stops waiting for the broker. */
  readonly #forcing = new Promise<void>((resolve) => {
    this.#force.signal.addEventListener('abort', () => resolve(), { once: true });
  });
  #current: AmqpConnection | undefined;
  /** Connections being closed, until their closing handshake finishes. */
  readonly #closing = new Set<AmqpConnection>();
  /** Set by `forceClose`: connections are cut instead of closed. */
  #forced = false;
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
    void this.#closeConnection(connection);
  }

  /**
   * Stops reconnecting, runs `beforeDisconnect` while the connection is still
   * open (to drain consumers) and closes the connection.
   */
  async close(beforeDisconnect: () => Promise<void>): Promise<void> {
    this.#closed = true;
    this.#stop.abort();
    // Once forced, nothing waits for the broker: draining and the closing handshake are cut short.
    await Promise.race([this.#reconnecting, this.#forcing]);
    try {
      await Promise.race([beforeDisconnect(), this.#forcing]);
    } finally {
      const connection = this.#current;
      this.#current = undefined;
      this.#ready = false;
      if (connection !== undefined) {
        await this.#closeConnection(connection);
      }
    }
  }

  /**
   * Stops reconnecting and cuts every connection, open or closing, without the closing handshake. A
   * connection that finishes opening afterwards is cut at once.
   */
  forceClose(): void {
    this.#closed = true;
    this.#forced = true;
    this.#stop.abort();
    this.#force.abort();
    for (const connection of [this.#current, ...this.#closing]) {
      if (connection !== undefined) {
        destroySocket(connection);
      }
    }
  }

  /** Closes a connection, which `forceClose` can cut until the broker answered; cuts it once forced. */
  async #closeConnection(connection: AmqpConnection): Promise<void> {
    if (this.#forced) {
      destroySocket(connection);
      return;
    }
    this.#closing.add(connection);
    await connection.close().catch(() => undefined);
    this.#closing.delete(connection);
  }

  async #establish(): Promise<void> {
    const connection = await this.#settings.connect({ signal: this.#force.signal });
    // Before anything can cut it: an 'error' nobody listens to would crash the process.
    connection.on('error', (error: Error) => {
      this.#settings.logger.warn(
        `[ConnectionSupervisor](establish) Connection error | error=${error.message}`,
      );
    });
    if (this.#closed) {
      // Opened after the transport closed: nobody would use or close it.
      await this.#closeConnection(connection);
      throw new RabbitMqUnavailableError('[ConnectionSupervisor](establish) Transport is closed');
    }
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
    await this.#closeConnection(connection);
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

/**
 * Destroys the socket under an amqplib connection (the channel model keeps it in fields its types
 * omit). With an error, so amqplib sees a socket error and runs its terminal cleanup: it stops
 * heartbeating, fails pending channel operations and emits `close`.
 */
function destroySocket(connection: AmqpConnection): void {
  const model = connection as {
    readonly connection?: { readonly stream?: { destroy(error: Error): void } };
  };
  model.connection?.stream?.destroy(
    new RabbitMqUnavailableError('Connection cut: the shutdown deadline passed'),
  );
}
