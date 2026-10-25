import type { RelayBusLogger } from '@relaybus/core';
import { systemClock } from '@relaybus/core';
import { schedule, type CancelTimer } from '@relaybus/core/internal';

/** Wait before a lost listening connection is opened again. */
const RECONNECT_DELAY_MS = 1_000;

/** What the listener needs of a connection: a `pg` `Client` has it. */
export interface ListeningConnection {
  on(event: 'notification', listener: () => void): unknown;
  on(event: 'error', listener: (error: Error) => void): unknown;
  on(event: 'end', listener: () => void): unknown;
  query(sql: string): Promise<unknown>;
  end(): Promise<void>;
}

/**
 * Returns a function that ends the connection once, however often it is called. `ended` is set
 * first because ending can report the end synchronously, which calls back into the loss handler.
 */
function endOnce(connection: ListeningConnection, ended: { value: boolean }): () => Promise<void> {
  let ending: Promise<void> = Promise.resolve();
  return () => {
    if (!ended.value) {
      ended.value = true;
      ending = connection.end().catch(() => undefined);
    }
    return ending;
  };
}

/**
 * Holds one connection of its own (outside any pool, so it never takes a connection the store needs)
 * that `LISTEN`s on a channel, and calls a listener for every notification. The notifications are hints
 * for the dispatcher, never needed for correctness: when the connection is lost it is replaced after a
 * pause, and the listener is called once for whatever was sent meanwhile.
 */
export class NotificationListener {
  readonly #connect: () => Promise<ListeningConnection>;
  readonly #channel: string;
  readonly #listener: () => void;
  readonly #logger: RelayBusLogger;
  /** Ends the current connection, listening or still starting; undefined while there is none. */
  #drop: (() => Promise<void>) | undefined;
  #reconnectTimer: CancelTimer | undefined;
  #closed = false;

  constructor(
    connect: () => Promise<ListeningConnection>,
    channel: string,
    listener: () => void,
    logger: RelayBusLogger,
  ) {
    this.#connect = connect;
    this.#channel = channel;
    this.#listener = listener;
    this.#logger = logger;
  }

  /** Connects and listens. Rejects when that fails; a connection lost later is replaced. */
  async start(): Promise<void> {
    const connection = await this.#connect();
    const ended = { value: false };
    const end = endOnce(connection, ended);
    if (this.#closed) {
      await end();
      return;
    }
    // Registered before LISTEN, so closing ends a connection whose LISTEN has not answered yet.
    this.#drop = end;
    let listening = false;
    const onLost = (error?: Error): void => {
      if (!listening || ended.value) {
        // While starting, a failure surfaces as the rejection of LISTEN, which ends the connection.
        return;
      }
      void end();
      // Only a listening connection gets here, and it is always the current one.
      this.#drop = undefined;
      this.#logger.warn(
        `[NotificationListener](start) Listening connection lost, opening another | channel=${this.#channel} | error=${error?.message ?? 'connection ended'}`,
      );
      this.#scheduleReconnect();
    };
    connection.on('error', onLost);
    connection.on('end', () => onLost());
    connection.on('notification', () => this.#listener());
    try {
      await connection.query(`LISTEN "${this.#channel}"`);
    } catch (error) {
      await this.#abandonFailedListen(end, error);
      return;
    }
    if (this.#closed) {
      await end();
      return;
    }
    listening = true;
  }

  /** Forgets and ends a connection whose LISTEN failed; rethrows the failure unless closing caused it. */
  async #abandonFailedListen(end: () => Promise<void>, error: unknown): Promise<void> {
    if (this.#drop === end) {
      this.#drop = undefined;
    }
    await end();
    if (this.#closed) {
      // Closing ended the connection while LISTEN was pending: not a failure to report.
      return;
    }
    throw error;
  }

  #scheduleReconnect(): void {
    this.#reconnectTimer = schedule(systemClock, {
      run: () => {
        this.#reconnectTimer = undefined;
        this.start().then(
          () => {
            if (!this.#closed) {
              this.#listener();
            }
          },
          (error: unknown) => {
            this.#logger.warn(
              `[NotificationListener](scheduleReconnect) Listening again failed, retrying | channel=${this.#channel} | error=${error instanceof Error ? error.message : String(error)}`,
            );
            if (!this.#closed) {
              this.#scheduleReconnect();
            }
          },
        );
      },
      delayMs: RECONNECT_DELAY_MS,
      unref: true,
    });
  }

  /** Stops listening for good and ends the connection. */
  async close(): Promise<void> {
    this.#closed = true;
    this.#reconnectTimer?.();
    const drop = this.#drop;
    this.#drop = undefined;
    await drop?.();
  }
}
