import type { RelayBusLogger } from '@relaybus/core';

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
  /** Ends the current listening connection; undefined while there is none. */
  #drop: (() => void) | undefined;
  #reconnectTimer: NodeJS.Timeout | undefined;
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
    let ended = false;
    // Called at most once: a loss is handled once (`ended`), and closing or a failed start end what was not lost.
    const end = (): void => {
      ended = true;
      connection.end().catch(() => undefined);
    };
    let listening = false;
    const onLost = (error?: Error): void => {
      if (!listening || ended) {
        // While starting, a failure surfaces as the rejection of LISTEN, which ends the connection.
        return;
      }
      // Closing ends the connection first, so a loss reported here is never one of a closed listener.
      end();
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
      end();
      throw error;
    }
    if (this.#closed) {
      end();
      return;
    }
    listening = true;
    this.#drop = end;
  }

  #scheduleReconnect(): void {
    this.#reconnectTimer = setTimeout(() => {
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
    }, RECONNECT_DELAY_MS);
    this.#reconnectTimer.unref();
  }

  /** Stops listening for good and ends the connection. */
  async close(): Promise<void> {
    this.#closed = true;
    clearTimeout(this.#reconnectTimer);
    const drop = this.#drop;
    this.#drop = undefined;
    drop?.();
  }
}
