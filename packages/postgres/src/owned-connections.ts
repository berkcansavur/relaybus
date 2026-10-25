import { Client, type ClientConfig } from 'pg';

/**
 * The connections the store opened itself: every connection of the pool it created from
 * `connectionString`, and the connections it listens for notifications on. A shutdown that runs past
 * its deadline destroys them, so a query that hangs (a lock wait, an unreachable server) fails at
 * once instead of holding `stop` and the process. Pools the application passed in are never tracked.
 */
export class OwnedConnections {
  readonly #clients = new Set<Client>();

  /**
   * The `Client` class for a pool the store creates: every connection is tracked from the moment it
   * is created, so one still connecting or authenticating is destroyed too.
   */
  clientClass(): typeof Client {
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- the class below needs the tracker
    const owned = this;
    return class OwnedClient extends Client {
      constructor(config?: string | ClientConfig) {
        super(config);
        owned.track(this);
      }
    };
  }

  /** Tracks one connection until it ends. */
  track(client: Client): void {
    this.#clients.add(client);
    client.once('end', () => this.#clients.delete(client));
  }

  /**
   * Destroys the socket of every tracked connection. Its queries fail with "Connection terminated"
   * and the pool drops it. The server ends the session when it next talks to the client.
   */
  destroyAll(): void {
    for (const client of this.#clients) {
      // A destroyed connection reports an error; nobody may be listening on a checked-out one.
      client.on('error', () => undefined);
      client.connection.stream.destroy();
    }
    this.#clients.clear();
  }
}
