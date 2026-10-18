import net from 'node:net';

/**
 * TCP proxy in front of the broker so tests can cut connections and refuse new
 * ones without touching the broker itself. `target` is a function because a
 * restarted container may get a new host port.
 */
export class TcpProxy {
  readonly #target: () => number;
  readonly #server: net.Server;
  readonly #sockets = new Set<net.Socket>();
  #refusing = false;

  constructor(target: () => number) {
    this.#target = target;
    this.#server = net.createServer((client) => this.#accept(client));
  }

  get port(): number {
    return (this.#server.address() as net.AddressInfo).port;
  }

  async listen(): Promise<void> {
    await new Promise<void>((resolve) => this.#server.listen(0, '127.0.0.1', resolve));
  }

  /** Destroys every open connection, like a network drop. */
  sever(): void {
    for (const socket of this.#sockets) {
      socket.destroy();
    }
  }

  /** While refusing, new connections are closed at once. */
  refuse(refusing: boolean): void {
    this.#refusing = refusing;
  }

  async close(): Promise<void> {
    this.sever();
    await new Promise((resolve) => this.#server.close(resolve));
  }

  #accept(client: net.Socket): void {
    if (this.#refusing) {
      client.destroy();
      return;
    }
    const upstream = net.connect(this.#target(), '127.0.0.1');
    for (const socket of [client, upstream]) {
      this.#sockets.add(socket);
      socket.on('close', () => {
        this.#sockets.delete(socket);
        (socket === client ? upstream : client).destroy();
      });
      socket.on('error', () => undefined);
    }
    client.pipe(upstream);
    upstream.pipe(client);
  }
}
