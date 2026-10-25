import { createServer, type Socket } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { RabbitMqUnavailableError } from '../src/errors.js';
import { RabbitMqTransport } from '../src/rabbitmq-transport.js';
import { FakeConnection, createDelivery, createEnvelope, createLogger, waitFor } from './fakes.js';
import { createTransport } from './support/transport-setup.js';

describe('RabbitMqTransport recovery', () => {
  it('rebuilds channels, topology and the consumer after the connection drops', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    const handler = vi.fn(async () => undefined);
    await transport.subscribe(['orders'], handler);
    connections[0]!.emit('close', new Error('CONNECTION_FORCED'));
    expect(transport.isConnected()).toBe(false);
    await expect(transport.publish(createEnvelope(), 'orders')).rejects.toThrow(
      RabbitMqUnavailableError,
    );
    await waitFor(() => transport.isConnected());
    expect(connections).toHaveLength(2);
    expect(connections[1]!.consumerChannel.consume).toHaveBeenCalledTimes(1);
    await transport.publish(createEnvelope(), 'orders');
    connections[1]!.consumerChannel.deliver!(createDelivery());
    await waitFor(() => handler.mock.calls.length === 1);
    await transport.close();
  });

  it('reconnects when the publisher channel closes on its own', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    connections[0]!.publisher.emit('close');
    expect(transport.isConnected()).toBe(false);
    await waitFor(() => transport.isConnected());
    expect(connections[0]!.close).toHaveBeenCalled();
    expect(connections).toHaveLength(2);
    await transport.close();
  });

  it('reconnects when the consumer channel closes or the consumer is cancelled', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    connections[0]!.consumerChannel.emit('close');
    await waitFor(() => connections.length === 2 && transport.isConnected());
    connections[1]!.consumerChannel.deliver!(null);
    await waitFor(() => connections.length === 3 && transport.isConnected());
    await transport.close();
  });

  it('retries when restoring the topology fails', async () => {
    const { transport, connections, logger } = createTransport();
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    const original = connections.push.bind(connections);
    connections.push = (...added: FakeConnection[]) => {
      if (connections.length === 1) {
        added[0]!.createChannel.mockRejectedValueOnce(new Error('channel limit'));
      }
      return original(...added);
    };
    connections[0]!.emit('close');
    await waitFor(() => connections.length === 3 && transport.isConnected());
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('Attempt failed | attempt=1'));
    await transport.close();
  });
});

describe('RabbitMqTransport close', () => {
  it('closes channels and connection once, and may be called again', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    await transport.subscribe(['orders'], async () => undefined);
    await transport.close();
    await transport.close();
    expect(connections[0]!.publisher.close).toHaveBeenCalledTimes(1);
    expect(connections[0]!.consumerChannel.close).toHaveBeenCalledTimes(1);
    expect(connections[0]!.close).toHaveBeenCalledTimes(1);
  });

  it('closes without ever having connected', async () => {
    const { transport } = createTransport();
    await transport.close();
    await expect(transport.connect()).rejects.toThrow(RabbitMqUnavailableError);
  });

  it('survives a publisher channel that fails to close', async () => {
    const { transport, connections } = createTransport();
    await transport.connect();
    connections[0]!.publisher.close.mockRejectedValue(new Error('boom'));
    await transport.close();
  });
});

describe('RabbitMqTransport forced close', () => {
  /** A connection whose closing handshake never completes, with the socket amqplib keeps under it. */
  function hangingConnection(stream?: { destroy: () => void }): FakeConnection {
    const connection = new FakeConnection();
    connection.close.mockImplementation(() => new Promise<void>(() => undefined));
    Object.assign(connection, { connection: stream === undefined ? {} : { stream } });
    return connection;
  }

  it('destroys the socket of its connection when the shutdown deadline passes', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    const transport = new RabbitMqTransport({ connect: async () => connection });
    await transport.connect();
    const deadline = new AbortController();
    void transport.close({ signal: deadline.signal });
    // The channels are closed and the closing handshake of the connection hangs.
    await waitFor(() => connection.close.mock.calls.length === 1);
    expect(destroy).not.toHaveBeenCalled();
    deadline.abort();
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('destroys the socket when the deadline passes while consumers still drain', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    const transport = new RabbitMqTransport({
      connect: async () => connection,
      consumerGroup: 'billing',
    });
    await transport.connect();
    await transport.subscribe(['orders'], () => new Promise<void>(() => undefined));
    const consumerChannel = connection.consumerChannel;
    consumerChannel.cancel.mockImplementation(() => new Promise(() => undefined));
    const deadline = new AbortController();
    void transport.close({ signal: deadline.signal });
    await waitFor(() => consumerChannel.cancel.mock.calls.length === 1);
    deadline.abort();
    expect(destroy).toHaveBeenCalledTimes(1);
    expect(connection.close).not.toHaveBeenCalled();
  });

  it('destroys it at once when the deadline passed before close was called', async () => {
    const destroy = vi.fn();
    const transport = new RabbitMqTransport({
      connect: async () => hangingConnection({ destroy }),
    });
    await transport.connect();
    void transport.close({ signal: AbortSignal.abort() });
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('forces nothing when there is no connection or no socket to destroy', async () => {
    const withoutSocket = new RabbitMqTransport({ connect: async () => hangingConnection() });
    await withoutSocket.connect();
    void withoutSocket.close({ signal: AbortSignal.abort() });
    const plain = new RabbitMqTransport({ connect: async () => new FakeConnection() });
    await plain.connect();
    await expect(plain.close({ signal: AbortSignal.abort() })).resolves.toBeUndefined();
    const unconnected = new RabbitMqTransport({ connect: async () => new FakeConnection() });
    await expect(unconnected.close({ signal: AbortSignal.abort() })).resolves.toBeUndefined();
  });

  it('cuts a connection that opens only after a forced close', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    let open!: (opened: FakeConnection) => void;
    const transport = new RabbitMqTransport({
      connect: () => new Promise<FakeConnection>((resolve) => (open = resolve)),
    });
    const connecting = transport.connect();
    await transport.close({ signal: AbortSignal.abort() });
    open(connection);
    await expect(connecting).rejects.toThrow(RabbitMqUnavailableError);
    expect(destroy).toHaveBeenCalledTimes(1);
    expect(connection.close).not.toHaveBeenCalled();
  });

  it('listens for the error of a connection it cuts because it opened after a forced close', async () => {
    const connection = hangingConnection();
    // amqplib reports the destroyed socket as an 'error' of the connection at once.
    Object.assign(connection, {
      connection: { stream: { destroy: (error: Error) => connection.emit('error', error) } },
    });
    let open!: (opened: FakeConnection) => void;
    const transport = new RabbitMqTransport({
      connect: () => new Promise<FakeConnection>((resolve) => (open = resolve)),
      logger: createLogger(),
    });
    const connecting = transport.connect();
    await transport.close({ signal: AbortSignal.abort() });
    open(connection);
    await expect(connecting).rejects.toThrow('Transport is closed');
  });

  it('closes a connection that opens only after a graceful close', async () => {
    const connection = new FakeConnection();
    let open!: (opened: FakeConnection) => void;
    const transport = new RabbitMqTransport({
      connect: () => new Promise<FakeConnection>((resolve) => (open = resolve)),
    });
    const connecting = transport.connect();
    await transport.close();
    open(connection);
    await expect(connecting).rejects.toThrow(RabbitMqUnavailableError);
    expect(connection.close).toHaveBeenCalledTimes(1);
  });

  it('cuts a discarded connection whose closing handshake hangs', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    connection.createConfirmChannel.mockRejectedValue(new Error('channel refused'));
    const transport = new RabbitMqTransport({ connect: async () => connection });
    void transport.connect().catch(() => undefined);
    await waitFor(() => connection.close.mock.calls.length === 1);
    void transport.close({ signal: AbortSignal.abort() });
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('lets a second close cut the connection the first one is still closing', async () => {
    const destroy = vi.fn();
    const connection = hangingConnection({ destroy });
    const transport = new RabbitMqTransport({ connect: async () => connection });
    await transport.connect();
    void transport.close();
    await waitFor(() => connection.close.mock.calls.length === 1);
    void transport.close({ signal: AbortSignal.abort() });
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('destroys the socket with an error, so amqplib runs its own cleanup', async () => {
    const destroy = vi.fn();
    const transport = new RabbitMqTransport({
      connect: async () => hangingConnection({ destroy }),
    });
    await transport.connect();
    void transport.close({ signal: AbortSignal.abort() });
    expect(destroy).toHaveBeenCalledWith(expect.any(Error));
  });

  it('cuts a socket still in the AMQP handshake with a broker that never answers', async () => {
    const accepted: Socket[] = [];
    // Reads and ignores whatever the client sends, so it sees the client hang up.
    const server = createServer((socket) => {
      socket.resume();
      accepted.push(socket);
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as { port: number };
    const transport = new RabbitMqTransport({ url: `amqp://127.0.0.1:${port}` });
    const connecting = transport.connect();
    await waitFor(() => accepted.length === 1);
    const serverSaw = new Promise<void>((resolve) => accepted[0]!.once('close', () => resolve()));
    const closing = transport.close({ signal: AbortSignal.abort() });
    await expect(connecting).rejects.toThrow();
    await serverSaw;
    await closing;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }, 10_000);

  it('passes the deadline signal to a connect function', async () => {
    let received: AbortSignal | undefined;
    const transport = new RabbitMqTransport({
      connect: async ({ signal }) => {
        received = signal;
        return new FakeConnection();
      },
    });
    await transport.connect();
    expect(received!.aborted).toBe(false);
    await transport.close({ signal: AbortSignal.abort() });
    expect(received!.aborted).toBe(true);
  });

  it('stops listening to the deadline once it closed', async () => {
    const destroy = vi.fn();
    const connection = new FakeConnection();
    Object.assign(connection, { connection: { stream: { destroy } } });
    const transport = new RabbitMqTransport({ connect: async () => connection });
    await transport.connect();
    const deadline = new AbortController();
    await transport.close({ signal: deadline.signal });
    deadline.abort();
    expect(destroy).not.toHaveBeenCalled();
  });
});
