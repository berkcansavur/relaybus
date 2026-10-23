import { EventEmitter } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { NotificationListener } from '../src/notification-listener.js';

class FakeConnection extends EventEmitter {
  readonly query = vi.fn(async (_sql: string) => ({ rows: [] }));
  readonly end = vi.fn(async () => {
    this.emit('end');
  });
}

function createListener() {
  const connections: FakeConnection[] = [];
  const connect = vi.fn(async () => {
    const connection = new FakeConnection();
    connections.push(connection);
    return connection;
  });
  const listener = vi.fn();
  const logger = { log: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const notifications = new NotificationListener(
    connect,
    'public.relaybus_deliveries',
    listener,
    logger,
  );
  return { notifications, connections, connect, listener, logger };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('NotificationListener', () => {
  it('listens on its channel, reports every notification and ends its connection when closed', async () => {
    const { notifications, connections, listener, logger } = createListener();
    await notifications.start();
    expect(connections[0]!.query).toHaveBeenCalledWith('LISTEN "public.relaybus_deliveries"');
    connections[0]!.emit('notification', {});
    connections[0]!.emit('notification', {});
    expect(listener).toHaveBeenCalledTimes(2);
    await notifications.close();
    await notifications.close();
    expect(connections[0]!.end).toHaveBeenCalledTimes(1);
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('replaces a lost connection after a pause and reports what it may have missed', async () => {
    vi.useFakeTimers();
    const { notifications, connections, connect, listener, logger } = createListener();
    await notifications.start();
    connections[0]!.end.mockRejectedValueOnce(new Error('already gone'));
    connections[0]!.emit('error', new Error('terminating connection'));
    connections[0]!.emit('end');
    expect(connections[0]!.end).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('[NotificationListener](start) Listening connection lost'),
    );
    // A first reconnection fails, the next one succeeds.
    // Whatever a driver rejects with is reported by its text.
    connect.mockRejectedValueOnce('connection refused');
    await vi.advanceTimersByTimeAsync(1_000);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        '[NotificationListener](scheduleReconnect) Listening again failed, retrying | channel=public.relaybus_deliveries | error=connection refused',
      ),
    );
    expect(listener).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(connections).toHaveLength(2);
    expect(listener).toHaveBeenCalledTimes(1);
    await notifications.close();
    expect(connections[1]!.end).toHaveBeenCalledTimes(1);
  });

  it('treats a connection that just ends as lost too', async () => {
    vi.useFakeTimers();
    const { notifications, connections, logger } = createListener();
    await notifications.start();
    connections[0]!.emit('end');
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('error=connection ended'));
    await notifications.close();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(connections).toHaveLength(1);
  });

  it('stops retrying once closed while a reconnection fails', async () => {
    vi.useFakeTimers();
    const { notifications, connections, connect } = createListener();
    await notifications.start();
    connections[0]!.emit('error', new Error('gone'));
    let rejectConnect!: (error: Error) => void;
    connect.mockImplementationOnce(
      () => new Promise((_, reject) => void (rejectConnect = reject)) as never,
    );
    await vi.advanceTimersByTimeAsync(1_000);
    await notifications.close();
    rejectConnect(new Error('refused'));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(connect).toHaveBeenCalledTimes(2);
  });

  it('ends a connection that comes back after it was closed, without reporting', async () => {
    vi.useFakeTimers();
    const { notifications, connections, connect, listener } = createListener();
    await notifications.start();
    connections[0]!.emit('error', new Error('gone'));
    let resolveConnect!: (connection: FakeConnection) => void;
    connect.mockImplementationOnce(
      () => new Promise((resolve) => void (resolveConnect = resolve)) as never,
    );
    await vi.advanceTimersByTimeAsync(1_000);
    await notifications.close();
    const late = new FakeConnection();
    resolveConnect(late);
    await vi.advanceTimersByTimeAsync(0);
    expect(late.end).toHaveBeenCalledTimes(1);
    expect(listener).not.toHaveBeenCalled();
  });

  it('rejects, ends the connection once and does not retry when it cannot start listening', async () => {
    vi.useFakeTimers();
    const { notifications, connections, connect, logger } = createListener();
    connect.mockImplementationOnce(async () => {
      const connection = new FakeConnection();
      connection.query.mockImplementationOnce(async () => {
        // The connection breaks while LISTEN runs: both the error and the rejection arrive.
        connection.emit('error', new Error('connection reset'));
        throw new Error('connection reset');
      });
      connections.push(connection);
      return connection;
    });
    await expect(notifications.start()).rejects.toThrow('connection reset');
    expect(connections[0]!.end).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(connect).toHaveBeenCalledTimes(1);
    expect(logger.warn).not.toHaveBeenCalled();
  });
  it('ends a reconnected connection whose LISTEN is still pending when it is closed', async () => {
    vi.useFakeTimers();
    const { notifications, connections, connect, listener, logger } = createListener();
    await notifications.start();
    connections[0]!.emit('error', new Error('gone'));
    const stalled = new FakeConnection();
    // LISTEN hangs until the connection is ended, as a stalled server would.
    stalled.query.mockImplementationOnce(
      () =>
        new Promise((_resolve, reject) => {
          stalled.end.mockImplementationOnce(async () => {
            reject(new Error('Connection terminated'));
            stalled.emit('end');
          });
        }),
    );
    connect.mockImplementationOnce(async () => {
      connections.push(stalled);
      return stalled;
    });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(stalled.query).toHaveBeenCalledTimes(1);
    await notifications.close();
    expect(stalled.end).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(connect).toHaveBeenCalledTimes(2);
    expect(listener).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });
  it('ends a connection whose LISTEN still succeeds after it was closed, without reporting', async () => {
    const { notifications, connections, connect, listener, logger } = createListener();
    let answerListen!: () => void;
    connect.mockImplementationOnce(async () => {
      const connection = new FakeConnection();
      // The server answers LISTEN even though the connection is being ended meanwhile.
      connection.query.mockImplementationOnce(
        () => new Promise((resolve) => void (answerListen = () => resolve({ rows: [] }))),
      );
      connection.end.mockImplementationOnce(async () => undefined);
      connections.push(connection);
      return connection;
    });
    const starting = notifications.start();
    await vi.waitFor(() => expect(connections[0]!.query).toHaveBeenCalledTimes(1));
    await notifications.close();
    answerListen();
    await starting;
    expect(connections[0]!.end).toHaveBeenCalledTimes(1);
    expect(listener).not.toHaveBeenCalled();
    expect(logger.warn).not.toHaveBeenCalled();
  });
});
