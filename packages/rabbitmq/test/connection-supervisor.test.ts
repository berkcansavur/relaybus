import { describe, expect, it, vi } from 'vitest';
import { createRetryPolicy } from '@relaybus/core';
import type { AmqpConnection } from '../src/amqp-types.js';
import { ConnectionSupervisor } from '../src/connection-supervisor.js';
import { RabbitMqUnavailableError } from '../src/errors.js';
import { FakeConnection, createLogger, deferred, waitFor } from './fakes.js';

const FAST_RECONNECT = createRetryPolicy({
  attempts: 1,
  initialDelay: 5,
  maxDelay: 5,
  jitter: 'none',
});

function createSupervisor(
  overrides: {
    connect?: () => Promise<AmqpConnection>;
    reconnect?: typeof FAST_RECONNECT | undefined;
    onConnected?: (connection: AmqpConnection) => Promise<void>;
  } = {},
) {
  const connections: FakeConnection[] = [];
  const logger = createLogger();
  const onDisconnected = vi.fn();
  const onConnected = vi.fn(overrides.onConnected ?? (async () => undefined));
  const connect = vi.fn(
    overrides.connect ??
      (async () => {
        const connection = new FakeConnection();
        connections.push(connection);
        return connection;
      }),
  );
  const supervisor = new ConnectionSupervisor({
    connect,
    reconnect: 'reconnect' in overrides ? overrides.reconnect : FAST_RECONNECT,
    onConnected,
    onDisconnected,
    logger,
    random: () => 0.5,
  });
  return { supervisor, connections, logger, onDisconnected, onConnected, connect };
}

describe('ConnectionSupervisor', () => {
  it('connects, becomes ready after onConnected and ignores a second start', async () => {
    const { supervisor, connect, onConnected } = createSupervisor();
    expect(supervisor.isConnected()).toBe(false);
    await supervisor.start(true);
    await supervisor.start(true);
    expect(supervisor.isConnected()).toBe(true);
    expect(connect).toHaveBeenCalledTimes(1);
    expect(onConnected).toHaveBeenCalledTimes(1);
    await supervisor.close(async () => undefined);
  });

  it('fails startup when the broker is unreachable and may be started again', async () => {
    let reachable = false;
    const { supervisor, connections } = createSupervisor({
      connect: async () => {
        if (!reachable) {
          throw new Error('ECONNREFUSED');
        }
        const connection = new FakeConnection();
        connections.push(connection);
        return connection;
      },
    });
    await expect(supervisor.start(true)).rejects.toThrow('ECONNREFUSED');
    expect(supervisor.isConnected()).toBe(false);
    reachable = true;
    await supervisor.start(true);
    expect(supervisor.isConnected()).toBe(true);
    await supervisor.close(async () => undefined);
  });

  it('keeps retrying in the background when startup may fail', async () => {
    let attempts = 0;
    const connections: FakeConnection[] = [];
    const { supervisor, logger } = createSupervisor({
      connect: async () => {
        attempts += 1;
        if (attempts < 3) {
          throw new Error('ECONNREFUSED');
        }
        const connection = new FakeConnection();
        connections.push(connection);
        return connection;
      },
    });
    await supervisor.start(false);
    expect(supervisor.isConnected()).toBe(false);
    await waitFor(() => supervisor.isConnected());
    expect(attempts).toBe(3);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('[ConnectionSupervisor](start) Broker unreachable'),
    );
    expect(logger.log).toHaveBeenCalledWith(
      expect.stringContaining('[ConnectionSupervisor](reconnect) Reconnected | attempt=2'),
    );
    await supervisor.close(async () => undefined);
  });

  it('discards a connection whose setup failed', async () => {
    const { supervisor, connections, onDisconnected } = createSupervisor({
      onConnected: async () => {
        throw new Error('PRECONDITION_FAILED');
      },
    });
    await expect(supervisor.start(true)).rejects.toThrow('PRECONDITION_FAILED');
    expect(connections[0]!.close).toHaveBeenCalledTimes(1);
    expect(onDisconnected).toHaveBeenCalledTimes(1);
    expect(supervisor.isConnected()).toBe(false);
  });

  it('still reports the setup error when the discarded connection fails to close', async () => {
    const { supervisor, connections } = createSupervisor({
      onConnected: async () => {
        connections[0]!.close.mockRejectedValue(new Error('already closed'));
        throw new Error('setup failed');
      },
    });
    await expect(supervisor.start(true)).rejects.toThrow('setup failed');
  });

  it('retries after a failed setup until it works', async () => {
    let setups = 0;
    const { supervisor, connections } = createSupervisor({
      onConnected: async () => {
        setups += 1;
        if (setups === 1) {
          throw new Error('first setup fails');
        }
      },
    });
    await supervisor.start(false);
    await waitFor(() => supervisor.isConnected());
    expect(connections).toHaveLength(2);
    await supervisor.close(async () => undefined);
  });

  it('reconnects after the connection closes and reports the loss', async () => {
    const { supervisor, connections, onDisconnected, onConnected, logger } = createSupervisor();
    await supervisor.start(true);
    connections[0]!.emit('close', new Error('CONNECTION_FORCED'));
    expect(supervisor.isConnected()).toBe(false);
    expect(onDisconnected).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledWith(
      '[ConnectionSupervisor](handleLoss) Connection lost | reason=CONNECTION_FORCED',
    );
    await waitFor(() => supervisor.isConnected());
    expect(onConnected).toHaveBeenCalledTimes(2);
    connections[1]!.emit('close');
    expect(logger.warn).toHaveBeenCalledWith(
      '[ConnectionSupervisor](handleLoss) Connection lost | reason=closed by peer',
    );
    await waitFor(() => supervisor.isConnected());
    await supervisor.close(async () => undefined);
  });

  it('ignores close events of connections it no longer tracks', async () => {
    const { supervisor, connections, onDisconnected } = createSupervisor();
    await supervisor.start(true);
    connections[0]!.emit('close');
    await waitFor(() => supervisor.isConnected());
    onDisconnected.mockClear();
    connections[0]!.emit('close');
    expect(onDisconnected).not.toHaveBeenCalled();
    expect(supervisor.isConnected()).toBe(true);
    await supervisor.close(async () => undefined);
  });

  it('logs connection errors without reconnecting by itself', async () => {
    const { supervisor, connections, logger } = createSupervisor();
    await supervisor.start(true);
    connections[0]!.emit('error', new Error('socket hang up'));
    expect(logger.warn).toHaveBeenCalledWith(
      '[ConnectionSupervisor](establish) Connection error | error=socket hang up',
    );
    expect(supervisor.isConnected()).toBe(true);
    await supervisor.close(async () => undefined);
  });

  it('stays disconnected after a loss when reconnection is disabled', async () => {
    const { supervisor, connections, connect } = createSupervisor({ reconnect: undefined });
    await supervisor.start(true);
    connections[0]!.emit('close');
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(supervisor.isConnected()).toBe(false);
    expect(connect).toHaveBeenCalledTimes(1);
    await supervisor.close(async () => undefined);
  });

  it('fails a setup that finishes after the connection was lost', async () => {
    const setup = deferred();
    const { supervisor, connections, onConnected } = createSupervisor({
      onConnected: () => setup.promise,
    });
    const starting = supervisor.start(true);
    await waitFor(() => connections.length === 1);
    connections[0]!.emit('close', new Error('dropped mid-setup'));
    setup.resolve();
    await expect(starting).rejects.toThrow(RabbitMqUnavailableError);
    expect(onConnected).toHaveBeenCalledTimes(1);
  });

  it('recovers from a channel failure by replacing the connection', async () => {
    const { supervisor, connections, onDisconnected } = createSupervisor();
    supervisor.recover('nothing to recover');
    await supervisor.start(true);
    supervisor.recover('publisher channel closed');
    expect(connections[0]!.close).toHaveBeenCalledTimes(1);
    expect(onDisconnected).toHaveBeenCalledTimes(1);
    expect(supervisor.isConnected()).toBe(false);
    await waitFor(() => supervisor.isConnected());
    expect(connections).toHaveLength(2);
    await supervisor.close(async () => undefined);
  });

  it('survives a connection that refuses to close during recovery', async () => {
    const { supervisor, connections } = createSupervisor();
    await supervisor.start(true);
    connections[0]!.close.mockRejectedValueOnce(new Error('already closed'));
    supervisor.recover('consumer cancelled');
    await waitFor(() => supervisor.isConnected());
    await supervisor.close(async () => undefined);
  });

  it('stops reconnecting on close, even in the middle of a backoff', async () => {
    const slow = createRetryPolicy({ attempts: 1, initialDelay: 60_000, jitter: 'none' });
    const { supervisor, connections, connect } = createSupervisor({ reconnect: slow });
    await supervisor.start(true);
    connections[0]!.emit('close');
    const beforeDisconnect = vi.fn(async () => undefined);
    await supervisor.close(beforeDisconnect);
    expect(beforeDisconnect).toHaveBeenCalledTimes(1);
    expect(connect).toHaveBeenCalledTimes(1);
  });

  it('drains before closing the connection and closes it even when draining fails', async () => {
    const { supervisor, connections } = createSupervisor();
    await supervisor.start(true);
    const order: string[] = [];
    connections[0]!.close.mockImplementation(async () => {
      order.push('connection closed');
    });
    await expect(
      supervisor.close(async () => {
        order.push('drained');
        throw new Error('drain failed');
      }),
    ).rejects.toThrow('drain failed');
    expect(order).toEqual(['drained', 'connection closed']);
    expect(supervisor.isConnected()).toBe(false);
  });

  it('tolerates a connection that fails to close', async () => {
    const { supervisor, connections } = createSupervisor();
    await supervisor.start(true);
    connections[0]!.close.mockRejectedValue(new Error('boom'));
    await supervisor.close(async () => undefined);
  });

  it('closes cleanly when it never connected and refuses to start afterwards', async () => {
    const { supervisor } = createSupervisor();
    await supervisor.close(async () => undefined);
    await expect(supervisor.start(true)).rejects.toThrow(RabbitMqUnavailableError);
  });

  it('closes a connection that was established while closing', async () => {
    const gate = deferred<void>();
    let attempts = 0;
    const connections: FakeConnection[] = [];
    const { supervisor } = createSupervisor({
      connect: async () => {
        attempts += 1;
        if (attempts === 1) {
          throw new Error('down');
        }
        await gate.promise;
        const connection = new FakeConnection();
        connections.push(connection);
        return connection;
      },
    });
    await supervisor.start(false);
    await waitFor(() => attempts === 2);
    const closing = supervisor.close(async () => undefined);
    gate.resolve();
    await closing;
    expect(connections[0]!.close).toHaveBeenCalledTimes(1);
    expect(supervisor.isConnected()).toBe(false);
  });

  it('stops looping when it is closed while an attempt is failing', async () => {
    const gate = deferred<void>();
    let attempts = 0;
    const { supervisor } = createSupervisor({
      connect: async () => {
        attempts += 1;
        if (attempts === 2) {
          await gate.promise;
        }
        throw new Error('down');
      },
    });
    await supervisor.start(false);
    await waitFor(() => attempts === 2);
    const closing = supervisor.close(async () => undefined);
    gate.resolve();
    await closing;
    expect(attempts).toBe(2);
  });
});
