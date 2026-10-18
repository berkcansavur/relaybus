import { describe, expect, it } from 'vitest';
import { FakeClock, InMemoryTransport, createEnvelope } from '@relaybus/testing';

describe('FakeClock', () => {
  it('starts at a fixed instant and moves only when told to', () => {
    const clock = new FakeClock();
    expect(clock.now().toISOString()).toBe('2026-01-01T00:00:00.000Z');
    expect(clock.advance(1_500).toISOString()).toBe('2026-01-01T00:00:01.500Z');
    expect(clock.set('2027-05-05T05:05:05.000Z').toISOString()).toBe('2027-05-05T05:05:05.000Z');
    expect(new FakeClock(0).now().getTime()).toBe(0);
  });
});

describe('InMemoryTransport', () => {
  it('records publishes, simulates outages and redelivers to subscribers', async () => {
    const transport = new InMemoryTransport();
    expect(transport.name).toBe('memory');
    await transport.connect();
    expect(transport.connected).toBe(true);
    const envelope = createEnvelope();
    transport.failNextPublishes(1);
    await expect(transport.publish(envelope, 'topic')).rejects.toThrow(
      'InMemoryTransport refused publish to topic',
    );
    await transport.publish(envelope, 'topic');
    expect(transport.published).toEqual([{ destination: 'topic', envelope }]);
    let calls = 0;
    await transport.subscribe(['topic', 'other'], async () => {
      calls += 1;
      if (calls < 3) {
        throw new Error('not yet');
      }
    });
    await transport.subscribe(['topic'], async () => undefined);
    expect(await transport.deliver('topic', envelope, 2)).toBe(4);
    await transport.close();
    expect(transport.closed).toBe(true);
  });
});
