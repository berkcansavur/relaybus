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

  it('leaves timers to the platform unless asked to control them', () => {
    expect(new FakeClock().schedule).toBeUndefined();
    expect(new FakeClock(0, { controlTimers: false }).schedule).toBeUndefined();
  });

  it('runs controlled timers when their time comes, in time order and then in the order they were set', () => {
    const clock = new FakeClock(0, { controlTimers: true });
    const ran: string[] = [];
    const at = (label: string) => () => ran.push(`${label}@${clock.now().getTime()}`);
    clock.schedule!({ run: at('late'), delayMs: 300 });
    clock.schedule!({ run: at('first'), delayMs: 100 });
    clock.schedule!({ run: at('second'), delayMs: 100 });
    clock.schedule!({ run: at('negative'), delayMs: -5 });
    expect(clock.pendingTimers).toBe(4);
    expect(clock.advance(99).getTime()).toBe(99);
    expect(ran).toEqual(['negative@0']);
    clock.advance(250);
    expect(ran).toEqual(['negative@0', 'first@100', 'second@100', 'late@300']);
    expect(clock.now().getTime()).toBe(349);
    expect(clock.pendingTimers).toBe(0);
  });

  it('repeats a repeating timer every interval and stops a cancelled one', () => {
    const clock = new FakeClock(0, { controlTimers: true });
    const ticks: number[] = [];
    const cancel = clock.schedule!({
      run: () => ticks.push(clock.now().getTime()),
      delayMs: 100,
      repeat: true,
    });
    const zero: number[] = [];
    const cancelZero = clock.schedule!({
      run: () => zero.push(clock.now().getTime()),
      delayMs: 0,
      repeat: true,
    });
    clock.advance(2);
    cancelZero();
    clock.advance(298);
    expect(ticks).toEqual([100, 200, 300]);
    expect(zero).toEqual([0, 1, 2]);
    cancel();
    cancel();
    clock.advance(1_000);
    expect(ticks).toEqual([100, 200, 300]);
    expect(clock.pendingTimers).toBe(0);
  });

  it('runs a timer that another timer sets when it falls due within the same advance', () => {
    const clock = new FakeClock(0, { controlTimers: true });
    const ran: number[] = [];
    clock.schedule!({
      run: () => {
        ran.push(clock.now().getTime());
        clock.schedule!({ run: () => ran.push(clock.now().getTime()), delayMs: 50 });
      },
      delayMs: 50,
    });
    clock.advance(100);
    expect(ran).toEqual([50, 100]);
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
