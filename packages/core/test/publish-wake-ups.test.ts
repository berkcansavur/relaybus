import { describe, expect, it, vi } from 'vitest';
import { FakeClock } from '@relaybus/testing';
import { PublishWakeUps } from '../src/bus/publish-wake-ups.js';
import { OrderPlaced, createTestBus, deferred, waitFor } from './helpers.js';

function createWakeUps() {
  const clock = new FakeClock(0, { controlTimers: true });
  const wake = vi.fn();
  const wakeUps = new PublishWakeUps({ clock, wake });
  const wokenAt: number[] = [];
  wake.mockImplementation(() => wokenAt.push(clock.now().getTime()));
  return { clock, wakeUps, wokenAt };
}

describe('PublishWakeUps', () => {
  it('wakes 25, 100 and 250 ms after a publish, one timer at a time', () => {
    const { clock, wakeUps, wokenAt } = createWakeUps();
    wakeUps.afterPublish(false);
    expect(clock.pendingTimers).toBe(1);
    clock.advance(1_000);
    expect(wokenAt).toEqual([25, 100, 250]);
    expect(clock.pendingTimers).toBe(0);
  });

  it('follows the latest publish, so every publish is woken for 25 ms after it', () => {
    const { clock, wakeUps, wokenAt } = createWakeUps();
    wakeUps.afterPublish(false);
    clock.advance(10);
    wakeUps.afterPublish(false);
    expect(clock.pendingTimers).toBe(1);
    clock.advance(80);
    wakeUps.afterPublish(false);
    clock.advance(1_000);
    expect(wokenAt).toEqual([25, 35, 110, 115, 190, 340]);
  });

  it('does not let a later wake-up of an earlier publish delay the next publish', () => {
    const { clock, wakeUps, wokenAt } = createWakeUps();
    wakeUps.afterPublish(false);
    clock.advance(101);
    wakeUps.afterPublish(false);
    expect(clock.pendingTimers).toBe(1);
    clock.advance(1_000);
    expect(wokenAt).toEqual([25, 100, 126, 201, 351]);
  });

  it('wakes at once, with no timer, for a publish whose transaction has committed', () => {
    const { clock, wakeUps, wokenAt } = createWakeUps();
    wakeUps.afterPublish(true);
    expect(wokenAt).toEqual([0]);
    expect(clock.pendingTimers).toBe(0);
  });

  it('stops waking once cancelled', () => {
    const { clock, wakeUps, wokenAt } = createWakeUps();
    wakeUps.afterPublish(false);
    clock.advance(30);
    wakeUps.cancel();
    wakeUps.cancel();
    clock.advance(1_000);
    expect(wokenAt).toEqual([25]);
    expect(clock.pendingTimers).toBe(0);
  });
});

describe('waking the dispatcher after a commit the bus cannot see', () => {
  function startBus() {
    const clock = new FakeClock(0, { controlTimers: true });
    const test = createTestBus({ clock, dispatcher: { pollInterval: '10s' } });
    const handled: number[] = [];
    test.bus.subscribe(OrderPlaced, () => void handled.push(clock.now().getTime()), {
      id: 'h',
    });
    return { ...test, clock, handled };
  }

  /** Publishes in a transaction of the caller's that commits when `commit` resolves. */
  async function publishAndHoldCommit(test: ReturnType<typeof startBus>) {
    const commit = deferred();
    const committed = test.store.transaction(async (transaction) => {
      await test.bus.publish(new OrderPlaced({ orderId: '1' }), { transaction });
      await commit.promise;
    });
    await waitFor(() => test.clock.pendingTimers > 1);
    return { commit: commit.resolve, committed };
  }

  it('finds a delivery whose commit came 80 ms after the publish at the 100 ms wake-up', async () => {
    const test = startBus();
    await test.bus.start();
    const { commit, committed } = await publishAndHoldCommit(test);
    test.clock.advance(80);
    commit();
    await committed;
    test.clock.advance(20);
    await waitFor(() => test.handled.length === 1);
    expect(test.handled).toEqual([100]);
    await test.bus.stop();
  });

  it('runs a delivery committed after the last wake-up as soon as the caller calls bus.wake()', async () => {
    const test = startBus();
    await test.bus.start();
    const { commit, committed } = await publishAndHoldCommit(test);
    test.clock.advance(300);
    commit();
    await committed;
    test.bus.wake();
    await waitFor(() => test.handled.length === 1);
    expect(test.handled).toEqual([300]);
    await test.bus.stop();
    expect(test.clock.pendingTimers).toBe(0);
  });
});
