import { ClaimLoop } from './dispatch/claim-loop.js';
import { ClaimPlanner } from './dispatch/claim-planner.js';
import { ShutdownLatch, type DispatcherDependencies } from './dispatch/dispatch-context.js';
import { DeliveryExecutor } from './dispatch/delivery-executor.js';
import { DeliveryOutcomes } from './dispatch/delivery-outcomes.js';
import { HandlerInvocation } from './dispatch/handler-invocation.js';
import { InFlightDeliveries } from './dispatch/in-flight-deliveries.js';
import { SettlementRecovery } from './dispatch/settlement-recovery.js';
import { CompletionBatcher } from './completion-batcher.js';
import { DeliveryTimers } from './delivery-timers.js';
import { DispatcherStoppedError } from './errors.js';
import { ShutdownDeadline } from './shutdown-deadline.js';

export type { DispatcherDependencies, DispatcherSettings } from './dispatch/dispatch-context.js';

/**
 * Claims due deliveries and runs their handlers: bounded concurrency, lease
 * heartbeats with self-fencing, fenced completion, retries with backoff and
 * dead-lettering. A facade over its parts: the {@link ClaimLoop} decides when to claim, the
 * {@link DeliveryExecutor} runs what was claimed, {@link DeliveryOutcomes} records how it ended
 * and {@link SettlementRecovery} retries what the store failed to record.
 */
export class Dispatcher {
  readonly #dependencies: DispatcherDependencies;
  readonly #inFlight = new InFlightDeliveries();
  readonly #shutdown = new ShutdownLatch();
  readonly #timers: DeliveryTimers;
  readonly #completions: CompletionBatcher | undefined;
  readonly #recovery: SettlementRecovery;
  readonly #executor: DeliveryExecutor;
  readonly #loop: ClaimLoop;
  #looping: Promise<void> | undefined;
  #unwatch: (() => Promise<void>) | undefined;
  #watching: Promise<void> | undefined;

  constructor(dependencies: DispatcherDependencies) {
    this.#dependencies = dependencies;
    this.#timers = new DeliveryTimers(dependencies.clock);
    this.#completions = this.#createCompletionBatcher();
    this.#recovery = new SettlementRecovery(dependencies, this.#shutdown);
    this.#executor = this.#createExecutor();
    this.#loop = new ClaimLoop({
      dependencies,
      inFlight: this.#inFlight,
      planner: new ClaimPlanner(this.#inFlight, dependencies.settings.batchSize),
      shutdown: this.#shutdown,
      start: (delivery) => this.#executor.start(delivery),
    });
  }

  #createCompletionBatcher(): CompletionBatcher | undefined {
    const { store, clock, settings } = this.#dependencies;
    const { completionBatchWindowMs: windowMs = 0, completionBatchSize: maxBatchSize = 1 } =
      settings;
    if (store.completeMany === undefined || !(windowMs > 0)) {
      return undefined;
    }
    return new CompletionBatcher(
      { completeMany: (leases, now) => store.completeMany!(leases, now) },
      clock,
      { windowMs, maxBatchSize },
      () => this.#inFlight.size,
    );
  }

  #createExecutor(): DeliveryExecutor {
    const dependencies = this.#dependencies;
    const outcomes = new DeliveryOutcomes({
      dependencies,
      recovery: this.#recovery,
      shutdown: this.#shutdown,
      completions: this.#completions,
      sleep: dependencies.sleep ?? ((milliseconds) => this.#timers.sleep(milliseconds)),
    });
    return new DeliveryExecutor({
      dependencies,
      inFlight: this.#inFlight,
      recovery: this.#recovery,
      outcomes,
      invocation: new HandlerInvocation(dependencies, this.#timers),
      timers: this.#timers,
      shutdown: this.#shutdown,
      onSettled: (delivery, succeeded) => this.#loop.onSettled(delivery, succeeded),
    });
  }

  get isRunning(): boolean {
    return this.#loop.isRunning;
  }

  get inFlightCount(): number {
    return this.#inFlight.size;
  }

  /**
   * True while the store reports new deliveries to this dispatcher by itself (see
   * {@link EventStore.watchNewDeliveries}), from every process: a publisher then has no reason to wake it.
   */
  get watchesNewDeliveries(): boolean {
    return this.#unwatch !== undefined;
  }

  start(): void {
    if (this.#loop.isRunning) {
      return;
    }
    this.#loop.open();
    this.#completions?.setClosing(false);
    this.#watching = this.#watchNewDeliveries();
    this.#looping = this.#loop.run();
  }

  /**
   * Asks for a claim soon, for example right after a publish: at once after a quiet period, otherwise
   * at the end of a short cooldown that merges the wake-ups arriving meanwhile into one claim.
   */
  wake(): void {
    this.#loop.wake();
  }

  /**
   * Starts listening to the store's reports of new deliveries. Polling stays the safety net: a store
   * that cannot listen, or fails to, only costs latency, so a failure is reported and otherwise ignored.
   */
  async #watchNewDeliveries(): Promise<void> {
    const { store, notifier } = this.#dependencies;
    try {
      const unwatch = await store.watchNewDeliveries?.(() => this.wake());
      if (unwatch !== undefined && !this.#loop.isRunning) {
        await unwatch();
        return;
      }
      this.#unwatch = unwatch;
      if (unwatch !== undefined) {
        // Deliveries committed after the first claim and before listening began were reported to nobody.
        this.wake();
      }
    } catch (error) {
      notifier.failure('watch', error);
    }
  }

  /** Stops listening to the store's reports of new deliveries. */
  async #stopWatching(): Promise<void> {
    await this.#watching;
    const unwatch = this.#unwatch;
    this.#unwatch = undefined;
    try {
      await unwatch?.();
    } catch (error) {
      this.#dependencies.notifier.failure('watch', error);
    }
  }

  /**
   * Stops claiming and waits for running handlers until the drain point of `deadline` (default: the
   * shutdown timeout from now), then aborts the rest and releases their deliveries for another
   * process. Deliveries still in flight when the deadline itself passes are abandoned: their leases
   * expire and they run again elsewhere. Resolves with how many were abandoned.
   */
  async stop(deadline?: ShutdownDeadline): Promise<number> {
    const { clock, settings } = this.#dependencies;
    const own = deadline ?? new ShutdownDeadline(clock, settings.shutdownTimeoutMs);
    try {
      return await this.#stopWithin(own);
    } finally {
      if (deadline === undefined) {
        own.dispose();
      }
    }
  }

  async #stopWithin(deadline: ShutdownDeadline): Promise<number> {
    if (!this.#loop.isRunning) {
      // Batches run by hand (`runOnce`) leave settlements to recover without ever starting the loop.
      await this.#recovery.close(deadline);
      return 0;
    }
    this.#loop.halt();
    this.#completions?.setClosing(true);
    this.#loop.interrupt();
    await deadline.race(this.#stopWatching());
    await deadline.race(this.#looping!);
    const settled = this.#inFlight.allSettled();
    if ((await deadline.raceDrain(settled)) === 'timeout') {
      this.#inFlight.abortAll(new DispatcherStoppedError('relaybus dispatcher stopped'));
      if ((await deadline.race(settled)) === 'timeout') {
        return this.#abandon();
      }
    }
    await this.#recovery.close(deadline);
    await deadline.race(this.#relinquish());
    return 0;
  }

  /**
   * The deadline passed with deliveries still in flight (aborted, their release not answered): forgets
   * them and every settlement still to retry. Whatever they release or complete from now on may reach a
   * closed store; a delivery whose state change is lost stays leased until the lease expires and then
   * runs again (at-least-once).
   */
  #abandon(): number {
    this.#shutdown.abandon();
    // Their leases must run out, so that another process takes the deliveries.
    this.#executor.abandon();
    this.#timers.cancelAll();
    this.#recovery.abandon();
    return this.#inFlight.size;
  }

  /** Gives lease groups the owner no longer works on back to the store, so other workers need not wait for expiry. */
  async #relinquish(): Promise<void> {
    const { store, clock, notifier, settings } = this.#dependencies;
    try {
      await store.relinquish?.(settings.owner, clock.now());
    } catch (error) {
      notifier.failure('relinquish', error);
    }
  }

  /** Claims one batch and resolves when every claimed delivery settled. Returns how many ran. */
  async processBatch(): Promise<number> {
    const { started } = await this.#loop.claimAndStart();
    await Promise.allSettled(started.map((delivery) => delivery.settled));
    return started.length;
  }
}
