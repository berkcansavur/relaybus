import { toErrorInfo } from './errors.js';
import type { FailureStage, RelayBusLogger, RelayBusObserver } from './observer.js';

type ObserverMethod = Exclude<keyof RelayBusObserver, 'onFailure'>;
type ObserverArgument<TMethod extends keyof RelayBusObserver> = Parameters<
  NonNullable<RelayBusObserver[TMethod]>
>[0];

/** Fans hook calls out to every observer, isolating the bus from observer bugs. */
export class Notifier {
  readonly #observers: readonly RelayBusObserver[];
  readonly #logger: RelayBusLogger;

  constructor(observers: readonly RelayBusObserver[], logger: RelayBusLogger) {
    this.#observers = observers;
    this.#logger = logger;
  }

  notify<TMethod extends ObserverMethod>(
    method: TMethod,
    argument: ObserverArgument<TMethod>,
  ): void {
    for (const observer of this.#observers) {
      try {
        (observer[method] as ((value: ObserverArgument<TMethod>) => void) | undefined)?.call(
          observer,
          argument,
        );
      } catch (error) {
        this.#reportObserverFailure(observer, error);
      }
    }
  }

  failure(stage: FailureStage, error: unknown): void {
    const errorInfo = toErrorInfo(error);
    this.#logger.error(
      `[Notifier](failure) relaybus ${stage} failed | ${errorInfo.name}: ${errorInfo.message}`,
    );
    for (const observer of this.#observers) {
      try {
        observer.onFailure?.({ stage, error: errorInfo });
      } catch (observerError) {
        this.#logObserverFailure(observerError);
      }
    }
  }

  #reportObserverFailure(failedObserver: RelayBusObserver, error: unknown): void {
    this.#logObserverFailure(error);
    const errorInfo = toErrorInfo(error);
    for (const observer of this.#observers) {
      if (observer === failedObserver) {
        continue;
      }
      try {
        observer.onFailure?.({ stage: 'observer', error: errorInfo });
      } catch (observerError) {
        this.#logObserverFailure(observerError);
      }
    }
  }

  #logObserverFailure(error: unknown): void {
    const errorInfo = toErrorInfo(error);
    this.#logger.warn(
      `[Notifier](notify) relaybus observer threw | ${errorInfo.name}: ${errorInfo.message}`,
    );
  }
}
