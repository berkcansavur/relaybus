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
  /** Hooks at least one observer implements, so callers skip building events nobody reads. */
  readonly #implemented: ReadonlySet<keyof RelayBusObserver>;

  constructor(observers: readonly RelayBusObserver[], logger: RelayBusLogger) {
    this.#observers = observers;
    this.#logger = logger;
    this.#implemented = new Set(
      observers.flatMap((observer) => hookNames(observer) as (keyof RelayBusObserver)[]),
    );
  }

  /** True when some observer implements `method`. */
  wants(method: ObserverMethod): boolean {
    return this.#implemented.has(method);
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
      `[Notifier](logObserverFailure) relaybus observer threw | ${errorInfo.name}: ${errorInfo.message}`,
    );
  }
}

/** Names of the functions of an observer, own or inherited (an observer may be a class instance). */
function hookNames(observer: RelayBusObserver): string[] {
  const names: string[] = [];
  for (
    let target: object | null = observer;
    target !== null && target !== Object.prototype;
    target = Object.getPrototypeOf(target) as object | null
  ) {
    names.push(
      ...Object.getOwnPropertyNames(target).filter(
        (name) => typeof (observer as Record<string, unknown>)[name] === 'function',
      ),
    );
  }
  return names;
}
