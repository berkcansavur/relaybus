import { systemClock } from '@relaybus/core';
import { schedule } from '@relaybus/core/internal';

/** Resolves after `milliseconds`, or as soon as `signal` aborts (immediately when it already has). Never rejects. */
export function abortableSleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    const finish = (): void => {
      cancel();
      signal.removeEventListener('abort', finish);
      resolve();
    };
    const cancel = schedule(systemClock, { run: finish, delayMs: milliseconds });
    signal.addEventListener('abort', finish, { once: true });
  });
}
