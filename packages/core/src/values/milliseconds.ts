import { MAX_TIMER_DELAY_MS, toMilliseconds, type Duration } from '../duration.js';
import { ConfigurationError } from '../errors.js';

/**
 * A configured length of time, converted once from a {@link Duration} option. Each factory names
 * the option, so its refusal says which one was wrong.
 */
export abstract class Milliseconds {
  /** Zero or more milliseconds. */
  static of(value: Duration, name: string): number {
    return toMilliseconds(value, name);
  }

  /** More than zero milliseconds: a wait, a timeout or an age that must pass at all. */
  static positive(value: Duration, name: string): number {
    const milliseconds = toMilliseconds(value, name);
    if (milliseconds === 0) {
      throw new ConfigurationError(`${name} must be greater than zero`);
    }
    return milliseconds;
  }

  /** A duration that drives a timer: more than zero, and no longer than timers can represent. */
  static forTimer(value: Duration, name: string): number {
    const milliseconds = Milliseconds.positive(value, name);
    if (milliseconds > MAX_TIMER_DELAY_MS) {
      throw new ConfigurationError(
        `${name} must not exceed ${MAX_TIMER_DELAY_MS}ms (about 24.8 days), received ${milliseconds}ms`,
      );
    }
    return milliseconds;
  }
}
