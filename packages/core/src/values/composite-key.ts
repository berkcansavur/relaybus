/** Separates the parts of a composite key: no identifier, handler id or ordering key contains it. */
const SEPARATOR = '\u0000';

/**
 * Two identifiers joined into one map or set key, such as an event id and a handler id, or a
 * handler id and an ordering key. The NUL separator keeps `('a:b', 'c')` and `('a', 'b:c')` apart.
 * Keys are plain strings, so they cost nothing more than the template literal they replace.
 */
export abstract class CompositeKey {
  static of(first: string, second: string): string {
    return `${first}${SEPARATOR}${second}`;
  }

  /** The start every key of `first` shares, to find them all by prefix. */
  static startOf(first: string): string {
    return `${first}${SEPARATOR}`;
  }
}
