/** Runs async tasks one after another, in submission order, even when one fails. */
export class SerialQueue {
  #tail: Promise<unknown> = Promise.resolve();

  run<TResult>(task: () => Promise<TResult>): Promise<TResult> {
    const result = this.#tail.then(task);
    this.#tail = result.catch(() => undefined);
    return result;
  }
}
