import type { ClaimRequest, ClaimedDelivery } from '../event-store.js';

/**
 * The claim skeleton of the database stores: a request for no handlers or no deliveries claims
 * nothing, and the claims of one owner run one after the other. Claims of one owner read the same
 * held lanes and the same bookkeeping of what was handed out, so they must not overlap (the
 * dispatcher claims one batch at a time anyway; `runOnce` next to a running dispatcher is what
 * this guards against). A store supplies only {@link OwnerSerializedClaims.claimInTurn}.
 */
export abstract class OwnerSerializedClaims {
  /** The last claim of each owner still running; the next one waits for it. */
  readonly #queues = new Map<string, Promise<void>>();

  claim(request: ClaimRequest): Promise<ClaimedDelivery[]> {
    if (this.claimsNothing(request)) {
      return Promise.resolve([]);
    }
    return this.#serialized(request.leaseOwner, () => this.claimInTurn(request));
  }

  /** Whether the request cannot claim anything: no handlers, or a limit that is not above 0. */
  protected claimsNothing(request: ClaimRequest): boolean {
    return request.handlers.length === 0 || !(request.limit > 0);
  }

  /** Claims for `request` once the earlier claims of its owner settled. */
  protected abstract claimInTurn(request: ClaimRequest): Promise<ClaimedDelivery[]>;

  #serialized<T>(owner: string, work: () => Promise<T>): Promise<T> {
    const previous = this.#queues.get(owner) ?? Promise.resolve();
    const run = previous.then(work, work);
    const tail = run.then(
      () => undefined,
      () => undefined,
    );
    this.#queues.set(owner, tail);
    void tail.then(() => {
      if (this.#queues.get(owner) === tail) {
        this.#queues.delete(owner);
      }
    });
    return run;
  }
}
