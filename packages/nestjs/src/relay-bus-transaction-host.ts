import { Inject, Injectable } from '@nestjs/common';
import { RelayBus, TransactionRequiredError } from '@relaybus/core';

/**
 * Hands repositories the transaction of the enclosing `@RelayBusTransaction()` method (or
 * `bus.transaction`, transactional handler, `transactionResolver`), typed as the store's
 * transaction handle: `PoolClient` for `pg`, a Kysely `Transaction`, a TypeORM `EntityManager`,
 * a Prisma transaction client, a MongoDB `ClientSession`.
 *
 * ```ts
 * @Injectable()
 * export class OrderRepository {
 *   constructor(private readonly transactions: RelayBusTransactionHost<PoolClient>) {}
 *
 *   async insert(orderId: string): Promise<void> {
 *     await this.transactions.current().query('INSERT INTO orders (id) VALUES ($1)', [orderId]);
 *   }
 * }
 * ```
 */
@Injectable()
export class RelayBusTransactionHost<TTransaction = unknown> {
  constructor(@Inject(RelayBus) private readonly bus: RelayBus<TTransaction>) {}

  /**
   * The active transaction.
   *
   * @throws TransactionRequiredError when the caller runs outside every transaction.
   */
  current(): TTransaction {
    const transaction = this.bus.currentTransaction();
    if (transaction === undefined) {
      throw new TransactionRequiredError(
        'No relaybus transaction is active: call the repository from a @RelayBusTransaction() method or inside bus.transaction()',
      );
    }
    return transaction;
  }

  /** The active transaction, or undefined outside every transaction. */
  find(): TTransaction | undefined {
    return this.bus.currentTransaction();
  }
}
