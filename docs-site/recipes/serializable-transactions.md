---
title: Serializable transactions - isolation levels and automatic rerun of serialization failures
description: Run a relaybus transaction at repeatable read or serializable, let PostgreSQL detect write skew instead of locking rows, and have relaybus rerun the transaction after a serialization failure (SQLSTATE 40001).
---

# Serializable transactions

`bus.transaction` and `@RelayBusTransaction()` run at the database default (READ COMMITTED on PostgreSQL) unless you ask for another level. This page shows when to ask for `serializable`, what relaybus does when PostgreSQL aborts a transaction because of a concurrent one, and what your code must not do inside such a transaction.

## The problem: write skew

Two requests each count a user's active reservations, check a limit of one, and insert:

```ts
@RelayBusTransaction()
async reserve(userId: string): Promise<void> {
  const active = await this.reservations.countActive(userId); // SELECT count(*) ...
  if (active >= 1) throw new LimitReachedError(userId);
  await this.reservations.insert(userId);
  await this.bus.publish(new ReservationCreated({ userId }));
}
```

At READ COMMITTED both requests count zero, both pass the check and both insert: the limit is exceeded, although every statement was correct. The usual fixes take a pessimistic lock (`pg_advisory_xact_lock`, `SELECT ... FOR UPDATE`) that makes every request for that user wait for the one before it.

## The fix: `isolationLevel: 'serializable'`

```ts
@RelayBusTransaction({ isolationLevel: 'serializable' })
async reserve(userId: string): Promise<void> {
  /* the same body */
}
```

Under SERIALIZABLE, PostgreSQL lets both requests run without blocking each other and aborts one of them with SQLSTATE `40001` (`could not serialize access`) when the outcome could not have been produced by running them one after the other. relaybus then **runs the aborted transaction again**, in a new one. This time the count sees the other request's committed row, and your own check rejects it with `LimitReachedError`, which is not a serialization failure and is thrown to the caller as it is.

The same with `bus.transaction`:

```ts
await bus.transaction(
  async (tx) => {
    /* ... */
  },
  { isolationLevel: 'serializable', maxAttempts: 5 },
);
```

## What runs again, and what that means for your code

At `'repeatable read'` and `'serializable'`, a serialization failure (SQLSTATE `40001`) during the transaction or at its commit rolls the attempt back and runs `work` again in a new transaction, up to `maxAttempts` attempts in all (default 3). When the last attempt fails too, its error is thrown unchanged. Other errors, the `TransactionTimeoutError` of a budget overrun and every error at `'read committed'` are never rerun.

**`work` may run more than once.** Keep everything that is not a write through the transaction out of it: HTTP calls, broker publishes, emails, in-memory counters, caches. Do them before the transaction starts or after it commits (the `no-external-call-in-transaction` lint rule already flags the usual ones). Events published by a failed attempt are rolled back with it, so the outbox never holds a duplicate or an orphan of an attempt. What a `publish` announces (the `onPublished` observer hook and the wake-up of the dispatcher) is held back until the attempt that committed is over, so a rerun neither reports nor wakes for events that never existed; without an isolation level both happen at publish time, as before. Do not catch a database error inside `work` and carry on: the transaction is already aborted, and relaybus can only rerun it when the error reaches it.

Each attempt is a transaction of its own and is held to its own `transactionTimeout`.

| Level                            | Rerun on serialization failure | What it prevents                                                                             |
| -------------------------------- | ------------------------------ | -------------------------------------------------------------------------------------------- |
| none (default), `read committed` | no                             | Nothing beyond the database default. Without a level the call behaves exactly as before.     |
| `repeatable read`                | yes                            | Non-repeatable reads and lost updates (a concurrent update of a row you updated aborts you). |
| `serializable`                   | yes                            | Everything above and write skew: transactions behave as if they ran one after the other.     |

## Nested calls

A `@RelayBusTransaction()` method called inside a transaction joins it, as it always did. A method that asks for an `isolationLevel` can join only a transaction that relaybus opened at that same level: the level of a running transaction cannot be changed. In any other case (an outer transaction without a level, at another level, or one that your `transactionResolver` found) the call throws a `TransactionIsolationConflictError` (`RELAYBUS_TRANSACTION_ISOLATION_CONFLICT`). A method without `isolationLevel` joins any transaction, as before.

A nested `bus.transaction(...)` call always opens a transaction of its own, with or without a level; only the decorator joins.

## Stores

| Store                                                                            | Levels                                              | How the level is applied                                                                                                                                                                                                                                                                 |
| -------------------------------------------------------------------------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL with `pg` (`PgExecutor`, the default for `pool` / `connectionString`) | `read committed`, `repeatable read`, `serializable` | `BEGIN ISOLATION LEVEL ...` in the same round trip that sets the transaction budget: an isolated transaction costs no statement more than a plain one.                                                                                                                                   |
| PostgreSQL through TypeORM                                                       | the same                                            | TypeORM's own `dataSource.transaction('SERIALIZABLE', ...)`, so the level is set before TypeORM subscribers run, then the budget statement (one round trip more than a plain transaction).                                                                                               |
| PostgreSQL through Kysely, Knex, Drizzle, Prisma or your own `PostgresExecutor`  | the same                                            | `SET TRANSACTION ISOLATION LEVEL ...` as the first statement of the transaction, then the budget statement (one round trip more than a plain transaction, only for transactions that ask for a level). PostgreSQL refuses the level once a query has run, so nothing may come before it. |
| MongoDB, `InMemoryEventStore`                                                    | none                                                | They run transactions at their own level. `isolationLevel` is rejected with a `ConfigurationError` before a transaction opens, because a level the database cannot give would be a silent lie.                                                                                           |

A serialization failure is recognized through every executor: the SQLSTATE `40001` of a `pg` error (also what Kysely and Knex rethrow), TypeORM's `QueryFailedError.driverError`, the `cause` of Drizzle's query error, and Prisma's `P2034` (write conflict or deadlock), raw-query errors that carry the SQLSTATE in `meta.code`, and driver-adapter errors. A custom `PostgresExecutor` whose library wraps the error in another shape implements the optional `isSerializationFailure(error)`.

## Choosing between a lock and SERIALIZABLE

- SERIALIZABLE adds no blocking of its own: its conflict detection takes no read locks, so requests that read the same data do not queue behind each other. Ordinary write locks still apply (two requests updating one row wait, and can deadlock). Under contention it costs reruns; keep `work` short and give hot paths a higher `maxAttempts`.
- A lock queues every request for that key, even when the outcomes would not conflict, and deadlocks when two locks are taken in different order.
- SERIALIZABLE protects every read-then-write on the data the transaction touched without naming a lock key. In exchange PostgreSQL may abort a transaction that a perfect checker would let through (a false positive), more often when a transaction scans many rows, because it tracks reads with predicate locks that it promotes from rows to pages to tables. The rerun hides those aborts; keep the reads of such a transaction indexed and short, and raise `max_pred_locks_per_transaction` if aborts stay frequent.
