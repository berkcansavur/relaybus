# @relaybus/mongodb

MongoDB event store for [relaybus](https://github.com/berkcansavur/relaybus): a transactional
outbox on multi-document transactions, with atomic leasing and per-key ordering.

Events are written in the **same MongoDB transaction** as your business data, so a committed
change always has its event and a rolled-back change never leaks one. Delivery is at-least-once;
handlers registered with `transactional: true` also get effectively-once processing, because
their writes and the delivery's completion commit together.

## Requirements

- MongoDB 6.0 or newer, **as a replica set or a sharded cluster**. MongoDB only supports
  transactions there. A single-node replica set is enough for development.
- Node.js 22.12 or newer, `mongodb` driver 6 or 7 (peer dependency), `@relaybus/core`.

A standalone `mongod` is rejected with a `ConfigurationError` that says so (from `setup()` and
from the first transaction). Start a development replica set with
`mongod --replSet rs0` followed by `rs.initiate()`, or use the `mongo` Docker image the same way.

## Install

```sh
npm install @relaybus/core @relaybus/mongodb mongodb
```

## Usage

```ts
import { MongoClient } from 'mongodb';
import { RelayBus, defineEvent } from '@relaybus/core';
import { MongoEventStore } from '@relaybus/mongodb';
import type { ClientSession } from 'mongodb';

class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

const client = await new MongoClient(process.env.MONGODB_URI!).connect();
const bus = new RelayBus<ClientSession>({
  store: new MongoEventStore({ client, database: 'shop' }),
});

bus.subscribe(OrderPlaced, async (event) => sendConfirmation(event.payload.orderId), {
  id: 'send-confirmation',
});
await bus.start(); // creates collections and indexes, then starts dispatching

await bus.transaction(async (session) => {
  await client.db('shop').collection('orders').insertOne({ _id: 'o-1' }, { session });
  await bus.publish(new OrderPlaced({ orderId: 'o-1' }), { key: 'o-1' });
});
```

The transaction handle is the driver's `ClientSession`. Pass it to every write that must be
atomic with the event.

### Using an existing session or Mongoose

`publish({ transaction })` and `store.append(session, ...)` accept any `ClientSession` that is
inside a started transaction, so you are not forced to use `bus.transaction`. Mongoose exposes
the same driver session (`mongoose.startSession()`, `Model.create([doc], { session })`), and
`mongoose.connection.getClient()` returns the `MongoClient` to give to the store:

```ts
const session = await mongoose.startSession();
await session.withTransaction(async () => {
  await Order.create([{ total: 10 }], { session });
  await bus.publish(new OrderPlaced({ orderId: '1' }), { transaction: session });
});
```

relaybus does not depend on Mongoose. `session.withTransaction` may run your callback again
after a transient error (see [Transactions and retries](#transactions-and-retries)).

### Transactional handlers

```ts
bus.subscribe(
  OrderPlaced,
  async (event, context) => {
    await projections.insertOne(
      { orderId: event.payload.orderId },
      { session: context.transaction },
    );
  },
  { id: 'projector', transactional: true },
);
```

The projection write and the delivery's completion commit together or not at all.

## Options

| Option               | Default                                         | Meaning                                                                          |
| -------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------- |
| `client`             | required                                        | A connected `MongoClient`. The store never closes it.                            |
| `database`           | the client's default database                   | Database for the collections. 1-63 characters of letters, digits, `_`, `-`.      |
| `collectionPrefix`   | `'relaybus_'`                                   | Collections are `<prefix>events`, `<prefix>deliveries`, `<prefix>key_sequences`. |
| `transactionOptions` | snapshot read concern, `w: 'majority'`, primary | Passed to `withTransaction`.                                                     |
| `purgeBatchSize`     | `500`                                           | Deliveries deleted per purge step.                                               |
| `generateIdentifier` | UUID v7                                         | Produces lease tokens.                                                           |

Non-transactional writes (claims, completions, retries) use write concern `majority` and primary
reads, whatever the client default is, so a lease survives a failover and the ordering check
never reads a stale secondary.

## How it works

**Collections.** `events` (`_id` = event id), `deliveries` (one per event and handler),
`key_sequences` (one counter document per ordering key).

**Idempotent append.** Event ids are deduplicated by reading what exists inside the transaction
and writing with upserts. A plain insert would raise a duplicate-key error (11000) when two
transactions race on the same id, and a duplicate-key error aborts the whole transaction without
a retry label. An upsert race surfaces as a retryable write conflict instead. The unique index on
`(eventId, handler)` is a backstop; the code never relies on catching it.

**Per-key sequence equals commit order.** Appending a keyed event runs
`findOneAndUpdate({ _id: key }, { $inc: { value: 1 } }, { upsert: true })` on the counter inside
the append transaction. A transaction holding an uncommitted increment makes every concurrent
appender of that key hit a write conflict; the loser restarts after the winner committed and reads
the next value. Two transactions can never both hold sequence `n`, and a lower sequence is always
committed before a higher one, so ordering by sequence is ordering by commit.

**Claim without `SKIP LOCKED`.** MongoDB has no row locks to skip, so claiming is optimistic:

1. Scan deliveries that are due (`dueAt <= now`) for the requested handlers, ordered by
   `(dueAt, _id)`. `dueAt` is `availableAt` while pending and `leaseExpiresAt` while processing; it
   is removed when a delivery finishes, so the claim index only holds unfinished work.
2. For a keyed delivery, check that no delivery of the same handler and key with a lower
   sequence is still unfinished (`dueAt` present). A predecessor that is pending (even delayed),
   processing (even with an expired lease) or requeued blocks its successors.
3. Take it with one `findOneAndUpdate` whose filter pins the exact state that was read (status,
   attempt count, `dueAt`). If another claimer changed the delivery in between, the filter matches
   nothing and this claimer moves on. Exactly one claimer wins each delivery.

The check in step 2 cannot race with a successor becoming claimable: a delivery's predecessors are
committed before it exists (see the counter above), and a predecessor only leaves
`pending`/`processing` through `complete` or `bury`. One claim call also never takes two
deliveries of the same key, and skips every later delivery of a key once one is found blocked, so a
key with thousands of queued deliveries costs one lookup per claim, not one per delivery.
The scan continues past blocked deliveries until `limit` is reached, so a hot blocked key cannot
starve unrelated work.

`requeue` is an operator action and makes a dead delivery pending again even if later deliveries
of its key already ran; it blocks the ones that have not been claimed yet.

**Fencing.** `complete`, `reschedule`, `release`, `bury` and `extendLease` filter by delivery id,
`status: 'processing'` and the lease token. A worker that lost its lease changes nothing. An expired
lease nobody has reclaimed yet is still honoured. `complete(lease, now, session)` joins your
transaction.

**Time.** The store uses only the `now` it is given, never the server clock.

**Payloads.** Payload and headers are stored as the JSON-normalized value: `Date` becomes an ISO
string, `undefined` members disappear, as in every other relaybus store. Keys containing `.` or
`$` are stored as they are (MongoDB 5+).

## Transactions and retries

`store.transaction(work)` uses `session.withTransaction`, which restarts the **whole callback**
after a `TransientTransactionError` (typically a write conflict) and re-sends only the commit after
an `UnknownTransactionCommitResult`. For everything written through the session this is exactly
what you want: an aborted attempt leaves nothing behind, and `append` re-reads state at the start
of every attempt, so a retry never appends twice. The value returned is that of the final attempt.

The consequence is that `work` can run more than once. Keep side effects that do not go through the
session (HTTP calls, other databases, message brokers) out of it, or make them idempotent. This
fits relaybus: handlers are at-least-once anyway, and event publishing is the mechanism meant to
replace such side effects.

Retries stop after 120 seconds (the driver's limit) with the last error.

A transactional handler that runs longer than the lease heartbeat interval (a third of
`leaseDuration`) can see its completion conflict with the heartbeat's lease extension. The driver
then restarts the handler in a fresh transaction. Keep transactional handlers short, as the core
documentation recommends.

## Indexes

Created idempotently by `setup()` (also run by `bus.start()` unless `setupStore: false`):

| Collection   | Index                              | Used by                                          |
| ------------ | ---------------------------------- | ------------------------------------------------ |
| `deliveries` | `(eventId, handler)` unique        | idempotent append, lookup by event               |
| `deliveries` | `(handler, dueAt, _id)` partial    | claim                                            |
| `deliveries` | `(handler, key, sequence)` partial | predecessor check of keyed deliveries            |
| `deliveries` | `(dueAt)` partial                  | `statistics().oldestDueAt`                       |
| `deliveries` | `(completedAt)` partial            | purge of completed deliveries                    |
| `deliveries` | `(status, createdAt)`              | status counts, listing, purge of dead deliveries |
| `deliveries` | `(handler, createdAt)`             | listing by handler                               |
| `deliveries` | `(eventType, createdAt)`           | listing by event type                            |

`setup()` also creates the three collections explicitly, because collections created implicitly
inside concurrent transactions conflict with each other. If you manage the schema yourself, create
the collections and indexes before the first publish.

Partial indexes contain only unfinished (or completed) deliveries, so they stay small however much
history you keep. If you never use `listDeliveries` filters, the three listing indexes can be
dropped at the cost of slower operator queries.

## Retention

`bus.purgeExpired()` (run on a schedule by the bus) calls `store.purge`, which deletes matching
deliveries in batches of `purgeBatchSize` and then deletes events no delivery references any more,
in a transaction. An append that adds a delivery to an event while it is being purged conflicts
with the purge and one of them restarts, so a delivery never loses its event.

One counter document per distinct ordering key stays in `key_sequences`. They are tiny; if your
keys are unbounded (for example one per request), clean up counters of long-finished keys yourself.

## Troubleshooting

- **`MongoEventStore needs multi-document transactions`**: the server is a standalone `mongod`.
  Use a replica set or sharded cluster.
- **`Transaction numbers are only allowed on a replica set member or mongos`** raised by your own
  code: same cause, for your own transactions.
- **`TransientTransactionError` / write conflicts under load**: expected when many publishers append
  the same key at once; they serialize through the key's counter and retry. Spread hot keys or batch
  publishes with `publishAll`.
- **Deliveries stay pending**: check `statistics().oldestDueAt`, and that the process that publishes
  also registered the handler (deliveries are created from the publisher's registrations).
- **A key is stuck**: a pending, delayed, processing or requeued delivery earlier in the same key
  blocks later ones; inspect it with `listDeliveries({ handler })` and `bus.requeue` or bury it.
- **Slow claims with a very large backlog of one key**: each claim does one predecessor lookup per
  blocked key per call; check that the `ordering` index exists (`setup()`).
