# @relaybus/mongodb

MongoDB event store for [relaybus](https://github.com/berkcansavur/relaybus): a transactional
outbox on multi-document transactions, with lane leases, batched claims and completions, and
per-key ordering.

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
inside a started transaction, so you are not forced to use `bus.transaction`. A session without a
started transaction is rejected with a `ConfigurationError`: MongoDB would commit each write
immediately, silently breaking the atomicity the outbox exists for. Mongoose exposes
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

| Option               | Default                                         | Meaning                                                                                                           |
| -------------------- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `client`             | required                                        | A connected `MongoClient`. The store never closes it.                                                             |
| `database`           | the client's default database                   | Database for the collections. 1-63 characters of letters, digits, `_`, `-`.                                       |
| `collectionPrefix`   | `'relaybus_'`                                   | Collections are `<prefix>events`, `<prefix>deliveries`, `<prefix>key_sequences`, `<prefix>lanes`.                 |
| `completionHistory`  | `false`                                         | Keep an append-only record of completed deliveries (`<prefix>completed_deliveries`). Same value in every process. |
| `transactionOptions` | snapshot read concern, `w: 'majority'`, primary | Passed to `withTransaction`.                                                                                      |
| `purgeBatchSize`     | `500`                                           | Documents deleted per purge step.                                                                                 |
| `logger`             | `console`                                       | Receives warnings about best-effort steps that failed (parking).                                                  |
| `generateIdentifier` | -                                               | Deprecated and ignored: a lease token is `<laneId>:<epoch>`.                                                      |

Non-transactional writes (claims, renewals) use write concern `majority` and primary reads, whatever the client
default is, so a lease survives a failover and the ordering check never reads a stale secondary.

## How it works

**Collections.** `events` (`_id` = event id), `deliveries` (one per unfinished event and handler),
`key_sequences` (one counter document per ordering key), `lanes` (256 lane documents), and
`completed_deliveries` with `completionHistory`.

**Idempotent append.** Event ids are deduplicated by reading what exists inside the transaction
and writing with upserts. A plain insert would raise a duplicate-key error (11000) when two
transactions race on the same id, and a duplicate-key error aborts the whole transaction without
a retry label. An upsert race surfaces as a retryable write conflict instead. Every event lists the
handlers it ever had a delivery for (`handlers`), completed ones included: a redelivered broker
message or a publish retry adds deliveries only for handlers the event does not list yet, so a
handler that already completed never runs twice although its delivery was deleted. The unique index
on `(eventId, handler)` is a backstop; the code never relies on catching it.

**Per-key sequence equals commit order.** Appending a keyed event runs
`findOneAndUpdate({ _id: key }, { $inc: { value: 1 } }, { upsert: true })` on the counter inside
the append transaction. A transaction holding an uncommitted increment makes every concurrent
appender of that key hit a write conflict; the loser restarts after the winner committed and reads
the next value. Two transactions can never both hold sequence `n`, and a lower sequence is always
committed before a higher one, so ordering by sequence is ordering by commit.

**Lane leases.** Every delivery belongs to one of 256 lanes, `laneId`: the 32-bit FNV-1a hash of
the UTF-8 ordering key, or of the event id when there is none, modulo 256. All deliveries of a key
share a lane. Workers lease lanes, not deliveries. A lane document holds the owner, the lease
expiry, an `epoch` and `inFlight`, the ids of the deliveries the owner claimed. A delivery runs while
its document exists as `pending` and its lane lists it; `processing` is derived, never stored.
A claim:

1. claims from the lanes the worker already holds under a live lease;
2. if it still needs more, takes free lanes (no owner, or an expired lease) that have due deliveries
   of its handlers: 4, then 8, 16, ... lanes per round, the lane with the oldest due delivery first,
   so a worker holds only as many lanes as its batch needs. Taking a lane is one conditional
   `findOneAndUpdate` that increments the epoch; a lane another claimer took first is skipped, never
   waited for;
3. reads the due, ready deliveries of those lanes in `(dueAt, _id)` order, leaving out the ones it
   already runs, applies the per-key rules below, and stops at `limit`;
4. records the batch with one conditional write per lane, all in one `bulkWrite` (`inFlight` gains
   the new ids and loses the ids whose documents are gone; the lease never shrinks), and hands back
   the lanes that yielded nothing and run nothing.

A claim of an unkeyed batch therefore costs a small, fixed number of round trips and writes **no
delivery document**. A completion deletes the delivery documents and writes no lane document.
Delivery documents are inserted once and deleted once, and no indexed field of theirs changes on
the normal path.

Lanes are held per store instance as well as per `leaseOwner`: a process restarted with a fixed
owner name does not adopt the lanes of its dead predecessor, it takes them over once their leases
expired, like any other worker. A claim reads the events of its batch before recording it, and a
pick that a dead letter requeued during the claim now precedes is given back right after recording,
without an attempt charged; parking is best effort and never fails a claim (failures go to the
`logger` option, default `console`).

**Lane exclusivity.** While a worker holds a lane under a live lease, other workers do not see the
pending deliveries of that lane, even idle ones. A worker that dies delays its lanes by at most the
lease duration; a worker that stops gracefully hands them back at once (`relinquish`). Lanes are
chosen per claim, so a backlog spreads over workers; a few hot keys live in few lanes and therefore
run on few workers at a time.

**Per-key order.** A keyed delivery is claimed only when no earlier delivery (lower sequence) of its
handler and key is pending (a running one is still a pending document) and no other delivery of them
runs. Because all deliveries of a key share one lane and only the lane owner claims from it, the
running ones are exactly the ones listed by the owner's lanes, which the claim loads once. No
delivery is locked or written to check this. One claim call never takes two deliveries of the same
key, and what it learns about a key answers every later delivery of that key without another
query. The scan continues past blocked deliveries until `limit` is reached, so a hot blocked key
cannot starve unrelated work.

**Parked backlogs.** A key whose head is slow can accumulate thousands of deliveries that are all
due and all blocked. Scanning them on every claim would cost time proportional to the backlog.
When one claim finds 32 or more deliveries of a key waiting behind an unfinished predecessor,
it moves them to the `parked` claim lane (the `lane` field), which the claim index leaves out. A
claim that hands a lane back also parks the waiting deliveries it found there, however few: they
are all the due work of that lane (typically followers of a head whose retry is not due yet), and
leaving them ready would make every later claim take the lane for nothing. At the start of every
claim, the lowest parked delivery of each key with no unfinished predecessor is moved back to
`ready` (one aggregation per handler, a distinct scan of the small parked index, then one
predecessor lookup per parked key, in parallel, and one update). This is deliberately level-triggered: it looks at the current state, never at an event,
so a crash while parking, or a predecessor finishing while its followers were being parked, can
delay a key until the next claim but never strand it. Parked deliveries keep their `dueAt`, so
`statistics().oldestDueAt` still counts them. The per-claim cost of the move-back step is one
index probe and one parallel lookup per key that currently has parked deliveries, so keep the number of such
simultaneously backlogged keys in the hundreds, not millions. Parking and unparking write delivery
documents; they are the slow path for blocked keys only.

`requeue` is an operator action and makes a dead delivery pending again even if later deliveries
of its key already ran; it blocks the ones that have not been claimed yet, and it cannot start
while a later delivery of its key is running.

**Fencing.** A lease token is `<laneId>:<epoch>`. `complete`, `completeMany`, `reschedule`,
`release`, `bury` and `extendLease` apply only while the lane still has that epoch and lists the
delivery; a worker that lost its lane changes nothing. A token this store cannot have issued is
refused without a database call. An expired lease nobody took over is still honoured.

**Completion and takeover.** A worker that takes a lane over first writes every delivery the
previous owner still had in flight (it charges the lost attempt, see below) and only then reads
what to claim. A completion never writes a lane document, so concurrent completions of one lane
never conflict with each other; it relies on that charge instead:

- `complete(lease, now, session)` (transactional handlers) and every completion when
  `completionHistory` is on run in a transaction that reads the lanes and deletes the deliveries
  (and inserts the history records). The completion and a takeover write the same delivery
  document, so whichever commits second either hits a write conflict and is retried against the
  new state (seeing the new epoch, or the delivery gone), or waits for the first to finish.
- Otherwise `complete` and `completeMany` need no transaction: they read the deliveries' attempt
  counts, then the lanes, and delete only the deliveries whose attempt count is still the one they
  read. A takeover before the lane read shows up in it as a new epoch; a takeover after it changed
  the attempt count, so the delete matches nothing. A batch of any size from any number of lanes
  costs three commands.

A completion and a takeover can never both succeed, which is what makes a transactional handler's
writes effectively-once. Two concurrent completions of the same lease without a transaction may both
report success; the dispatcher never issues them.

**Attempts.** `attempts` in a delivery document counts attempts that **ended**: `reschedule` and
`bury` add the failed one, `release` (graceful shutdown) adds nothing, and the claim result reports
`attempts + 1`. When a lane is taken over, every delivery the previous owner still had in flight is
charged one attempt, so a worker that keeps crashing on a poison message still runs it into the
retry limit and the dead letters. The take moves the previous owner's in-flight list to the lane's
`lost` list in the same write; `lost` is charged before anything is read from the lane and cleared
afterwards, and whoever holds the lane next charges a `lost` list it finds first. A claim that fails
anywhere after the take therefore loses no charge and leaves nothing listed as running. Charging
and clearing are separate writes, not a transaction: a failure exactly between them charges those
deliveries once more.

**Completion history.** Completed deliveries are deleted. With `completionHistory: true` every
completion also inserts a record into `completed_deliveries` in the same transaction. Records are
listed by `listDeliveries` as `completed` (all filters apply), counted by `statistics().completed`
and removed by `purge({ completedBefore })`. Without it, `listDeliveries({ status: 'completed' })` is
empty and `statistics().completed` is 0 (except for `completed` documents left by 0.1.0).

**Time.** The store uses only the `now` it is given, never the server clock. Lease expiry is
compared with the `now` of the claimer, so keep worker clocks synchronized.

**Payloads.** Payload and headers are stored as the JSON-normalized value: `Date` becomes an ISO
string, `undefined` members disappear, as in every other relaybus store. Keys containing `.` or
`$` are stored as they are (MongoDB 5+). Text with a **lone UTF-16 surrogate** (a broken emoji,
for example) is rejected with an `EventValidationError`: BSON strings are UTF-8, so MongoDB would
silently store U+FFFD instead. Run such text through `String.prototype.toWellFormed()` first.

**Untrusted input.** The fields of `listDeliveries` and the ids of `requeue` are coerced to
strings before they reach a query, so an object parsed from an admin request cannot become a
query operator such as `{ $ne: ... }`.

## Transactions and retries

`store.transaction(work)` uses `session.withTransaction`, which restarts the **whole callback**
after a `TransientTransactionError` (typically a write conflict) and re-sends only the commit after
an `UnknownTransactionCommitResult`. For everything written through the session this is exactly
what you want: an aborted attempt leaves nothing behind, and `append` re-reads state at the start
of every attempt, so a retry never appends twice. The value returned is that of the final attempt.

The consequence is that `work` can run more than once, and the same holds for `bus.transaction`,
which is built on it. Keep side effects that do not go through the session (HTTP calls, other
databases, message brokers) out of it, or make them idempotent. This fits relaybus: handlers are
at-least-once anyway, and event publishing is the mechanism meant to replace such side effects.

Retries stop after 120 seconds (the driver's limit) with the last error.

Transactional handlers run inside such a transaction too, so the same rule applies to them. The
core does not renew the lease of a transactional handler while it runs (its completion must not
conflict with a renewal), so the lease must outlast the handler: keep transactional handlers
short, as the core documentation recommends.

## Indexes

Created idempotently by `setup()` (also run by `bus.start()` unless `setupStore: false`):

| Collection             | Index                                                 | Used by                                        |
| ---------------------- | ----------------------------------------------------- | ---------------------------------------------- |
| `deliveries`           | `(eventId, handler)` unique                           | idempotent append, lookup by event             |
| `deliveries`           | `(laneId, dueAt, _id, handler)` partial               | claim, free-lane discovery (`ready` lane only) |
| `deliveries`           | `(handler, key, sequence)` partial                    | predecessor check of keyed deliveries          |
| `deliveries`           | `(handler, key, sequence)` partial                    | lowest parked delivery of each key (`parked`)  |
| `deliveries`           | `(dueAt)` partial                                     | `statistics().oldestDueAt`                     |
| `deliveries`           | `(completedAt)` partial                               | purge of `completed` documents left by 0.1.0   |
| `deliveries`           | `(status, createdAt)`                                 | status counts, listing by status               |
| `deliveries`           | `(status, updatedAt)`                                 | purge of dead deliveries                       |
| `deliveries`           | `(handler, createdAt)`                                | listing by handler                             |
| `deliveries`           | `(eventType, createdAt)`                              | listing by event type                          |
| `events`               | `(createdAt, _id)`                                    | purge of events nothing references             |
| `completed_deliveries` | `(completedAt, _id)`, `(eventId)`, `(createdAt, _id)` | purge, event references, listing               |

The claim index ends with `handler` so that a query over many lanes stays a merge of one index scan
per lane (the planner merges at most 200), and a claim queries at most 128 lanes at once. The lane
collection has 256 small documents and no secondary index.

`setup()` also creates the collections explicitly, because collections created implicitly inside
concurrent transactions conflict with each other, and seeds the lane documents. If you manage the
schema yourself, run `setup()` once from a deployment script instead.

Partial indexes contain only unfinished (or legacy completed) deliveries, so they stay small. If you
never use `listDeliveries` filters, the three listing indexes can be dropped at the cost of slower
operator queries.

## Upgrading from 0.1.0

`setup()` upgrades a 0.1.0 database in place, idempotently, and is safe to run again: `processing`
deliveries become `pending` and due at once (their attempt stays counted, as a lost lease would count
it), the per-delivery lease fields are removed, every delivery gets its `laneId` and every event its
`handlers` and `createdAt` (from its deliveries, in batches of 1,000), the `running` index and the
0.1.0 `claim` index are dropped, and the lane documents are created. The lane documents mark the
upgrade as done, so later runs skip it. `completed` documents left by 0.1.0 stay until a purge with
`completedBefore` removes them. Stop all 0.1.0 workers before the first `setup()`: they cannot work
with the upgraded documents.

## Tuning

- **Batch size.** A claim takes 4, 8, 16, ... lanes until it has `limit` deliveries, so a worker's
  lane count follows its batch size. Unkeyed deliveries spread over all 256 lanes; very small batches
  with many workers keep lanes changing hands often, very large ones let one worker hold most lanes.
- **Lease duration.** A lane whose owner died is unavailable for at most the lease duration. Keep it
  well above the handler run time (the dispatcher renews it while handlers run).
- **Completion batching.** The dispatcher completes deliveries that finish close together with one
  `completeMany`: three commands per batch (one transaction with `completionHistory`).
- **Transactional handlers** commit their writes with the deletion of the delivery. They conflict
  only with a takeover of their own delivery, not with each other.

## Retention

`bus.purgeExpired()` (run on a schedule by the bus) calls `store.purge`, which deletes, in batches of
`purgeBatchSize`, dead deliveries (`deadBefore`), completion history records and legacy `completed`
documents (`completedBefore`), each batch together with the events nothing references any more **in
one transaction**, so a crash can never leave a delivery without its event. Then it deletes events
created before the earliest given cutoff that nothing references: the events of completed (deleted)
deliveries, and events no handler was subscribed to. An append that adds a delivery to an event while
it is being purged writes the event, conflicts with the purge, and one of them restarts, so a
delivery never loses its event.

An event deleted by purge takes its `handlers` list with it: a broker redelivery or publish retry
of that event id after the purge is appended as a new event and runs its handlers again (the same
holds for the PostgreSQL store). Keep retention longer than any redelivery window.

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
- **Deliveries wait although workers are idle**: another worker holds their lane under a live lease
  (look at `<prefix>lanes`: `owner`, `leaseExpiresAt`); it is released when that worker finishes,
  stops, or the lease expires.
- **A key is stuck**: a pending, delayed, running or requeued delivery earlier in the same key
  blocks later ones, and so does any running delivery of the key; inspect it with
  `listDeliveries({ handler })` and `bus.requeue` or bury it.
- **Slow claims with a very large backlog of one key**: backlogs of 32 or more waiting deliveries
  are parked automatically; check that the `claim`, `ordering` and `parked` indexes exist
  (`setup()`). Deliveries with `lane: 'parked'` are expected while their key is blocked.
- **`MongoEventStore.append needs a ClientSession inside a started transaction`**: the session
  passed to `publish({ transaction })` or `append` has no transaction. Start one with
  `session.withTransaction` or `session.startTransaction()`.
