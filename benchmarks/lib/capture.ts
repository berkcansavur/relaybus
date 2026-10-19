// Learns the exact SQL text of every hot statement by running the real relaybus code once against a scratch database.
import type { Backend } from './backends.ts';
import { core, pg, postgresPkg } from './deps.ts';

export type Captured = { sql: string; params: any[] };
export const captured = new Map<string, Captured>();

const KINDS: Array<[string, RegExp]> = [
  ['claim', /WITH due_window/],
  ['claimSlow', /WITH heads AS/],
  ['complete', /SET status = 'completed'/],
  ['extendLease', /SET lease_expires_at = \$3/],
  ['reserveSequences', /^INSERT INTO [\w."]*relaybus_key_sequences/],
  ['insertEvents', /^INSERT INTO [\w."]*relaybus_events/],
  ['insertDeliveries', /^INSERT INTO [\w."]*relaybus_deliveries/],
];

let installed = false;
function installCapture(): void {
  if (installed) return;
  installed = true;
  const originalQuery = pg.Client.prototype.query;
  pg.Client.prototype.query = function (this: any, ...queryArgs: any[]) {
    const [text, values] = queryArgs;
    if (typeof text === 'string' && Array.isArray(values)) {
      const kind = KINDS.find(([, pattern]) => pattern.test(text.trim()))?.[0];
      if (kind !== undefined && !captured.has(kind)) captured.set(kind, { sql: text, params: values });
    }
    return originalQuery.apply(this, queryArgs);
  };
}

class Order extends core.defineEvent<{ id: number }>('bench.order.placed.v1') {}

export async function captureStatements(be: Backend, log: (message: string) => void): Promise<void> {
  installCapture();
  await be.createFresh('capture');
  const pool = new pg.Pool({ connectionString: be.connection('capture').url, max: 2 });
  pool.on('error', () => undefined);
  const store = new postgresPkg.PostgresEventStore({ pool });
  const bus = new core.RelayBus({ store, dispatcher: { enabled: false }, retention: { completed: false }, logger: { log() {}, warn() {}, error() {} } });
  bus.subscribe(Order, () => undefined, { id: 'h1' });
  // 300 events of one key: the front of the due queue is all blocked followers, which also exercises the slow claim.
  await bus.transaction(async (tx: unknown) => {
    await bus.publishAll(Array.from({ length: 300 }, (_, id) => new Order({ id })), { transaction: tx, key: 'k' });
  });
  await bus.transaction(async (tx: unknown) => {
    await bus.publish(new Order({ id: 1000 }), { transaction: tx, key: 'k2' });
  });
  const now = new Date();
  const claimed = await store.claim({ handlers: ['h1'], limit: 10, leaseOwner: 'cap', now, leaseExpiresAt: new Date(now.getTime() + 30_000) });
  await store.extendLease(claimed[0], new Date(now.getTime() + 60_000), now);
  await store.complete(claimed[0], now);
  await pool.end();
  await be.drop('capture');
  log(`captured statements: ${[...captured.keys()].join(', ')}`);
}
