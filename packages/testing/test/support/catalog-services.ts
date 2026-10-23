/**
 * Two services that share one catalog, for the multi-service integration tests
 * of the broker packages. The orders service stores its events in PostgreSQL,
 * the shipping service in MongoDB; a real broker connects them. Each broker
 * test supplies the transports and the destinations.
 */
import {
  RelayBus,
  consume,
  defineCatalog,
  defineEvent,
  produce,
  type CatalogDeclaration,
  type ReceiveStatus,
  type RelayBusLogger,
  type RouteOptions,
  type StandardSchemaV1,
  type Transport,
} from '@relaybus/core';
import { MongoEventStore } from '@relaybus/mongodb';
import { PostgresEventStore } from '@relaybus/postgres';
import { startReplicaSet, uniqueDatabaseName } from '../../../mongodb/test/support.js';
import {
  dropSchema,
  startDatabase,
  uniqueSchema,
} from '../../../postgres/test/support/postgres-container.js';

/** Requires a non-empty string orderId, so a malformed payload from the broker is caught. */
const orderPayloadSchema: StandardSchemaV1<unknown, { orderId: string; total: number }> = {
  '~standard': {
    version: 1,
    vendor: 'relaybus-tests',
    validate: (value) => {
      const candidate = value as { orderId?: unknown; total?: unknown };
      return typeof candidate.orderId === 'string' &&
        candidate.orderId.length > 0 &&
        typeof candidate.total === 'number'
        ? { value: { orderId: candidate.orderId, total: candidate.total } }
        : { issues: [{ message: 'orderId must be a non-empty string and total a number' }] };
    },
  },
};

export class OrderPlaced extends defineEvent('order.placed.v1', { schema: orderPayloadSchema }) {}
export class OrderCancelled extends defineEvent<{ orderId: string }>('order.cancelled.v1') {}
export class OrderShipped extends defineEvent<{ orderId: string; carrier: string }>(
  'order.shipped.v1',
) {}

/** The contracts package both services import. */
export const OrderEvents = defineCatalog(
  { OrderPlaced, OrderCancelled, OrderShipped },
  { name: 'OrderEvents' },
);

/**
 * Where events travel on the broker. Both order events share one destination
 * so the broker keeps them in order per key (one Kafka partition per key, one
 * RabbitMQ queue).
 */
export interface Destinations {
  readonly orders: string;
  readonly shipments: string;
}

export const silentLogger: RelayBusLogger = {
  log: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

/** Databases of both services, started once per test file. */
export interface ServiceDatabases {
  readonly postgres: Awaited<ReturnType<typeof startDatabase>>;
  readonly mongo: Awaited<ReturnType<Awaited<ReturnType<typeof startReplicaSet>>['connect']>>;
  stop(): Promise<void>;
}

export async function startServiceDatabases(): Promise<ServiceDatabases> {
  const postgres = await startDatabase();
  const replicaSet = await startReplicaSet();
  const mongo = await replicaSet.connect();
  return {
    postgres,
    mongo,
    async stop() {
      await mongo.close();
      await Promise.all([postgres.stop(), replicaSet.stop()]);
    },
  };
}

/** What a service did, recorded by its handlers and its bus observer. */
export interface ServiceRecord {
  readonly handled: string[];
  readonly received: ReceiveStatus[];
}

/** The parts of a service bus the tests drive. */
export type ServiceBus = Pick<
  RelayBus,
  'start' | 'stop' | 'handlers' | 'listDeliveries' | 'isRunning'
>;

export interface OrdersService {
  readonly bus: ServiceBus;
  readonly record: ServiceRecord;
  setupOrdersTable(): Promise<void>;
  placeOrder(orderId: string, total: number, options?: PlaceOrderOptions): Promise<void>;
  cancelOrder(orderId: string): Promise<void>;
  storedOrders(): Promise<string[]>;
  drop(): Promise<void>;
}

export interface ShippingService {
  readonly bus: ServiceBus;
  readonly record: ServiceRecord;
  shipOrder(orderId: string, carrier: string): Promise<void>;
  shipmentStatuses(): Promise<Record<string, string>>;
}

export interface PlaceOrderOptions {
  /** Ordering key of the event. Default: the order id. */
  readonly customer?: string;
  /** Throw after publishing, which rolls back the order and the event. */
  readonly fail?: boolean;
}

/** Options shared by both service factories. */
export interface ServiceOptions {
  readonly transport: Transport;
  readonly destinations: Destinations;
  /** Replace the role declarations, to test the boot check. */
  readonly catalogs?: readonly CatalogDeclaration<typeof OrderEvents>[];
  /** Leave out the routes, to test the boot check. */
  readonly withoutRoutes?: boolean;
  /** Leave out the handlers, to test the boot check. */
  readonly withoutHandlers?: boolean;
}

/**
 * The orders service (PostgreSQL): produces OrderPlaced and OrderCancelled,
 * consumes OrderShipped. It writes its orders and publishes in one transaction.
 */
export function createOrdersService(
  databases: ServiceDatabases,
  options: ServiceOptions,
): OrdersService {
  const schema = uniqueSchema();
  const store = new PostgresEventStore({
    pool: databases.postgres.pool,
    schema,
    // The tests read completed deliveries back (to wait on them and replay them).
    completionHistory: true,
  });
  const record: ServiceRecord = { handled: [], received: [] };
  const routes: RouteOptions[] = [
    {
      event: OrderPlaced,
      transport: options.transport.name,
      destination: options.destinations.orders,
    },
    {
      event: OrderCancelled,
      transport: options.transport.name,
      destination: options.destinations.orders,
    },
  ];
  const bus = new RelayBus({
    store,
    source: 'orders',
    transports: [options.transport],
    routes: options.withoutRoutes === true ? [] : routes,
    subscriptions: [
      { transport: options.transport.name, destinations: [options.destinations.shipments] },
    ],
    catalogs: options.catalogs ?? [
      produce(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }),
      consume(OrderEvents, { events: ['OrderShipped'] }),
    ],
    dispatcher: { pollInterval: '20ms' },
    retry: { initialDelay: '50ms', maxDelay: '200ms', jitter: 'none' },
    observers: [{ onReceived: ({ status }) => void record.received.push(status) }],
    logger: silentLogger,
  });
  if (options.withoutHandlers !== true) {
    bus.subscribeCatalog(
      OrderEvents,
      {
        onOrderShipped: (event) => {
          record.handled.push(`shipped:${event.payload.orderId}:${event.payload.carrier}`);
        },
      },
      { events: ['OrderShipped'], idPrefix: 'orders' },
    );
  }
  const orders = bus.forCatalog(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] });

  /** Creates the orders table next to the relaybus tables. */
  async function setupOrdersTable(): Promise<void> {
    await databases.postgres.pool.query(
      `CREATE TABLE IF NOT EXISTS "${schema}".orders (id text PRIMARY KEY, total integer NOT NULL)`,
    );
  }

  /** Inserts an order and publishes OrderPlaced in the same transaction. */
  async function placeOrder(
    orderId: string,
    total: number,
    { customer = orderId, fail = false }: PlaceOrderOptions = {},
  ): Promise<void> {
    await bus.transaction(async (transaction) => {
      await transaction.query(`INSERT INTO "${schema}".orders (id, total) VALUES ($1, $2)`, [
        orderId,
        total,
      ]);
      await orders.publish(new OrderPlaced({ orderId, total }), { transaction, key: customer });
      if (fail) {
        throw new Error('checkout failed after publishing');
      }
    });
  }

  async function cancelOrder(orderId: string): Promise<void> {
    await bus.transaction(async (transaction) => {
      await orders.publish(new OrderCancelled({ orderId }), { transaction, key: orderId });
    });
  }

  async function storedOrders(): Promise<string[]> {
    const result = await databases.postgres.pool.query<{ id: string }>(
      `SELECT id FROM "${schema}".orders ORDER BY id`,
    );
    return result.rows.map((row) => row.id);
  }

  return {
    bus,
    record,
    setupOrdersTable,
    placeOrder,
    cancelOrder,
    storedOrders,
    drop: () => dropSchema(databases.postgres.pool, schema),
  };
}

/**
 * The shipping service (MongoDB): consumes OrderPlaced and OrderCancelled,
 * produces OrderShipped. Its handlers are transactional: the shipment document
 * and the delivery completion commit together, so a redelivered event changes
 * nothing.
 */
export function createShippingService(
  databases: ServiceDatabases,
  options: ServiceOptions,
): ShippingService {
  const database = uniqueDatabaseName();
  const store = new MongoEventStore({
    client: databases.mongo,
    database,
    // The tests read completed deliveries back (to wait on them and replay them).
    completionHistory: true,
  });
  const shipments = databases.mongo
    .db(database)
    .collection<{ _id: string; status: string }>('shipments');
  const record: ServiceRecord = { handled: [], received: [] };
  const bus = new RelayBus({
    store,
    source: 'shipping',
    transports: [options.transport],
    routes:
      options.withoutRoutes === true
        ? []
        : [
            {
              event: OrderShipped,
              transport: options.transport.name,
              destination: options.destinations.shipments,
            },
          ],
    subscriptions: [
      {
        transport: options.transport.name,
        destinations: [options.destinations.orders],
      },
    ],
    catalogs: options.catalogs ?? [
      consume(OrderEvents, { events: ['OrderPlaced', 'OrderCancelled'] }),
      produce(OrderEvents, { events: ['OrderShipped'] }),
    ],
    dispatcher: { pollInterval: '20ms' },
    retry: { initialDelay: '50ms', maxDelay: '200ms', jitter: 'none', attempts: 3 },
    observers: [{ onReceived: ({ status }) => void record.received.push(status) }],
    logger: silentLogger,
  });
  const shipped = bus.forCatalog(OrderEvents, { events: ['OrderShipped'] });
  if (options.withoutHandlers !== true) {
    bus.subscribeCatalog(
      OrderEvents,
      {
        onOrderPlaced: async (event, context) => {
          record.handled.push(`placed:${event.payload.orderId}:${event.payload.total}`);
          await shipments.insertOne(
            { _id: event.payload.orderId, status: 'pending' },
            { session: context.transaction! },
          );
        },
        onOrderCancelled: async (event, context) => {
          record.handled.push(`cancelled:${event.payload.orderId}`);
          await shipments.updateOne(
            { _id: event.payload.orderId },
            { $set: { status: 'cancelled' } },
            { session: context.transaction! },
          );
        },
      },
      { events: ['OrderPlaced', 'OrderCancelled'], idPrefix: 'shipping', transactional: true },
    );
  }

  async function shipOrder(orderId: string, carrier: string): Promise<void> {
    await bus.transaction(async (transaction) => {
      await shipped.publish(new OrderShipped({ orderId, carrier }), { transaction, key: orderId });
    });
  }

  async function shipmentStatuses(): Promise<Record<string, string>> {
    const documents = await shipments.find().sort({ _id: 1 }).toArray();
    return Object.fromEntries(documents.map((document) => [document._id, document.status]));
  }

  return { bus, record, shipOrder, shipmentStatuses };
}

export async function eventually(
  condition: () => boolean | Promise<boolean>,
  description: string,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}
