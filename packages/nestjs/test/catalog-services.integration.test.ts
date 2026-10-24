/**
 * Two NestJS applications, a producer and a consumer, that share one catalog
 * and talk through a real RabbitMQ broker. Each application has its own store
 * and its own RelayBusModule.
 */
import { Inject, Injectable, Logger, Module, type DynamicModule } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import {
  InMemoryEventStore,
  RelayBus,
  consume,
  defineCatalog,
  defineEvent,
  produce,
  type CatalogDeclaration,
  type EventOf,
} from '@relaybus/core';
import { RabbitMqTransport } from '@relaybus/rabbitmq';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { eventually, startBroker, unique } from '../../rabbitmq/test/support/broker.js';
import { EventConsumer, RelayBusModule } from '../src/index.js';

class InvoiceIssued extends defineEvent<{ invoiceId: string; amount: number }>(
  'invoice.issued.v1',
) {}
class InvoicePaid extends defineEvent<{ invoiceId: string }>('invoice.paid.v1') {}

/** The contracts module both applications import. */
const InvoiceEvents = defineCatalog({ InvoiceIssued, InvoicePaid }, { name: 'InvoiceEvents' });

const silent = { log: () => undefined, warn: () => undefined, error: () => undefined };

let broker: Awaited<ReturnType<typeof startBroker>>;
const applications: TestingModule[] = [];

beforeAll(async () => {
  broker = await startBroker();
});

afterEach(async () => {
  await Promise.all(applications.splice(0).map((application) => application.close()));
  vi.restoreAllMocks();
});

afterAll(async () => {
  await broker.stop();
});

function transport(exchange: string, consumerGroup?: string): RabbitMqTransport {
  return new RabbitMqTransport({
    name: 'rabbit',
    url: broker.getAmqpUrl(),
    exchange,
    ordered: true,
    ...(consumerGroup === undefined ? {} : { consumerGroup }),
    reconnect: { initialDelay: '100ms', maxDelay: '500ms' },
    logger: silent,
  });
}

/** Issues invoices; the billing application publishes InvoiceIssued and nothing else. */
@Injectable()
class BillingService {
  constructor(@Inject(RelayBus) private readonly bus: RelayBus<unknown>) {}

  async issue(invoiceId: string, amount: number): Promise<void> {
    const invoices = this.bus.forCatalog(InvoiceEvents, { events: ['InvoiceIssued'] });
    await this.bus.transaction((transaction) =>
      invoices.publish(new InvoiceIssued({ invoiceId, amount }), { transaction, key: invoiceId }),
    );
  }
}

function billingModule(exchange: string, catalogs: readonly CatalogDeclaration[]): DynamicModule {
  @Module({
    imports: [
      RelayBusModule.forRoot({
        store: new InMemoryEventStore(),
        source: 'billing',
        transports: [transport(exchange)],
        routes: [{ event: InvoiceIssued, transport: 'rabbit', destination: 'invoices' }],
        catalogs,
        dispatcher: { pollInterval: '20ms' },
      }),
    ],
    providers: [BillingService],
  })
  class BillingModule {}
  return { module: BillingModule };
}

/** Records the invoices it is told about; consumes InvoiceIssued only. */
@Injectable()
@EventConsumer(InvoiceEvents, { events: ['InvoiceIssued'] })
class LedgerHandlers {
  readonly booked: string[] = [];

  onInvoiceIssued(event: EventOf<typeof InvoiceIssued>): void {
    this.booked.push(`${event.payload.invoiceId}:${event.payload.amount}`);
  }

  /** Not selected: the ledger does not consume InvoicePaid, so this never runs. */
  onInvoicePaid(): void {
    this.booked.push('paid');
  }
}

function ledgerModule(
  exchange: string,
  catalogs: readonly CatalogDeclaration[],
  withHandlers: boolean,
): DynamicModule {
  @Module({
    imports: [
      RelayBusModule.forRoot({
        store: new InMemoryEventStore(),
        source: 'ledger',
        transports: [transport(exchange, unique('ledger'))],
        subscriptions: [{ transport: 'rabbit', destinations: ['invoices'] }],
        catalogs,
        dispatcher: { pollInterval: '20ms' },
      }),
    ],
    providers: withHandlers ? [LedgerHandlers] : [],
  })
  class LedgerModule {}
  return { module: LedgerModule };
}

async function boot(module: DynamicModule): Promise<TestingModule> {
  vi.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
  const application = await Test.createTestingModule({ imports: [module] }).compile();
  applications.push(application);
  await application.init();
  return application;
}

describe('two NestJS applications sharing one catalog over RabbitMQ', () => {
  it('delivers an event from the producer application to the consumer application', async () => {
    const exchange = unique('invoices');
    const ledger = await boot(
      ledgerModule(exchange, [consume(InvoiceEvents, { events: ['InvoiceIssued'] })], true),
    );
    const billing = await boot(
      billingModule(exchange, [produce(InvoiceEvents, { events: ['InvoiceIssued'] })]),
    );

    await billing.get(BillingService).issue('inv-1', 120);
    await billing.get(BillingService).issue('inv-2', 80);
    const handlers = ledger.get(LedgerHandlers);
    await eventually(() => handlers.booked.length === 2, 60_000, 'two booked invoices');

    // The invoices are published without a key, and unkeyed events carry no order guarantee.
    expect([...handlers.booked].sort()).toEqual(['inv-1:120', 'inv-2:80']);
    expect(
      ledger
        .get(RelayBus)
        .handlers()
        .map((handler) => handler.id),
    ).toEqual(['LedgerHandlers.onInvoiceIssued']);
  });

  it('fails to boot a consumer application that lacks a handler for a consumed event', async () => {
    await expect(
      boot(ledgerModule(unique('invoices'), [consume(InvoiceEvents)], true)),
    ).rejects.toThrow(
      [
        'Catalog events this service does not cover:',
        '  - InvoiceEvents.InvoicePaid (invoice.paid.v1): consumed here, needs handler method onInvoicePaid',
      ].join('\n'),
    );
    await expect(
      boot(
        ledgerModule(
          unique('invoices'),
          [consume(InvoiceEvents, { events: ['InvoiceIssued'] })],
          false,
        ),
      ),
    ).rejects.toThrow(
      'InvoiceEvents.InvoiceIssued (invoice.issued.v1): consumed here, needs handler method onInvoiceIssued',
    );
  });

  it('fails to boot a producer application that has no route for a produced event', async () => {
    await expect(boot(billingModule(unique('invoices'), [produce(InvoiceEvents)]))).rejects.toThrow(
      [
        'Catalog events this service does not cover:',
        '  - InvoiceEvents.InvoicePaid (invoice.paid.v1): produced here, needs a route to a transport',
      ].join('\n'),
    );
  });
});
