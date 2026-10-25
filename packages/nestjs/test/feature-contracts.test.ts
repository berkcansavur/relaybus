import { Injectable, Module } from '@nestjs/common';
import {
  ConfigurationError,
  InMemoryEventStore,
  consume,
  defineCatalog,
  defineEvent,
  produce,
  type EventOf,
} from '@relaybus/core';
import { InMemoryTransport } from '@relaybus/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  RelayBusCatalogConsumer,
  RelayBusConsumer,
  RelayBusFeatureContract,
  RelayBusModule,
  type RelayBusModuleOptions,
} from '../src/index.js';
import { OrderPlaced, createApplication, manualOptions, relayBusRoot } from './helpers.js';

class OrderCancelled extends defineEvent<{ orderId: string }>('order.cancelled.v1') {}
class InvoiceIssued extends defineEvent<{ invoiceId: string }>('invoice.issued.v1') {}

const OrderEvents = defineCatalog({ OrderPlaced, OrderCancelled }, { name: 'OrderEvents' });
const BillingEvents = defineCatalog({ InvoiceIssued }, { name: 'BillingEvents' });

@Injectable()
class PlacedHandlers {
  @RelayBusConsumer(OrderPlaced)
  async chargeCard(event: EventOf<typeof OrderPlaced>): Promise<void> {
    void event;
  }
}

@Injectable()
class CancelledHandlers {
  @RelayBusConsumer(OrderCancelled)
  async refund(event: EventOf<typeof OrderCancelled>): Promise<void> {
    void event;
  }
}

afterEach(() => {
  vi.restoreAllMocks();
});

async function boot(
  modules: (new () => object)[],
  options: RelayBusModuleOptions = manualOptions(),
): Promise<void> {
  const moduleRef = await createApplication({ imports: [relayBusRoot(options), ...modules] });
  try {
    await moduleRef.init();
  } finally {
    await moduleRef.close();
  }
}

function featureModule(
  name: string,
  {
    consumes = [],
    produces = [],
    providers = [],
  }: {
    readonly consumes?: Parameters<typeof RelayBusModule.forFeature>[0]['consumes'];
    readonly produces?: Parameters<typeof RelayBusModule.forFeature>[0]['produces'];
    readonly providers?: (new (...args: never[]) => object)[];
  },
): new () => object {
  @Module({ imports: [RelayBusModule.forFeature({ consumes, produces })], providers })
  class Feature {}
  Object.defineProperty(Feature, 'name', { value: name });
  return Feature;
}

describe('RelayBusModule.forFeature', () => {
  it('boots when providers of the module consume every event, spread over classes', async () => {
    const Billing = featureModule('BillingModule', {
      consumes: [OrderEvents],
      providers: [PlacedHandlers, CancelledHandlers],
    });
    await expect(boot([Billing])).resolves.toBeUndefined();
  });

  it('fails the boot naming the module and each event without a consumer', async () => {
    const Billing = featureModule('BillingModule', {
      consumes: [OrderEvents],
      providers: [PlacedHandlers],
    });
    const failure = boot([Billing]);
    await expect(failure).rejects.toThrow(ConfigurationError);
    await expect(boot([Billing])).rejects.toThrow(
      '[RelayBusHandlerExplorer](assertModuleContracts) BillingModule: OrderEvents.OrderCancelled (order.cancelled.v1) has no consumer',
    );
  });

  it('does not count consumers of another module', async () => {
    const Billing = featureModule('BillingModule', { consumes: [OrderEvents] });
    const Shipping = featureModule('ShippingModule', {
      providers: [PlacedHandlers, CancelledHandlers],
    });
    await expect(boot([Billing, Shipping])).rejects.toThrow(
      'BillingModule: OrderEvents.OrderPlaced (order.placed.v1) has no consumer; ' +
        'BillingModule: OrderEvents.OrderCancelled (order.cancelled.v1) has no consumer',
    );
  });

  it('requires only the declared events and accepts catalog consumers', async () => {
    @Injectable()
    @RelayBusCatalogConsumer(OrderEvents, { events: ['OrderCancelled'] })
    class CancellationConsumer {
      onOrderCancelled(event: EventOf<typeof OrderCancelled>): void {
        void event;
      }
    }
    const Billing = featureModule('BillingModule', {
      consumes: [consume(OrderEvents, { events: ['OrderCancelled'] })],
      providers: [CancellationConsumer],
    });
    await expect(boot([Billing])).resolves.toBeUndefined();
  });

  it('requires a route for every produced event', async () => {
    const Billing = featureModule('BillingModule', {
      produces: [BillingEvents, produce(OrderEvents, { events: ['OrderPlaced'] })],
    });
    await expect(boot([Billing])).rejects.toThrow(
      'BillingModule: BillingEvents.InvoiceIssued (invoice.issued.v1) has no route to a transport; ' +
        'BillingModule: OrderEvents.OrderPlaced (order.placed.v1) has no route to a transport',
    );

    const routed: RelayBusModuleOptions = {
      store: new InMemoryEventStore(),
      dispatcher: { enabled: false },
      transports: [new InMemoryTransport('memory')],
      routes: [
        { event: InvoiceIssued, transport: 'memory' },
        { event: OrderPlaced, transport: 'memory' },
      ],
    };
    await expect(boot([Billing], routed)).resolves.toBeUndefined();
  });

  it('names events of an unnamed catalog after the catalog role and takes empty options', () => {
    const contract = new RelayBusFeatureContract({
      consumes: [defineCatalog({ OrderPlaced })],
    });
    expect(contract.consumes).toEqual([{ label: 'catalog.OrderPlaced', type: 'order.placed.v1' }]);
    expect(new RelayBusFeatureContract({})).toMatchObject({ consumes: [], produces: [] });
  });

  it('rejects a declaration of the other role and anything that is not a catalog', () => {
    expect(() => RelayBusModule.forFeature({ consumes: [produce(OrderEvents)] })).toThrow(
      'RelayBusModule.forFeature consumes takes catalogs and consume(...) declarations, received produce(OrderEvents)',
    );
    expect(() => RelayBusModule.forFeature({ produces: [consume(OrderEvents)] })).toThrow(
      'RelayBusModule.forFeature produces takes catalogs and produce(...) declarations, received consume(OrderEvents)',
    );
    expect(() => RelayBusModule.forFeature({ consumes: [{}] })).toThrow(ConfigurationError);
  });
});
