import { Injectable } from '@nestjs/common';
import { defineCatalog, type EventOf } from '@relaybus/core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventConsumer, RelayBusConsumer, RelayBusHandler, Subscribe } from '../src/index.js';
import { OrderPlaced, busOf, createApplication, relayBusRoot } from './helpers.js';

afterEach(() => {
  vi.restoreAllMocks();
});

const OrderEvents = defineCatalog({ OrderPlaced }, { name: 'OrderEvents' });

@Injectable()
class CurrentNames {
  @RelayBusHandler(OrderPlaced, { id: 'method', transactional: true })
  onPlaced(): void {}
}

@Injectable()
@RelayBusConsumer(OrderEvents, { retry: { attempts: 3 } })
class CurrentConsumer {
  onOrderPlaced(event: EventOf<typeof OrderPlaced>): void {
    void event;
  }
}

@Injectable()
class DeprecatedNames {
  @Subscribe(OrderPlaced, { id: 'method', transactional: true })
  onPlaced(): void {}
}

@Injectable()
@EventConsumer(OrderEvents, { retry: { attempts: 3 } })
class DeprecatedConsumer {
  onOrderPlaced(event: EventOf<typeof OrderPlaced>): void {
    void event;
  }
}

async function handlersOf(providers: (new () => object)[]): Promise<unknown> {
  const moduleRef = await createApplication({ imports: [relayBusRoot()], providers });
  await moduleRef.init();
  const handlers = busOf(moduleRef)
    .handlers()
    .map(({ id, ...rest }) => ({
      id: id.replace(/^(Current|Deprecated)Consumer/, 'Consumer'),
      ...rest,
    }));
  await moduleRef.close();
  return handlers;
}

describe('deprecated decorator names', () => {
  it('are the same functions as the current names', () => {
    expect(Subscribe).toBe(RelayBusHandler);
    expect(EventConsumer).toBe(RelayBusConsumer);
  });

  it('register exactly the handlers the current names register', async () => {
    const current = await handlersOf([CurrentNames, CurrentConsumer]);
    const deprecated = await handlersOf([DeprecatedNames, DeprecatedConsumer]);
    expect(current).toHaveLength(2);
    expect(deprecated).toEqual(current);
  });
});
