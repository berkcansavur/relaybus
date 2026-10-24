import { Inject, Injectable, Logger, Module } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { InMemoryEventStore, RelayBus } from '@relaybus/core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  RelayBusHealth,
  RelayBusModule,
  type RelayBusModuleOptions,
  type RelayBusModuleOptionsFactory,
} from '../src/index.js';
import { busOf, createApplication, manualOptions, relayBusRoot } from './helpers.js';

afterEach(() => {
  vi.restoreAllMocks();
});

class FailingSetupStore extends InMemoryEventStore {
  constructor(private readonly reason: string) {
    super();
  }

  setup(): Promise<void> {
    return Promise.reject(new Error(this.reason));
  }
}

async function initialise(moduleRef: TestingModule): Promise<TestingModule> {
  await moduleRef.init();
  return moduleRef;
}

describe('RelayBusModule.forRoot', () => {
  it('provides a RelayBus built from the options and starts it on init', async () => {
    const store = new InMemoryEventStore();
    const moduleRef = await initialise(
      await createApplication({
        imports: [
          RelayBusModule.forRoot({ store, source: 'orders', dispatcher: { enabled: false } }),
        ],
      }),
    );

    const bus = busOf(moduleRef);

    expect(bus).toBeInstanceOf(RelayBus);
    expect(bus.store).toBe(store);
    expect(bus.isRunning).toBe(true);
    await moduleRef.close();
  });

  it('stops the bus when the application closes', async () => {
    const moduleRef = await initialise(await createApplication({ imports: [relayBusRoot()] }));
    const bus = busOf(moduleRef);

    await moduleRef.close();

    expect(bus.isRunning).toBe(false);
  });

  it('forwards bus log lines to a Nest Logger with the RelayBus context', async () => {
    const log = vi.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
    const moduleRef = await initialise(
      await createApplication({ imports: [relayBusRoot()] }, false),
    );

    expect(log).toHaveBeenCalledWith(
      expect.stringContaining('[RelayBus](startComponents) relaybus started'),
    );
    const contexts = log.mock.contexts as Logger[];
    expect(
      contexts.some((logger) => (logger as unknown as { context: string }).context === 'RelayBus'),
    ).toBe(true);
    await moduleRef.close();
  });

  it('fails application init when the store cannot be set up', async () => {
    const store = new FailingSetupStore('database unreachable');
    const moduleRef = await createApplication({ imports: [relayBusRoot(manualOptions(store))] });

    await expect(moduleRef.init()).rejects.toThrow('database unreachable');
    expect(busOf(moduleRef).isRunning).toBe(false);
  });

  it('runs the dispatcher by default', async () => {
    const moduleRef = await initialise(
      await createApplication({
        imports: [RelayBusModule.forRoot({ store: new InMemoryEventStore() })],
      }),
    );

    expect(busOf(moduleRef).isRunning).toBe(true);
    await moduleRef.close();
  });
});

describe('RelayBusModule global option', () => {
  @Injectable()
  class Consumer {
    constructor(@Inject(RelayBus) readonly bus: RelayBus) {}
  }

  @Module({ providers: [Consumer], exports: [Consumer] })
  class ConsumerModule {}

  it('is global by default', async () => {
    const moduleRef = await initialise(
      await createApplication({ imports: [relayBusRoot(), ConsumerModule] }),
    );

    expect(moduleRef.get(Consumer).bus).toBe(busOf(moduleRef));
    expect(moduleRef.get(RelayBusHealth, { strict: false })).toBeInstanceOf(RelayBusHealth);
    await moduleRef.close();
  });

  it('is not injectable elsewhere with isGlobal false unless imported', async () => {
    await expect(
      createApplication({
        imports: [RelayBusModule.forRoot({ ...manualOptions(), isGlobal: false }), ConsumerModule],
      }),
    ).rejects.toThrow(/RelayBus/);
  });

  it('is injectable through an explicit import with isGlobal false', async () => {
    const relayBusModule = RelayBusModule.forRoot({ ...manualOptions(), isGlobal: false });
    @Module({ imports: [relayBusModule], providers: [Consumer] })
    class ImportingModule {}
    const moduleRef = await initialise(
      await createApplication({ imports: [relayBusModule, ImportingModule] }),
    );

    expect(moduleRef.get(Consumer, { strict: false }).bus).toBeInstanceOf(RelayBus);
    await moduleRef.close();
  });
});

describe('RelayBusModule.forRootAsync', () => {
  it('builds the options with useFactory and injected providers', async () => {
    @Injectable()
    class ConfigService {
      readonly source = 'from-config';
    }
    @Module({ providers: [ConfigService], exports: [ConfigService] })
    class ConfigModule {}

    const store = new InMemoryEventStore();
    const moduleRef = await initialise(
      await createApplication({
        imports: [
          RelayBusModule.forRootAsync({
            imports: [ConfigModule],
            inject: [ConfigService],
            useFactory: (config: ConfigService) => ({
              store,
              source: config.source,
              dispatcher: { enabled: false },
            }),
          }),
        ],
      }),
    );
    expect(busOf(moduleRef).store).toBe(store);
    await moduleRef.close();
  });

  it('accepts an async useFactory', async () => {
    const moduleRef = await initialise(
      await createApplication({
        imports: [
          RelayBusModule.forRootAsync({ useFactory: () => Promise.resolve(manualOptions()) }),
        ],
      }),
    );

    expect(busOf(moduleRef).isRunning).toBe(true);
    await moduleRef.close();
  });

  it('builds the options with useClass', async () => {
    @Injectable()
    class OptionsFactory implements RelayBusModuleOptionsFactory {
      create(): RelayBusModuleOptions {
        return manualOptions();
      }
    }
    const moduleRef = await initialise(
      await createApplication({
        imports: [RelayBusModule.forRootAsync({ useClass: OptionsFactory })],
      }),
    );

    expect(busOf(moduleRef).isRunning).toBe(true);
    await moduleRef.close();
  });

  it('builds the options with useExisting', async () => {
    @Injectable()
    class OptionsFactory implements RelayBusModuleOptionsFactory {
      create(): RelayBusModuleOptions {
        return manualOptions();
      }
    }
    @Module({ providers: [OptionsFactory], exports: [OptionsFactory] })
    class OptionsModule {}

    const moduleRef = await initialise(
      await createApplication({
        imports: [
          RelayBusModule.forRootAsync({ imports: [OptionsModule], useExisting: OptionsFactory }),
        ],
      }),
    );

    expect(busOf(moduleRef).isRunning).toBe(true);
    await moduleRef.close();
  });

  it('is global by default and honours isGlobal false', async () => {
    @Injectable()
    class Consumer {
      constructor(@Inject(RelayBus) readonly bus: RelayBus) {}
    }
    @Module({ providers: [Consumer] })
    class ConsumerModule {}

    const globalRef = await initialise(
      await createApplication({
        imports: [
          RelayBusModule.forRootAsync({ useFactory: () => manualOptions() }),
          ConsumerModule,
        ],
      }),
    );
    expect(globalRef.get(Consumer, { strict: false }).bus).toBeInstanceOf(RelayBus);
    await globalRef.close();

    await expect(
      createApplication({
        imports: [
          RelayBusModule.forRootAsync({ isGlobal: false, useFactory: () => manualOptions() }),
          ConsumerModule,
        ],
      }),
    ).rejects.toThrow(/RelayBus/);
  });

  it('fails application init when the factory options make the store fail', async () => {
    const store = new FailingSetupStore('no database');
    const moduleRef = await createApplication({
      imports: [RelayBusModule.forRootAsync({ useFactory: () => manualOptions(store) })],
    });

    await expect(moduleRef.init()).rejects.toThrow('no database');
  });
});
