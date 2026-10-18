import { Test, type TestingModule } from '@nestjs/testing';
import type { ModuleMetadata } from '@nestjs/common';
import { Logger } from '@nestjs/common';
import { InMemoryEventStore, RelayBus, defineEvent } from '@relaybus/core';
import { vi } from 'vitest';
import { RelayBusModule, type RelayBusModuleOptions } from '../src/index.js';

export class OrderPlaced extends defineEvent<{ orderId: string }>('order.placed.v1') {}

/** Options that keep the dispatcher off so tests drive deliveries with `bus.drain()`. */
export function manualOptions(store = new InMemoryEventStore()): RelayBusModuleOptions {
  return { store, dispatcher: { enabled: false } };
}

export async function createApplication(
  metadata: ModuleMetadata,
  keepOutputQuiet = true,
): Promise<TestingModule> {
  if (keepOutputQuiet) {
    vi.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
  }
  const moduleRef = await Test.createTestingModule(metadata).compile();
  return moduleRef;
}

export function relayBusRoot(options: RelayBusModuleOptions = manualOptions()) {
  return RelayBusModule.forRoot(options);
}

export function busOf(moduleRef: TestingModule): RelayBus {
  return moduleRef.get(RelayBus);
}
