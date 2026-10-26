import {
  registerKafkaCodecContract,
  type KafkaCodecContractOptions,
} from './codec-contract/kafka.js';
import {
  registerRabbitMqCodecContract,
  type RabbitMqCodecContractOptions,
} from './codec-contract/rabbitmq.js';
import type { TestFramework } from './codec-contract/framework.js';

// The codec contract registered with the `describe` and `it` globals of Jest:
// `@relaybus/testing/jest`. Imports no test framework, so it needs no Vitest.

/** The `describe` and `it` globals; a clear error when the runner does not provide them. */
function globalFramework(): TestFramework {
  const scope = globalThis as { readonly describe?: unknown; readonly it?: unknown };
  if (typeof scope.describe !== 'function' || typeof scope.it !== 'function') {
    throw new Error(
      '@relaybus/testing/jest needs the describe and it globals of Jest; with another runner use registerKafkaCodecContract or registerRabbitMqCodecContract from @relaybus/testing/codec-contract',
    );
  }
  return {
    describe: scope.describe as TestFramework['describe'],
    it: scope.it as TestFramework['it'],
  };
}

/** Registers one Jest test per captured Kafka record (see `registerKafkaCodecContract`). */
export function kafkaCodecContract(name: string, options: KafkaCodecContractOptions): void {
  registerKafkaCodecContract(globalFramework(), { name, options });
}

/** Registers one Jest test per captured RabbitMQ message (see `registerRabbitMqCodecContract`). */
export function rabbitMqCodecContract(name: string, options: RabbitMqCodecContractOptions): void {
  registerRabbitMqCodecContract(globalFramework(), { name, options });
}

export * from './codec-contract/index.js';
