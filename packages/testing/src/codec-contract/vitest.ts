import { describe, it } from 'vitest';
import { registerKafkaCodecContract, type KafkaCodecContractOptions } from './kafka.js';
import { registerRabbitMqCodecContract, type RabbitMqCodecContractOptions } from './rabbitmq.js';

// The codec contract registered with Vitest, as `@relaybus/testing` has exported it since 0.4.0.

/** Registers one Vitest test per captured Kafka record (see `registerKafkaCodecContract`). */
export function kafkaCodecContract(name: string, options: KafkaCodecContractOptions): void {
  registerKafkaCodecContract({ describe, it }, { name, options });
}

/** Registers one Vitest test per captured RabbitMQ message (see `registerRabbitMqCodecContract`). */
export function rabbitMqCodecContract(name: string, options: RabbitMqCodecContractOptions): void {
  registerRabbitMqCodecContract({ describe, it }, { name, options });
}
