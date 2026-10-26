// The codec contract without a test framework: `@relaybus/testing/codec-contract`.

export {
  assertKafkaCodecCase,
  kafkaCodecMismatches,
  registerKafkaCodecContract,
  type KafkaCapturedRecord,
  type KafkaCodecCase,
  type KafkaCodecContractOptions,
  type KafkaDeliveredRecord,
  type KafkaWrittenRecord,
} from './kafka.js';
export {
  assertRabbitMqCodecCase,
  rabbitMqCodecMismatches,
  registerRabbitMqCodecContract,
  type RabbitMqCapturedMessage,
  type RabbitMqCodecCase,
  type RabbitMqCodecContractOptions,
  type RabbitMqDeliveredMessage,
  type RabbitMqWrittenMessage,
} from './rabbitmq.js';
export { CodecContractError, type TestFramework } from './framework.js';
export type { BodyComparison, WireBytes } from './compare.js';
