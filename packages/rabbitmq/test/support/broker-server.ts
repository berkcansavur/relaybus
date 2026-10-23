import { RabbitMQContainer, type StartedRabbitMQContainer } from '@testcontainers/rabbitmq';

export const RABBITMQ_IMAGE = 'rabbitmq:4-alpine';

export interface RabbitMqServer {
  readonly amqpUrl: string;
  readonly amqpPort: number;
  readonly containerId: string;
  stop(): Promise<void>;
}

/** Starts a RabbitMQ container with its data directory on tmpfs. */
export async function startDedicatedBroker(): Promise<StartedRabbitMQContainer> {
  return new RabbitMQContainer(RABBITMQ_IMAGE).withTmpFs({ '/var/lib/rabbitmq': 'rw' }).start();
}

/** Starts the broker that every RabbitMQ test file shares for one run. */
export async function startRabbitMqServer(): Promise<RabbitMqServer> {
  const container = await startDedicatedBroker();
  return {
    amqpUrl: container.getAmqpUrl(),
    amqpPort: container.getMappedPort(5672),
    containerId: container.getId(),
    stop: async () => {
      await container.stop();
    },
  };
}
