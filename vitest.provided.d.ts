import 'vitest';

declare module 'vitest' {
  /** Connection details of the servers started once per run by `vitest.global-setup.ts`. */
  export interface ProvidedContext {
    /** Postgres connection URI by image name. */
    postgresServers: Record<string, string>;
    mongoConnectionString: string;
    rabbitmqBroker: { amqpUrl: string; amqpPort: number; containerId: string };
    kafkaBrokers: string[];
  }
}
