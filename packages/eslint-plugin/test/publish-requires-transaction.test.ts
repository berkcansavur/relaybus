import { publishRequiresTransaction } from '../src/rules/publish-requires-transaction.js';
import { tester } from './setup.js';

const error = (call: string) => ({ messageId: 'noTransaction', data: { call } });

tester.run('publish-requires-transaction', publishRequiresTransaction, {
  valid: [
    `class OrderService {
       @RelayBusTransaction()
       async place() { await this.bus.publish(new OrderPlaced({})); await this.emit(); }
       async emit() { await this.eventBus.publishAll([]); }
       async explicit(tx) {
         await this.bus.publish(e, { transaction: tx });
         await this.bus.publish(e, options);
         await this.bus.publish(e, { ...options });
         await this.bus.publish(e, { ['transaction']: tx });
       }
     }`,
    `await bus.transaction(async () => { await bus.publish(e); await orderPublisher.publish(e); });`,
    // Not a relaybus publisher, and not a publish method.
    `await redis.publish('channel', 'message'); await bus.emit(e); await bus[method](e); await bus[0](e);
     await getBus().publish(e);`,
    {
      code: `await events.publish(e, { transaction: tx });`,
      options: [{ publishers: '^events$' }],
    },
  ],
  invalid: [
    {
      code: `class OrderService {
        async place() { await this.bus.publish(new OrderPlaced({}), { [transaction]: tx }); }
        async many() { await this.bus.publishAll([e], { key: 'k' }); }
      }`,
      errors: [error('this.bus.publish()'), error('this.bus.publishAll()')],
    },
    {
      code: `await publisher.publish(e); await relayBus.publish(e); await bus['publish'](e);`,
      errors: [
        error('publisher.publish()'),
        error('relayBus.publish()'),
        error("bus['publish']()"),
      ],
    },
    {
      code: `await events.publish(e);`,
      options: [{ publishers: '^events$' }],
      errors: [error('events.publish()')],
    },
  ],
});
