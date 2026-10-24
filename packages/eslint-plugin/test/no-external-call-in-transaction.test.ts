import { noExternalCallInTransaction } from '../src/rules/no-external-call-in-transaction.js';
import { plainTester, tester } from './setup.js';

const error = (call: string) => ({ messageId: 'externalCall', data: { call } });

tester.run('no-external-call-in-transaction', noExternalCallInTransaction, {
  valid: [
    // Before and after the transaction.
    `async function place(bus) {
       const price = await fetch('https://pricing');
       await bus.transaction(async () => { await bus.publish(new OrderPlaced({ price })); });
       await fetch('https://audit');
     }`,
    // Outside any transaction, in a class.
    `class Service {
       @RelayBusConsumer(OrderPlaced)
       async ship() { await this.httpService.post('/ship'); await axios.get('/x'); }
       async plain() { await got('https://x'); }
     }`,
    // Not HTTP: a local function named like a client method, a non-transaction callback.
    `import { request } from 'some-lib';
     async function run(bus, items) {
       await bus.transaction(async () => { request(); undiciLike.request(); });
       items.forEach(async () => { await fetch('/x'); });
       await bus.subscribe(OrderPlaced, async () => { await fetch('/x'); }, { id: 'h' });
     }`,
    // A transactional option that is not literally true, computed members, a non-transaction member call.
    `class C {
       @RelayBusConsumer(OrderPlaced, { transactional: flag, [key]: true, ...rest })
       async a() { await fetch('/x'); }
       @Other()
       async b() { await fetch('/x'); }
       @RelayBusTransaction
       async c() { await fetch('/x'); }
       async ['d']() { await this['e'](); }
       async e() { await this[name](); await fetch('/x'); }
     }
     class D { async run() { await db.query(); bus[method](); helper(); } }
     const x = { async transaction() { await fetch('/x'); } };`,
    // Calls that are not to a named function, computed names, a parser without decorators.
    `import https from 'node:https';
     import { create } from 'axios';
     class C {
       async ['d']() { await fetch('/x'); }
       async run(bus, name) {
         await bus.transaction(async () => { getClient()(); undici.fetch(); https[method](); create(); });
         await bus[name](async () => { await fetch('/x'); });
       }
     }
     @RelayBusCatalogConsumer(OrderEvents, { transactional: true })
     class Handlers { async [handlerName]() { await fetch('/x'); } }`,
    // A catalog consumer without transactional, or a method that is not a handler.
    `@RelayBusCatalogConsumer(OrderEvents, { transactional: true })
     class Handlers { async helper() { await fetch('/x'); } }
     @RelayBusCatalogConsumer(OrderEvents)
     class Plain { async onOrderPlaced() { await fetch('/x'); } }
     const axiosInstance = require('lodash'); const y = require(name);`,
  ],
  invalid: [
    {
      code: `class C {
        @RelayBusTransaction()
        async [name]() { await fetch('/a'); }
        async other() { await fetch('/b'); }
      }`,
      errors: [error('fetch()')],
    },
    {
      code: `import { 'request' as send } from 'undici';
      async function run(bus) { await bus['transaction'](async () => { await send('/x'); }); }`,
      errors: [error('send()')],
    },
    {
      code: `class C {
        @RelayBusTransaction()
        async ['place']() { await fetch('/a'); }
        async other() { await fetch('/b'); }
      }`,
      errors: [error('fetch()')],
    },
    {
      code: `async function place(bus) {
        await bus.transaction(async (tx) => { await fetch('https://pricing'); });
      }`,
      errors: [error('fetch()')],
    },
    {
      code: `import axios from 'axios';
      import * as https from 'node:https';
      import { request as send } from 'undici';
      import http from 'http';
      class OrderService {
        @RelayBusTransaction()
        async place() {
          await axios.post('/charge');
          await axios('/charge');
          https.request('https://x');
          http.get('http://x');
          await send('https://x');
          await this.httpService.axiosRef.get('/x');
        }
      }`,
      errors: [
        error('axios.post()'),
        error('axios()'),
        error('https.request()'),
        error('http.get()'),
        error('send()'),
        error('this.httpService.axiosRef.get()'),
      ],
    },
    {
      code: `const client = require('got');
      class Handlers {
        @RelayBusConsumer(OrderPlaced, { id: 'h', transactional: true })
        async onPlaced() { await client.post('/x'); }
        @Subscribe(OrderPlaced, { transactional: true })
        async legacy() { await this.http.get('/x'); }
      }`,
      errors: [error('client.post()'), error('this.http.get()')],
    },
    {
      code: `@RelayBusCatalogConsumer(OrderEvents, { transactional: true })
      class Handlers { async onOrderPlaced() { await got('/x'); } }
      @EventConsumer(OrderEvents, { transactional: true })
      class Legacy { async onOrderShipped() { await fetch('/x'); } }`,
      errors: [error('got()'), error('fetch()')],
    },
    {
      // Helpers called through this from a transaction, transitively, and other transaction APIs.
      code: `class OrderService {
        @RelayBusTransaction()
        async place() { await this.#charge(); }
        async #charge() { await this.notify(); }
        async notify() { await fetch('/notify'); }
        async other() {
          await this.dataSource.transaction(async () => { await this.helper(); });
          await prisma.$transaction(async (tx) => { await fetch('/p'); });
          await session.withTransaction(async () => { await fetch('/m'); });
          bus.subscribe(OrderPlaced, async () => { await fetch('/h'); }, { transactional: true });
        }
        async helper() { await fetch('/helper'); }
      }`,
      errors: [
        error('fetch()'),
        error('fetch()'),
        error('fetch()'),
        error('fetch()'),
        error('fetch()'),
      ],
    },
  ],
});

plainTester.run(
  'no-external-call-in-transaction (JavaScript, no decorators)',
  noExternalCallInTransaction,
  {
    valid: ['class S { async run() { await fetch("/x"); } }'],
    invalid: [
      {
        code: 'class S { async run(bus) { await bus.transaction(async () => { await fetch("/x"); }); } }',
        errors: [error('fetch()')],
      },
    ],
  },
);
