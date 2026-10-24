import type { Rule } from 'eslint';
import type * as ESTree from 'estree';
import { definitionName, lastName, memberName } from '../ast.js';
import { TransactionTracker } from '../transaction-tracker.js';

const PUBLISH_METHODS = new Set(['publish', 'publishAll']);
/** Default names of the objects whose `publish` is a relaybus publish: `bus`, `this.eventBus`, `orderPublisher`. */
const DEFAULT_PUBLISHERS = '(?:^|[a-z_$])(?:[Bb]us|[Pp]ublisher)$|^(?:bus|publisher)$';

type Options = { readonly publishers?: string };

/**
 * Whether `call` passes a transaction itself: `{ transaction }` in its options, or options the
 * rule cannot see into (a variable, a spread).
 */
function passesTransaction(call: ESTree.CallExpression): boolean {
  const options = call.arguments[1];
  if (options === undefined) {
    return false;
  }
  if (options.type !== 'ObjectExpression') {
    return true;
  }
  return options.properties.some(
    (property) => property.type === 'SpreadElement' || definitionName(property) === 'transaction',
  );
}

/**
 * Reports `bus.publish` / `bus.publishAll` outside a transaction: not in a
 * `@RelayBusTransaction()` method, a transactional handler or a `.transaction` callback (or a
 * method they call through `this`), and without `{ transaction }`.
 */
export const publishRequiresTransaction: Rule.RuleModule = {
  meta: {
    type: 'problem',
    docs: {
      description: 'Require relaybus publish calls to run inside a transaction',
      url: 'https://berkcansavur.github.io/relaybus/recipes/transactions-in-nestjs',
    },
    schema: [
      {
        type: 'object',
        properties: { publishers: { type: 'string' } },
        additionalProperties: false,
      },
    ],
    messages: {
      noTransaction:
        '{{call}} runs outside a transaction, so the event is not atomic with your writes and ' +
        'relaybus throws TransactionRequiredError. Publish inside a @RelayBusTransaction() method ' +
        'or a bus.transaction(...) callback, or pass { transaction }.',
    },
  },
  create(context) {
    const [options = {}] = context.options as [Options?];
    const publishers = new RegExp(options.publishers ?? DEFAULT_PUBLISHERS);
    const tracker = new TransactionTracker(context.sourceCode);
    const publishes: (Rule.Node & ESTree.CallExpression)[] = [];
    return {
      CallExpression: (node) => {
        tracker.record(node);
        const { callee } = node;
        if (
          callee.type === 'MemberExpression' &&
          PUBLISH_METHODS.has(memberName(callee) ?? '') &&
          publishers.test(lastName(callee.object) ?? '')
        ) {
          publishes.push(node);
        }
      },
      'Program:exit': () => {
        for (const call of publishes) {
          if (!passesTransaction(call) && !tracker.isInTransaction(call)) {
            context.report({
              node: call,
              messageId: 'noTransaction',
              data: { call: `${context.sourceCode.getText(call.callee)}()` },
            });
          }
        }
      },
    };
  },
};
