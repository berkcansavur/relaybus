import type { Rule } from 'eslint';
import type * as ESTree from 'estree';
import { ExternalCallDetector } from '../external-calls.js';
import { TransactionTracker } from '../transaction-tracker.js';

/**
 * Reports calls to other services (HTTP clients) made inside a transaction: a
 * `@RelayBusTransaction()` method, a transactional handler, a `bus.transaction` (or other
 * `.transaction`) callback, and the methods they call through `this`.
 */
export const noExternalCallInTransaction: Rule.RuleModule = {
  meta: {
    type: 'problem',
    docs: {
      description: 'Disallow calls to other services inside a database transaction',
      url: 'https://berkcansavur.github.io/relaybus/recipes/transactions-in-nestjs',
    },
    schema: [],
    messages: {
      externalCall:
        '{{call}} calls another service inside a transaction. The transaction keeps its row locks ' +
        'and its database connection until the call returns, and relaybus rolls it back with ' +
        'TransactionTimeoutError once it outlives transactionTimeout. Make the call before the ' +
        'transaction starts or after it commits (for example from the handler of an event you publish).',
    },
  },
  create(context) {
    const tracker = new TransactionTracker(context.sourceCode);
    const detector = new ExternalCallDetector();
    const calls: (Rule.Node & ESTree.CallExpression)[] = [];
    return {
      ImportDeclaration: (node) => detector.recordImport(node),
      VariableDeclarator: (node) => detector.recordRequire(node),
      CallExpression: (node) => {
        tracker.record(node);
        calls.push(node);
      },
      'Program:exit': () => {
        for (const call of calls) {
          if (detector.isExternalCall(call) && tracker.isInTransaction(call)) {
            context.report({
              node: call,
              messageId: 'externalCall',
              data: { call: `${context.sourceCode.getText(call.callee)}()` },
            });
          }
        }
      },
    };
  },
};
