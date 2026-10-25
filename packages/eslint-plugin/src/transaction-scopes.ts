import type { Rule } from 'eslint';
import type * as ESTree from 'estree';
import {
  findDecorator,
  definitionName,
  hasTransactionalOption,
  memberName,
  type FunctionNode,
} from './ast.js';

/** Methods whose callback runs in a database transaction: relaybus, TypeORM, Knex, Sequelize, Prisma, MongoDB sessions. */
const TRANSACTION_METHODS = new Set(['transaction', '$transaction', 'withTransaction']);
const TRANSACTION_DECORATORS = new Set(['RelayBusTransaction']);
/** Method decorators of relaybus handlers, current name and deprecated alias. */
const CONSUMER_DECORATORS = new Set(['RelayBusConsumer', 'Subscribe']);
/** Class decorators of relaybus catalog handlers, current name and deprecated alias. */
const CATALOG_CONSUMER_DECORATORS = new Set(['RelayBusCatalogConsumer', 'EventConsumer']);
const CATALOG_HANDLER_NAME = /^on[A-Z]/;

/**
 * Whether `method` runs in a transaction by itself: `@RelayBusTransaction()`, a
 * `@RelayBusConsumer(..., { transactional: true })` handler, or an `on<Event>` method of a class
 * marked `@RelayBusCatalogConsumer(..., { transactional: true })` (or the deprecated names).
 */
export function isTransactionalMethod(method: Rule.Node & ESTree.MethodDefinition): boolean {
  if (findDecorator(method, TRANSACTION_DECORATORS) !== undefined) {
    return true;
  }
  const consumer = findDecorator(method, CONSUMER_DECORATORS);
  if (consumer !== undefined && hasTransactionalOption(consumer)) {
    return true;
  }
  // A method's parent is the class body, whose parent is the class.
  const catalogConsumer = findDecorator(method.parent.parent!, CATALOG_CONSUMER_DECORATORS);
  return (
    catalogConsumer !== undefined &&
    hasTransactionalOption(catalogConsumer) &&
    CATALOG_HANDLER_NAME.test(definitionName(method) ?? '')
  );
}

/**
 * Whether `fn` runs in a transaction: a transactional method, the callback of
 * `x.transaction(fn)` (`bus.transaction`, `dataSource.transaction`, `prisma.$transaction`,
 * `session.withTransaction`), or a handler subscribed with `{ transactional: true }`.
 */
export function isTransactionScope(fn: FunctionNode): boolean {
  const parent = fn.parent;
  if (parent.type === 'MethodDefinition') {
    return isTransactionalMethod(parent);
  }
  if (parent.type !== 'CallExpression' || parent.callee.type !== 'MemberExpression') {
    return false;
  }
  const method = memberName(parent.callee);
  if (TRANSACTION_METHODS.has(method ?? '')) {
    return parent.arguments[0] === fn;
  }
  return method === 'subscribe' && parent.arguments[1] === fn && hasTransactionalOption(parent);
}
