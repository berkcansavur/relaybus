import type { Rule, SourceCode } from 'eslint';
import type * as ESTree from 'estree';
import { definitionName, isFunction, memberName } from './ast.js';
import { isTransactionScope, isTransactionalMethod } from './transaction-scopes.js';

/** A `this.method()` call and the class body it is written in. */
type ThisCall = {
  readonly node: Rule.Node;
  readonly method: string;
  readonly classBody: Rule.Node;
};

/**
 * Tells whether a node runs inside a transaction. Besides transaction callbacks and transactional
 * methods, a method of the same class that is called with `this.method()` from inside a transaction
 * counts as well, transitively, so helpers of a `@RelayBusTransaction()` method are covered.
 * Record every call with {@link record} while traversing; ask {@link isInTransaction} at `Program:exit`.
 */
export class TransactionTracker {
  readonly #sourceCode: SourceCode;
  readonly #thisCalls: ThisCall[] = [];
  readonly #transactional = new Map<Rule.Node, Set<string>>();

  constructor(sourceCode: SourceCode) {
    this.#sourceCode = sourceCode;
  }

  record(call: Rule.Node & ESTree.CallExpression): void {
    const { callee } = call;
    if (callee.type !== 'MemberExpression' || callee.object.type !== 'ThisExpression') {
      return;
    }
    const method = memberName(callee);
    const classBody = this.#ancestors(call).findLast((ancestor) => ancestor.type === 'ClassBody');
    if (method !== undefined && classBody !== undefined) {
      this.#thisCalls.push({ node: call, method, classBody });
    }
  }

  isInTransaction(node: Rule.Node): boolean {
    return this.#isInTransaction(node, (classBody) => this.#transactionalMethods(classBody));
  }

  #isInTransaction(node: Rule.Node, methodsOf: (classBody: Rule.Node) => Set<string>): boolean {
    for (const ancestor of this.#ancestors(node).reverse()) {
      if (isFunction(ancestor) && isTransactionScope(ancestor)) {
        return true;
      }
      if (ancestor.type === 'MethodDefinition') {
        return methodsOf(ancestor.parent).has(definitionName(ancestor) ?? '');
      }
    }
    return false;
  }

  #ancestors(node: Rule.Node): Rule.Node[] {
    return this.#sourceCode.getAncestors(node) as Rule.Node[];
  }

  /** Methods of a class that run in a transaction, directly or through `this.method()` calls. */
  #transactionalMethods(classBody: Rule.Node): Set<string> {
    const known = this.#transactional.get(classBody);
    if (known !== undefined) {
      return known;
    }
    const methods = new Set(
      (classBody as Rule.Node & ESTree.ClassBody).body
        .filter((member) => member.type === 'MethodDefinition')
        .filter((member) => isTransactionalMethod(member as Rule.Node & ESTree.MethodDefinition))
        .map((member) => definitionName(member) ?? ''),
    );
    const calls = this.#thisCalls.filter((call) => call.classBody === classBody);
    for (let grew = true; grew;) {
      const added = calls.filter(
        (call) => !methods.has(call.method) && this.#isInTransaction(call.node, () => methods),
      );
      added.forEach((call) => methods.add(call.method));
      grew = added.length > 0;
    }
    this.#transactional.set(classBody, methods);
    return methods;
  }
}
