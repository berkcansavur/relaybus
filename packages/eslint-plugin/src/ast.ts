import type { Rule } from 'eslint';
import type * as ESTree from 'estree';

/** A function node as ESLint hands it to rules, with its parent. */
export type FunctionNode = Rule.Node &
  (ESTree.FunctionExpression | ESTree.ArrowFunctionExpression | ESTree.FunctionDeclaration);

/** What the TypeScript parser adds to classes and class members. */
type Decorated = { readonly decorators?: readonly { readonly expression: ESTree.Node }[] };

const FUNCTION_TYPES = new Set([
  'FunctionExpression',
  'ArrowFunctionExpression',
  'FunctionDeclaration',
]);

export function isFunction(node: Rule.Node): node is FunctionNode {
  return FUNCTION_TYPES.has(node.type);
}

/** The call of the first decorator of `node` named one of `names`, such as `@RelayBusTransaction()`. */
export function findDecorator(
  node: object,
  names: ReadonlySet<string>,
): ESTree.CallExpression | undefined {
  for (const { expression } of (node as Decorated).decorators ?? []) {
    if (
      expression.type === 'CallExpression' &&
      expression.callee.type === 'Identifier' &&
      names.has(expression.callee.name)
    ) {
      return expression;
    }
  }
  return undefined;
}

/** Whether an argument of `call` is an object literal with `transactional: true`. */
export function hasTransactionalOption(call: ESTree.CallExpression): boolean {
  return call.arguments.some(
    (argument) =>
      argument.type === 'ObjectExpression' &&
      argument.properties.some(
        (property) =>
          property.type === 'Property' &&
          definitionName(property) === 'transactional' &&
          property.value.type === 'Literal' &&
          property.value.value === true,
      ),
  );
}

/**
 * The name of a property key, class member or import: `name`, `#name` for a private member, the
 * text of a string literal (`'name'`); undefined for anything computed at runtime.
 */
export function keyName(key: ESTree.Node): string | undefined {
  if (key.type === 'Identifier') {
    return key.name;
  }
  if (key.type === 'PrivateIdentifier') {
    return `#${key.name}`;
  }
  return key.type === 'Literal' && typeof key.value === 'string' ? key.value : undefined;
}

/** The member a member expression reads: `get` for `a.get` and `a['get']`, undefined for `a[name]`. */
export function memberName(member: ESTree.MemberExpression): string | undefined {
  return staticName(member.computed, member.property);
}

/** The name of a class member or object property: `place` for `place()` and `['place']()`, undefined for `[name]()`. */
export function definitionName(
  definition: ESTree.MethodDefinition | ESTree.Property,
): string | undefined {
  return staticName(definition.computed, definition.key);
}

function staticName(computed: boolean, key: ESTree.Node): string | undefined {
  return computed && key.type !== 'Literal' ? undefined : keyName(key);
}

/** The last name of an expression: `bus` for `bus`, `this.bus` and `app.services.bus`. */
export function lastName(node: ESTree.Node): string | undefined {
  if (node.type === 'Identifier') {
    return node.name;
  }
  return node.type === 'MemberExpression' ? memberName(node) : undefined;
}
