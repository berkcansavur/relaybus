/**
 * R18 of the tag monorepo coding standards: a function takes at most two positional parameters;
 * more travel in one typed input object. Constructors are exempt. A public port or API signature
 * that must keep its shape (an EventStore, Transport or executor method, a published function)
 * is exempted on its line with a reason.
 */
const MAXIMUM = 2;

export default {
  meta: {
    type: 'suggestion',
    docs: { description: 'At most two positional parameters; pass more in an input object' },
    schema: [],
    messages: {
      tooMany:
        '{{count}} parameters: take at most two and pass the rest in one typed input object (R18).',
    },
  },
  create(context) {
    const check = (node) => {
      if (node.params.length <= MAXIMUM) return;
      if (node.parent?.type === 'MethodDefinition' && node.parent.kind === 'constructor') return;
      context.report({ node, messageId: 'tooMany', data: { count: node.params.length } });
    };
    return {
      FunctionDeclaration: check,
      FunctionExpression: check,
      ArrowFunctionExpression: check,
    };
  },
};
