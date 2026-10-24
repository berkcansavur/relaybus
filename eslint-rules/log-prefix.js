/**
 * Every log line starts with `[ClassName](methodName) message`, naming where the call physically is:
 * the enclosing class and method (`#` dropped, `constructor` for constructors), or, outside a class,
 * the module's main class (its first exported class, else its first exported function) and the
 * enclosing named function. Closures belong to the method or function that contains them. Log
 * calls are calls of `log`, `info`, `warn`, `error` or `debug` on an object whose name mentions
 * `logger` or is `console`; their message must be a string or template literal that starts with a
 * literal prefix.
 */

const LOG_METHODS = new Set(['log', 'info', 'warn', 'error', 'debug']);
const PREFIX = /^\[([A-Za-z0-9_$]+)\]\(([A-Za-z0-9_$]+)\) /;

function memberName(node) {
  if (node.type === 'PrivateIdentifier') return node.name;
  if (node.type === 'Identifier') return node.name;
  if (node.type === 'Literal') return String(node.value);
  return undefined;
}

/** Text of a callee object, e.g. `this.#settings.logger`, to recognize loggers. */
function objectText(node) {
  if (node.type === 'Identifier') return node.name;
  if (node.type === 'ThisExpression') return 'this';
  if (node.type === 'MemberExpression') {
    return `${objectText(node.object)}.${memberName(node.property) ?? '?'}`;
  }
  if (node.type === 'ChainExpression') return objectText(node.expression);
  return '';
}

function isLogCall(node) {
  const callee = node.callee.type === 'ChainExpression' ? node.callee.expression : node.callee;
  if (callee.type !== 'MemberExpression' || !LOG_METHODS.has(memberName(callee.property))) {
    return false;
  }
  const text = objectText(callee.object);
  return /logger/i.test(text) || text === 'console';
}

/** The literal start of the message, or undefined when the message is not a literal. */
function literalStart(argument) {
  if (argument === undefined) return undefined;
  if (argument.type === 'Literal' && typeof argument.value === 'string') return argument.value;
  if (argument.type === 'TemplateLiteral') return argument.quasis[0].value.cooked;
  return undefined;
}

function mainName(program) {
  const exported = program.body
    .filter((statement) => statement.type === 'ExportNamedDeclaration' && statement.declaration)
    .map((statement) => statement.declaration);
  const declarations = [
    ...exported,
    ...program.body.filter(
      (statement) =>
        statement.type === 'ClassDeclaration' || statement.type === 'FunctionDeclaration',
    ),
  ];
  const named = (type) =>
    declarations.find((declaration) => declaration.type === type && declaration.id);
  return (named('ClassDeclaration') ?? named('FunctionDeclaration'))?.id.name;
}

/** The class and method (or function) the node is physically written in. */
function location(node, program) {
  let method;
  for (let current = node.parent; current; current = current.parent) {
    if (method === undefined && current.type === 'MethodDefinition') {
      method = current.kind === 'constructor' ? 'constructor' : memberName(current.key);
    }
    if (method === undefined && current.type === 'PropertyDefinition') {
      method = memberName(current.key);
    }
    if (method === undefined && current.type === 'FunctionDeclaration' && current.id) {
      method = current.id.name;
    }
    if (current.type === 'ClassDeclaration' || current.type === 'ClassExpression') {
      return { className: current.id?.name ?? mainName(program), method };
    }
  }
  return { className: mainName(program), method };
}

export default {
  meta: {
    type: 'problem',
    docs: { description: 'Log lines start with [ClassName](methodName) of where they are written' },
    schema: [],
    messages: {
      missing:
        'Log message must start with a literal `[{{className}}]({{method}}) ` prefix naming where it is written.',
      mismatch:
        'Log prefix `{{found}}` must be `[{{className}}]({{method}})`: the class and method it is written in.',
    },
  },
  create(context) {
    const program = context.sourceCode.ast;
    return {
      CallExpression(node) {
        if (!isLogCall(node)) return;
        const { className, method } = location(node, program);
        const start = literalStart(node.arguments[0]);
        const match = start === undefined ? null : PREFIX.exec(start);
        const data = { className, method: method ?? '?' };
        if (match === null) {
          context.report({ node: node.arguments[0] ?? node, messageId: 'missing', data });
          return;
        }
        if (match[1] !== className || match[2] !== method) {
          context.report({
            node: node.arguments[0],
            messageId: 'mismatch',
            data: { ...data, found: `[${match[1]}](${match[2]})` },
          });
        }
      },
    };
  },
};
