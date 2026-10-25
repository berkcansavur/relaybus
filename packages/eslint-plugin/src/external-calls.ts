import type { Rule } from 'eslint';
import type * as ESTree from 'estree';
import { lastName, memberName } from './ast.js';

/** HTTP client modules, by import source. */
type ClientModule = 'axios' | 'got' | 'http' | 'undici';

const MODULES: Readonly<Record<string, ClientModule>> = {
  axios: 'axios',
  got: 'got',
  http: 'http',
  'node:http': 'http',
  https: 'http',
  'node:https': 'http',
  undici: 'undici',
};

/** Members of a module object that send a request; `undefined`: every member does. */
const REQUEST_MEMBERS: Readonly<Record<ClientModule, ReadonlySet<string> | undefined>> = {
  axios: undefined,
  got: undefined,
  http: new Set(['request', 'get']),
  undici: new Set(['request', 'fetch', 'stream', 'pipeline']),
};

/** `HttpService` of `@nestjs/axios`, injected as `httpService` (or `http`), and its `axiosRef`. */
const HTTP_SERVICE_NAME = /^(?:http|httpService|axiosRef)$/i;

/**
 * Recognises calls to other services: `fetch`, `axios` and `got` (called or through a method),
 * `http.request` / `https.get` of Node.js, `undici.request` and friends (imported or required),
 * and `HttpService` of `@nestjs/axios`.
 */
export class ExternalCallDetector {
  /** Local names of imported HTTP client modules. */
  readonly #modules = new Map<string, ClientModule>([
    ['axios', 'axios'],
    ['got', 'got'],
  ]);
  /** Local names of imported request functions, such as `request` from `undici`. */
  readonly #functions = new Set<string>(['fetch']);

  /** Records `import x from 'axios'`, `import * as https from 'node:https'`, `import { request } from 'undici'`. */
  recordImport(declaration: ESTree.ImportDeclaration): void {
    const module = MODULES[String(declaration.source.value)];
    if (module === undefined) {
      return;
    }
    for (const specifier of declaration.specifiers) {
      if (specifier.type !== 'ImportSpecifier') {
        this.#modules.set(specifier.local.name, module);
      } else if (REQUEST_MEMBERS[module]?.has(importedName(specifier)) === true) {
        this.#functions.add(specifier.local.name);
      }
    }
  }

  /** Records `const axios = require('axios')`. */
  recordRequire(declarator: ESTree.VariableDeclarator): void {
    const { id, init } = declarator;
    if (
      id.type === 'Identifier' &&
      init?.type === 'CallExpression' &&
      lastName(init.callee) === 'require' &&
      init.arguments[0]?.type === 'Literal'
    ) {
      const module = MODULES[String(init.arguments[0].value)];
      if (module !== undefined) {
        this.#modules.set(id.name, module);
      }
    }
  }

  /** Whether `call` sends a request to another service. */
  isExternalCall(call: Rule.Node & ESTree.CallExpression): boolean {
    const { callee } = call;
    if (callee.type === 'Identifier') {
      const module = this.#modules.get(callee.name);
      return this.#functions.has(callee.name) || module === 'axios' || module === 'got';
    }
    return callee.type === 'MemberExpression' && this.#isClientMember(callee);
  }

  /** `axios.post`, `https.request`, `this.httpService.get`, `httpService.axiosRef.get`. */
  #isClientMember(callee: ESTree.MemberExpression): boolean {
    if (HTTP_SERVICE_NAME.test(lastName(callee.object) ?? '')) {
      return true;
    }
    const module =
      callee.object.type === 'Identifier' ? this.#modules.get(callee.object.name) : undefined;
    if (module === undefined) {
      return false;
    }
    return REQUEST_MEMBERS[module]?.has(memberName(callee) ?? '') ?? true;
  }
}

/** `request` for `import { request }` and `import { 'request' as send }`. */
function importedName(specifier: ESTree.ImportSpecifier): string {
  const { imported } = specifier;
  return imported.type === 'Identifier' ? imported.name : String(imported.value);
}
