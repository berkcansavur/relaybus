import { fileURLToPath } from 'node:url';
import ts from 'typescript';
import { describe, expect, it } from 'vitest';

const packages = [
  'core',
  'eslint-plugin',
  'kafka',
  'mongodb',
  'nestjs',
  'postgres',
  'rabbitmq',
  'testing',
];
const entries: Record<string, string> = {
  ...Object.fromEntries(
    packages.map((name) => [
      `@relaybus/${name}`,
      fileURLToPath(new URL(`../../${name}/src/index.ts`, import.meta.url)),
    ]),
  ),
  '@relaybus/core/internal': fileURLToPath(new URL('../src/internal/index.ts', import.meta.url)),
};

/** Every exported name of an entry point with what it is: a value, a type, or both. */
function exportedSurface(program: ts.Program, file: string): string[] {
  const checker = program.getTypeChecker();
  const source = program.getSourceFile(file)!;
  const module = checker.getSymbolAtLocation(source)!;
  return checker
    .getExportsOfModule(module)
    .map((symbol) => {
      const target =
        symbol.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(symbol) : symbol;
      const value = (target.flags & ts.SymbolFlags.Value) !== 0;
      const type = (target.flags & ts.SymbolFlags.Type) !== 0;
      return `${symbol.name}: ${value && type ? 'value+type' : value ? 'value' : 'type'}`;
    })
    .sort();
}

// The exported surface of every published entry point. A change here is a change of the
// public API: update the snapshot only on purpose (`vitest -u`) and say so in the commit.
describe('public API', () => {
  const program = ts.createProgram(Object.values(entries), {
    target: ts.ScriptTarget.ES2023,
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    experimentalDecorators: true,
    skipLibCheck: true,
    noEmit: true,
    paths: Object.fromEntries(Object.entries(entries).map(([name, file]) => [name, [file]])),
  });

  it.each(Object.entries(entries))('%s keeps its exported surface', (_name, file) => {
    expect(exportedSurface(program, file)).toMatchSnapshot();
  });
});
