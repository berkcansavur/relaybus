#!/usr/bin/env node
// Locks the public API of every published package with API Extractor.
//
// One report per entry point in `exports` (types only), written to
// packages/<name>/etc/<entry>.api.md and committed. Any change to an exported
// declaration changes a report, so it shows up in the pull request diff.
//
// Usage: node scripts/api-report.mjs [--check]
// Run `pnpm build` first: the reports are read from the bundled declarations in dist/.
// Without --check the reports are rewritten and reports of removed entry points deleted.
// With --check nothing in etc/ is written: the script exits 1 when a committed report
// differs from the current API, is missing, or belongs to an entry point that no longer exists.
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { Extractor, ExtractorConfig } from '@microsoft/api-extractor';
import ts from 'typescript';

const root = resolve(import.meta.dirname, '..');
const check = process.argv.includes('--check');

/** Entry points of one package: report name and declaration file, from its `exports` map. */
function entryPoints(packageDir, manifest) {
  return Object.entries(manifest.exports)
    .filter(([subpath]) => subpath !== './package.json')
    .map(([subpath, target]) => ({
      subpath,
      name: subpath === '.' ? 'index' : subpath.slice(2).replaceAll('/', '-'),
      types: join(packageDir, target.import?.types ?? target.types),
      runtime: join(packageDir, target.import?.default ?? target.default),
    }));
}

/** True when the export is reachable at runtime: no type-only export or import on its alias chain. */
function isValueExport(checker, symbol) {
  let current = symbol;
  while (current.flags & ts.SymbolFlags.Alias) {
    if (current.declarations?.some((node) => ts.isTypeOnlyImportOrExportDeclaration(node))) {
      return false;
    }
    current = checker.getImmediateAliasedSymbol(current) ?? current;
    if (current === symbol) break;
  }
  return (current.flags & ts.SymbolFlags.Value) !== 0;
}

/**
 * Names a module exports only through `export type * from` (directly or through nested
 * `export * from`). The checker hands back the original symbols of star exports, so the
 * type-only edge is invisible to isValueExport. Explicit exports of a module win over stars.
 */
function typeOnlyStarNames(checker, moduleSymbol, seen = new Set([moduleSymbol])) {
  const explicit = new Set(
    [...(moduleSymbol.exports?.keys() ?? [])].filter((name) => name !== '__export'),
  );
  const names = new Set();
  for (const declaration of moduleSymbol.declarations ?? []) {
    const statements = ts.isSourceFile(declaration)
      ? declaration.statements
      : (declaration.body?.statements ?? []);
    for (const statement of statements) {
      if (
        !ts.isExportDeclaration(statement) ||
        statement.exportClause ||
        !statement.moduleSpecifier
      )
        continue;
      const target = checker.getSymbolAtLocation(statement.moduleSpecifier);
      if (!target || seen.has(target)) continue;
      const reached = statement.isTypeOnly
        ? checker.getExportsOfModule(target).map((symbol) => symbol.name)
        : typeOnlyStarNames(checker, target, new Set([...seen, target]));
      for (const name of reached) if (!explicit.has(name)) names.add(name);
    }
  }
  return names;
}

/**
 * Section appended to every report: the entry point's subpath and each export marked as
 * value or type. API Extractor prints a class the same way whether it is exported as a
 * value or type-only, and two subpaths can map to one report file name.
 */
function exportInventory(packageName, entry) {
  const program = ts.createProgram([entry.types, entry.runtime], {
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    allowJs: true,
    skipLibCheck: true,
    noEmit: true,
  });
  const checker = program.getTypeChecker();
  const moduleSymbol = checker.getSymbolAtLocation(program.getSourceFile(entry.types));
  const typeOnly = typeOnlyStarNames(checker, moduleSymbol);
  // The declaration bundle can still list a name the bundled JavaScript dropped (tsup turns
  // `export type * from` into `export * from` in declarations), so a value must also exist there.
  const runtimeModule = checker.getSymbolAtLocation(program.getSourceFile(entry.runtime));
  const runtimeNames = new Set(
    runtimeModule ? checker.getExportsOfModule(runtimeModule).map((symbol) => symbol.name) : [],
  );
  const lines = checker
    .getExportsOfModule(moduleSymbol)
    .map((symbol) => {
      const value =
        runtimeNames.has(symbol.name) &&
        !typeOnly.has(symbol.name) &&
        isValueExport(checker, symbol);
      return `${value ? 'value' : 'type '} ${symbol.name}`;
    })
    .sort();
  const specifier =
    entry.subpath === '.' ? packageName : `${packageName}/${entry.subpath.slice(2)}`;
  return `## Exports of "${specifier}"\n\n\`\`\`text\n${lines.join('\n')}\n\`\`\`\n`;
}

/** Reads a file, or returns null when it does not exist (no separate existence check, so no race). */
function readIfPresent(file) {
  try {
    return readFileSync(file, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}

/** Report files committed in etc/, or an empty list when the folder does not exist. */
function committedReports(reportDir) {
  try {
    return readdirSync(reportDir).filter((file) => file.endsWith('.api.md'));
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }
}

/** Runs API Extractor for one entry point and returns the generated report text, or null on errors. */
function generateReport(packageDir, packageJsonFullPath, manifest, entry) {
  const config = ExtractorConfig.prepare({
    configObject: {
      projectFolder: packageDir,
      mainEntryPointFilePath: entry.types,
      // Analyses the emitted declarations only, so a minimal program is enough and
      // does not depend on the TypeScript version API Extractor bundles.
      compiler: {
        overrideTsconfig: {
          compilerOptions: {
            lib: ['es2023'],
            types: ['node'],
            module: 'nodenext',
            moduleResolution: 'nodenext',
            skipLibCheck: true,
          },
        },
      },
      // Both folders point at temp/: API Extractor only writes the report there, and this
      // script compares it with etc/ itself. API Extractor's own comparison ignores
      // whitespace changes, including inside string literal types.
      apiReport: {
        enabled: true,
        reportFileName: `${entry.name}.api.md`,
        reportFolder: '<projectFolder>/temp/',
        reportTempFolder: '<projectFolder>/temp/',
        // Types that public signatures use without exporting them are part of the API too.
        includeForgottenExports: true,
      },
      docModel: { enabled: false },
      dtsRollup: { enabled: false },
      tsdocMetadata: { enabled: false },
      newlineKind: 'lf',
      messages: {
        compilerMessageReporting: { default: { logLevel: 'warning' } },
        extractorMessageReporting: {
          default: { logLevel: 'warning', addToApiReportFile: true },
          // Release tags (@public, @beta, @internal) are not used yet: every export is public.
          'ae-missing-release-tag': { logLevel: 'none', addToApiReportFile: false },
          // Raised for .cts type files of dependencies (@eslint/plugin-kit); it names an
          // absolute path, which would make the report differ on every machine.
          'ae-wrong-input-file-type': { logLevel: 'none', addToApiReportFile: false },
        },
        tsdocMessageReporting: { default: { logLevel: 'none' } },
      },
    },
    configObjectFullPath: join(packageDir, 'api-extractor.json'),
    packageJsonFullPath,
  });
  const result = Extractor.invoke(config, { localBuild: false, showVerboseMessages: false });
  if (!result.succeeded) return null;
  return (
    readFileSync(join(packageDir, 'temp', `${entry.name}.api.md`), 'utf8') +
    '\n' +
    exportInventory(manifest.name, entry)
  );
}

const problems = [];
const packageDirs = readdirSync(join(root, 'packages'), { withFileTypes: true })
  .filter((dirent) => dirent.isDirectory())
  .map((dirent) => dirent.name)
  .sort();
for (const dirName of packageDirs) {
  const packageDir = join(root, 'packages', dirName);
  const reportDir = join(packageDir, 'etc');
  const packageJsonFullPath = join(packageDir, 'package.json');
  const manifest = JSON.parse(readFileSync(packageJsonFullPath, 'utf8'));
  const entries = entryPoints(packageDir, manifest);
  const names = entries.map((entry) => entry.name);
  const duplicates = names.filter((name, index) => names.indexOf(name) !== index);
  if (duplicates.length > 0) {
    problems.push(`${manifest.name}: entry points share a report name: ${duplicates.join(', ')}`);
    continue;
  }

  for (const entry of entries) {
    const label = `${manifest.name} (${entry.name})`;
    const generated = generateReport(packageDir, packageJsonFullPath, manifest, entry);
    if (generated === null) {
      problems.push(`${label}: API Extractor reported errors`);
      continue;
    }
    const reportFile = join(reportDir, `${entry.name}.api.md`);
    const committed = readIfPresent(reportFile);
    if (committed?.replaceAll('\r\n', '\n') === generated) continue;
    if (check) {
      problems.push(`${label}: ${committed === null ? 'report missing' : 'report out of date'}`);
    } else {
      mkdirSync(reportDir, { recursive: true });
      writeFileSync(reportFile, generated);
    }
  }

  const expected = new Set(entries.map((entry) => `${entry.name}.api.md`));
  for (const file of committedReports(reportDir)) {
    if (expected.has(file)) continue;
    if (check) problems.push(`${manifest.name}: ${file} has no entry point in exports`);
    else rmSync(join(reportDir, file));
  }
}

if (problems.length > 0) {
  console.error(
    `[api-report](main) API reports are not up to date:\n  - ${problems.join('\n  - ')}`,
  );
  if (check)
    console.error('Run `pnpm build && pnpm api:report` and commit packages/*/etc/*.api.md.');
  process.exit(1);
}
