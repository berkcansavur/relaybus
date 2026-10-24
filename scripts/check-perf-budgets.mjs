#!/usr/bin/env node
// Enforces the ratchet of perf-budgets.json: compared with a base revision, no budget may grow and
// none may disappear. Usage: node scripts/check-perf-budgets.mjs [base-ref] (default origin/main).
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { URL } from 'node:url';

const base = process.argv[2] ?? 'origin/main';
const current = JSON.parse(readFileSync(new URL('../perf-budgets.json', import.meta.url), 'utf8'));

let previousText;
try {
  previousText = execFileSync('git', ['show', `${base}:perf-budgets.json`], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
  });
} catch {
  console.log(`[check-perf-budgets](main) No perf-budgets.json at ${base}, nothing to compare`);
  process.exit(0);
}
const previous = JSON.parse(previousText);

const problems = [];
for (const [id, entry] of Object.entries(previous)) {
  if (typeof entry !== 'object') {
    continue;
  }
  const now = current[id];
  if (typeof now !== 'object') {
    problems.push(`${id}: removed (budget was ${entry.budget})`);
  } else if (now.budget > entry.budget) {
    problems.push(`${id}: raised from ${entry.budget} to ${now.budget}`);
  }
}
if (problems.length > 0) {
  console.error(
    `[check-perf-budgets](main) Budgets may only be lowered | base=${base}\n  ${problems.join('\n  ')}`,
  );
  process.exit(1);
}
console.log(`[check-perf-budgets](main) No budget grew | base=${base}`);
