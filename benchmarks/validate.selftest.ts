// Self-test of the exactness validator: node benchmarks/validate.selftest.ts
import assert from 'node:assert/strict';
import { validateRun } from './lib/validate.ts';

const events = [
  { w: 0, n: 0, key: 'k', sequence: 1 },
  { w: 0, n: 1, key: 'k', sequence: 2 },
  { w: 0, n: 2, key: 'k', sequence: 3 },
  { w: 1, n: 0, key: null, sequence: null },
];
const code = (n: number, h = 1): number => n * 16 + h;
const all: Array<[number, number, number]> = [[0, code(0), 1], [0, code(1), 2], [0, code(2), 3], [1, code(0), 4]];
const run = (handled: Array<[number, number, number]>, open: any[] = [], extra: Record<string, unknown> = {}) =>
  validateRun({ handlers: 1, handled, data: { events, open }, drained: true, leaseLost: 0, ...extra } as any);

assert.equal(run(all).valid, true);
assert.equal(run(all.slice(0, 3)).lost, 1); // one delivery vanished
assert.equal(run(all.slice(0, 3)).valid, false);
assert.equal(run(all.slice(0, 3), [{ w: 1, n: 0, handler: 'h1', status: 'pending' }]).lost, 0); // still open: not lost, but not drained
assert.equal(run(all.slice(0, 3), [{ w: 1, n: 0, handler: 'h1', status: 'pending' }]).valid, false);
assert.equal(run([...all, [1, code(0), 5]]).duplicates, 1); // duplicate without lease loss is invalid
assert.equal(run([...all, [1, code(0), 5]]).valid, false);
assert.equal(run([...all, [1, code(0), 5]], [], { leaseLost: 1 }).valid, true); // documented redelivery
assert.equal(run([...all.slice(0, 3), [1, code(0), 4], [1, code(0), 4]]).lost, 0);
assert.equal(run([[0, code(1), 1], [0, code(0), 2], [0, code(2), 3], [1, code(0), 4]]).orderViolations > 0, true); // 2 before 1
assert.equal(run([[0, code(0), 1], [0, code(2), 2], [1, code(0), 4]]).orderViolations > 0, true); // gap: 3 before 2
assert.equal(run([...all, [9, code(0), 6]]).unexpected, 1);
assert.equal(run(all, [{ w: 1, n: 0, handler: 'h1', status: 'dead' }]).valid, false);
console.log('validate.selftest: ok');
