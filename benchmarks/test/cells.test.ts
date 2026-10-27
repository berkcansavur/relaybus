import { describe, expect, it } from 'vitest';
import {
  filterPlan,
  fullPlan,
  planDescriptor,
  QUICK_TARGETS,
  quickPlan,
  TEMPLATE_SAFETY,
  templateEventsFor,
} from '../lib/cells.ts';
import { WORKLOAD_SEED } from '../lib/deterministic.ts';
import { fingerprintOf } from '../lib/baseline.ts';

describe('plans', () => {
  it('gives every cell a unique id and every dispatch cell an existing template of its store', () => {
    for (const plan of [quickPlan(), fullPlan()]) {
      const ids = plan.cells.map((cell) => cell.id);
      expect(new Set(ids).size).toBe(ids.length);
      for (const cell of plan.cells) {
        if (cell.kind !== 'dispatch') continue;
        const template = plan.templates.find((candidate) => candidate.name === cell.template);
        expect(template?.store).toBe(cell.store);
        expect(template?.handlers).toBe(cell.handlers);
      }
    }
  });

  it('adds the routed cells to the full run only, each with its own routed template', () => {
    expect(quickPlan().cells.some((cell) => cell.id.includes('routed'))).toBe(false);
    const plan = fullPlan();
    const routed = plan.cells.filter((cell) => cell.kind === 'dispatch' && cell.routed === true);
    expect(routed.map((cell) => cell.id)).toEqual([
      'pg-disp-routed-k1000-1i-tuned',
      'pg-disp-routed-hot10-1i-tuned',
    ]);
    for (const cell of routed) {
      if (cell.kind !== 'dispatch') continue;
      const template = plan.templates.find((candidate) => candidate.name === cell.template);
      expect(template?.routed).toBe(true);
    }
    expect(plan.templates.filter((candidate) => candidate.routed === true)).toHaveLength(2);
    expect(filterPlan(plan, ['routed']).templates.map((candidate) => candidate.name)).toEqual([
      'pg-routed-k1000',
      'pg-routed-hot10',
    ]);
  });

  it('keeps the quick gate small: fixed windows, the requested pairs, no mixed cell', () => {
    const plan = quickPlan(4);
    expect(plan.cells).toHaveLength(8);
    expect(
      plan.cells.every(
        (cell) =>
          cell.reps === 4 &&
          cell.kind !== 'mixed' &&
          (cell.kind === 'dispatch' || cell.warmupMs + cell.durationMs <= 10_000),
      ),
    ).toBe(true);
  });

  it('keeps the full run to about 20 s per cell and gives headline cells more samples', () => {
    const plan = fullPlan();
    expect(plan.cells.every((cell) => cell.warmupMs + cell.durationMs <= 24_000)).toBe(true);
    expect(plan.cells.some((cell) => cell.reps === 3)).toBe(true);
    expect(plan.cells.some((cell) => cell.reps === 1)).toBe(true);
    expect(fullPlan(2).cells.every((cell) => cell.reps === 2)).toBe(true);
  });

  it('filters cells and drops templates nobody uses', () => {
    const filtered = filterPlan(quickPlan(), ['mongo-disp']);
    expect(filtered.cells.map((cell) => cell.id)).toEqual(['mongo-disp-1i-tuned']);
    expect(filtered.templates.map((template) => template.name)).toEqual(['mongo-unkeyed']);
  });

  it('describes what is measured, not how often', () => {
    expect(fingerprintOf(planDescriptor(quickPlan(3)))).toBe(
      fingerprintOf(planDescriptor(quickPlan(5))),
    );
    expect(fingerprintOf(planDescriptor(quickPlan()))).not.toBe(
      fingerprintOf(planDescriptor(fullPlan())),
    );
  });

  it('sizes a template from the work of its cell, with a safety factor, in whole loading batches', () => {
    expect(templateEventsFor(200_000, 1)).toBe(400_000);
    expect(templateEventsFor(200_000, 5)).toBe(80_000);
    expect(templateEventsFor(1, 1)).toBe(10_000);
    expect(templateEventsFor(60_000, 1)).toBeGreaterThanOrEqual(60_000 * TEMPLATE_SAFETY);
  });

  it('gives every quick dispatch cell a fixed amount of work that its template cannot run out of', () => {
    const plan = quickPlan();
    const dispatch = plan.cells.filter((cell) => cell.kind === 'dispatch');
    expect(dispatch.map((cell) => cell.id).sort()).toEqual(Object.keys(QUICK_TARGETS).sort());
    for (const cell of dispatch) {
      const template = plan.templates.find((candidate) => candidate.name === cell.template);
      expect(cell.targetDeliveries).toBe(QUICK_TARGETS[cell.id]);
      expect((template?.events ?? 0) * cell.handlers).toBeGreaterThanOrEqual(
        (cell.targetDeliveries ?? Number.NaN) * TEMPLATE_SAFETY,
      );
    }
    expect(plan.templates.find((candidate) => candidate.name === 'pg-unkeyed-wide')?.events).toBe(
      480_000,
    );
  });

  it('leaves the full run on fixed duration windows', () => {
    expect(
      fullPlan().cells.every(
        (cell) => cell.kind !== 'dispatch' || cell.targetDeliveries === undefined,
      ),
    ).toBe(true);
  });

  it('starts a fresh baseline when the workload seed changes', () => {
    expect((planDescriptor(quickPlan()) as { seed: number }).seed).toBe(WORKLOAD_SEED);
  });
});
