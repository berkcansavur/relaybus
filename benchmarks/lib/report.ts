// Markdown report of a comparison: gate verdict, one row per cell with ratios and confidence intervals, and a
// detail table with every metric. Pure: the caller supplies already compared data.
import type { Cell } from './cells.ts';
import type { CellComparison, GateResult, MetricComparison, Verdict } from './compare.ts';

export interface ReportInput {
  readonly mode: string;
  readonly base: { readonly ref: string; readonly sha: string };
  readonly candidate: { readonly ref: string; readonly sha: string };
  readonly fingerprint: string;
  /** `interleaved`: base and candidate measured alternately in this run. `baseline`: base loaded from storage. */
  readonly baseSource: 'interleaved' | 'baseline';
  readonly comparisons: readonly CellComparison[];
  readonly cells: readonly Cell[];
  readonly gate: GateResult;
  readonly notes: readonly string[];
  readonly elapsedMinutes: number;
}

const format = (value: number, digits = 3): string =>
  Number.isFinite(value) ? value.toFixed(digits) : '-';
const quantity = (value: number): string =>
  Number.isFinite(value) ? (Math.abs(value) >= 100 ? value.toFixed(0) : value.toFixed(2)) : '-';

function ratioText(metric: MetricComparison | undefined): string {
  if (metric === undefined || !Number.isFinite(metric.ratio)) return '-';
  const interval =
    metric.low === null || metric.high === null
      ? ''
      : ` [${format(metric.low, 2)}, ${format(metric.high, 2)}]`;
  return `${format(metric.ratio)}${interval} ${verdictText(metric.verdict)}`;
}

function verdictText(verdict: Verdict): string {
  return verdict === 'unchanged' ? '(no change)' : verdict === 'n/a' ? '' : `(${verdict})`;
}

function primary(
  comparison: CellComparison,
  names: readonly string[],
): MetricComparison | undefined {
  return names
    .map((name) => comparison.metrics.find((metric) => metric.metric === name))
    .find((metric) => metric !== undefined);
}

function counterSummary(comparison: CellComparison): string {
  const counters = comparison.metrics.filter(
    (metric) => metric.definition.class === 'counter' && metric.verdict !== 'n/a',
  );
  const moved = counters.filter((metric) => metric.verdict !== 'unchanged');
  if (counters.length === 0) return '-';
  if (moved.length === 0) return `${counters.length} counters within 3 %`;
  return moved
    .map(
      (metric) =>
        `${metric.definition.label} ${Number.isFinite(metric.ratio) ? `${format(metric.ratio, 2)}x` : 'from 0'} ${verdictText(metric.verdict)}`,
    )
    .join('<br>');
}

function validationText(comparison: CellComparison): string {
  const total = comparison.baseSamples + comparison.candidateSamples;
  return comparison.validationFailures.length === 0
    ? `valid ${total}/${total}`
    : `FAILED: ${comparison.validationFailures.join(' | ')}`;
}

/** Renders the report. */
export function renderReport(input: ReportInput): string {
  const lines: string[] = [];
  const titles = new Map(input.cells.map((cell) => [cell.id, cell.title]));
  lines.push(`# relaybus benchmark: ${input.mode}`, '');
  lines.push(`- candidate: \`${input.candidate.ref}\` (${input.candidate.sha.slice(0, 10)})`);
  lines.push(
    `- base: \`${input.base.ref}\` (${input.base.sha.slice(0, 10)}), ${input.baseSource === 'interleaved' ? 'measured interleaved with the candidate in this run' : 'loaded from the stored baseline'}`,
  );
  lines.push(
    `- machine fingerprint: \`${input.fingerprint}\`, run time ${input.elapsedMinutes.toFixed(1)} min`,
  );
  lines.push(`- gate: **${input.gate.passed ? 'PASS' : 'FAIL'}**`);
  for (const reason of input.gate.reasons) lines.push(`  - ${reason}`);
  for (const waived of input.gate.accepted) lines.push(`  - accepted: ${waived}`);
  lines.push('');
  if (input.notes.length > 0) {
    lines.push('## Notes', '', ...input.notes.map((note) => `- ${note}`), '');
  }
  lines.push(
    '## Summary',
    '',
    'Ratios are candidate / base (median based). Throughput: higher is better; latency: lower is better. Intervals are 95 % bootstrap intervals; with fewer than 3 samples a ratio is shown without a verdict. Counters are database-side counters per event (publish) or per delivery (dispatch) and gate at 3 %.',
    '',
    '| cell | samples (base/candidate) | throughput ratio | p99 latency ratio | counters | validation | notes |',
    '|---|---|---|---|---|---|---|',
  );
  for (const comparison of input.comparisons) {
    const flags = [
      comparison.pairing === 'paired' ? 'paired' : 'unpaired',
      ...(comparison.discardedBusy > 0
        ? [`${comparison.discardedBusy} busy samples discarded`]
        : []),
      ...(comparison.noisy ? ['noisy: too few clean samples'] : []),
    ].join(', ');
    lines.push(
      `| ${titles.get(comparison.cellId) ?? comparison.cellId} (\`${comparison.cellId}\`) | ${comparison.baseSamples}/${comparison.candidateSamples} | ${ratioText(primary(comparison, ['deliveriesPerSec', 'eventsPerSec', 'achievedPerSec']))} | ${ratioText(primary(comparison, ['txP99Ms', 'e2eP99Ms']))} | ${counterSummary(comparison)} | ${validationText(comparison)} | ${flags} |`,
    );
  }
  lines.push('', '## Details', '');
  for (const comparison of input.comparisons) {
    lines.push(
      `### ${titles.get(comparison.cellId) ?? comparison.cellId}`,
      '',
      '| metric | base | candidate | ratio | verdict |',
      '|---|---|---|---|---|',
    );
    for (const metric of comparison.metrics) {
      lines.push(
        `| ${metric.definition.label} | ${quantity(metric.baseMedian)} | ${quantity(metric.candidateMedian)} | ${ratioText(metric).replace(/ \(.*\)$/, '')} | ${metric.verdict === 'n/a' ? '' : metric.verdict} |`,
      );
    }
    lines.push('');
  }
  return `${lines.join('\n')}\n`;
}
