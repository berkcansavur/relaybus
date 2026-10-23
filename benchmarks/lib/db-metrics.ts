// Turns raw database-side counters of one measured window into per-unit metrics. A unit is one published event
// (publish cells) or one completed delivery (dispatch and mixed cells), counted by the database itself over the same
// window as the counters, so the ratios do not depend on how the client measured its window.

export interface DbWindow {
  /** Net rows added to the events table over the window (exact counts, not lazily flushed statistics). */
  readonly eventsInserted: number;
  readonly deliveriesInserted: number;
  /** Fall in the number of deliveries that are not completed; valid for dispatch cells, where nothing is inserted. */
  readonly deliveriesCompleted: number;
  /** Raw counters: `statements`, `walBytes`, `<table>.ins|upd|del` (PostgreSQL); `insert`, `ops`, `oplogEntries` (MongoDB). */
  readonly counters: Readonly<Record<string, number>>;
  /** HOT updates of the deliveries table, PostgreSQL only. */
  readonly deliveriesHotUpdates: number | null;
  readonly dockerCpuAvgPct: number | null;
  /** Human readable top wait events (PostgreSQL). */
  readonly waits: string;
}

const round = (value: number): number => Math.round(value * 10_000) / 10_000;

/** Per-unit counter metrics (`ctr.<name>PerUnit`); every value is `null` when the window saw no unit. */
export function counterMetrics(window: DbWindow, units: number): Record<string, number | null> {
  const metrics: Record<string, number | null> = {};
  for (const [name, value] of Object.entries(window.counters)) {
    metrics[`ctr.${name}PerUnit`] = units > 0 ? round(value / units) : null;
  }
  const updates = window.counters['deliveries.upd'];
  if (window.deliveriesHotUpdates !== null && updates !== undefined && updates > 0) {
    metrics['ctr.hotUpdPct'] = round((100 * window.deliveriesHotUpdates) / updates);
  }
  return metrics;
}
