// The record one measured run leaves behind. It is what baselines store and what the comparison consumes.

export type SideName = 'base' | 'candidate';

/** Machine load observed around one sample. */
export interface LoadReading {
  /** One minute load average when the sample started / ended. */
  readonly loadavgStart: number;
  readonly loadavgEnd: number;
  /** Wall time of a fixed CPU spin right before the sample; a busy machine makes it slower. */
  readonly calibrationMs: number;
  /** Host CPU utilisation over the sample (includes the benchmark itself). */
  readonly hostCpuPct: number;
  /** Average CPU of the database container reported by `docker stats`. */
  readonly dbCpuAvgPct: number | null;
}

export interface Validation {
  readonly valid: boolean;
  readonly reasons: readonly string[];
}

export interface Sample {
  readonly cellId: string;
  readonly side: SideName;
  /** Index of the pair this sample belongs to (base and candidate of one pair ran back to back). */
  readonly pair: number;
  readonly startedAt: string;
  /** Metric key to value; `null` when the quantity does not exist for this code (for example a table that is new). */
  readonly metrics: Readonly<Record<string, number | null>>;
  readonly validation: Validation;
  readonly load: LoadReading;
  /** Free text, for example the top wait events. */
  readonly notes: readonly string[];
}
