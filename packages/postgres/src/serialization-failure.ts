/** PostgreSQL SQLSTATE `serialization_failure`: a concurrent transaction conflicts, running this one again can succeed. */
const SERIALIZATION_FAILURE = '40001';

/**
 * Prisma's code for "write conflict or deadlock" of an interactive transaction (`P2034`): raised for
 * `40001` and, since running it again is just as safe, for a deadlock (`40P01`).
 */
const PRISMA_WRITE_CONFLICT = 'P2034';

const CONFLICT_CODES: ReadonlySet<unknown> = new Set([
  SERIALIZATION_FAILURE,
  PRISMA_WRITE_CONFLICT,
]);

/** Wrappers nested deeper than this are not searched; real ones are two or three levels deep. */
const MAXIMUM_DEPTH = 6;

/** The properties that hold the error a library wrapped: standard `cause`, TypeORM `driverError`. */
const WRAPPED_ERROR_KEYS = ['cause', 'driverError', 'originalError'] as const;

type ErrorShape = {
  readonly code?: unknown;
  readonly originalCode?: unknown;
  readonly driverAdapterError?: unknown;
  readonly meta?: unknown;
  readonly [key: string]: unknown;
};

/**
 * True when `error` is, or wraps, a PostgreSQL serialization failure (SQLSTATE `40001`), whichever
 * library raised it: `pg` (Kysely, Knex and the `pg` executor pass its error through), TypeORM's
 * `QueryFailedError` (`driverError`), Drizzle's wrapper (`cause`), Prisma (`P2034`, or a raw-query
 * error whose `meta.code` is the SQLSTATE, and driver-adapter errors that nest the original error).
 */
export function isPostgresSerializationFailure(error: unknown, depth = 0): boolean {
  if (typeof error !== 'object' || error === null || depth > MAXIMUM_DEPTH) {
    return false;
  }
  const shape = error as ErrorShape;
  if (CONFLICT_CODES.has(shape.code) || shape.originalCode === SERIALIZATION_FAILURE) {
    return true;
  }
  const meta = shape.meta as ErrorShape | undefined;
  if (
    isPostgresSerializationFailure(meta, depth + 1) ||
    isPostgresSerializationFailure(meta?.driverAdapterError, depth + 1)
  ) {
    return true;
  }
  return WRAPPED_ERROR_KEYS.some((key) => isPostgresSerializationFailure(shape[key], depth + 1));
}
