import { toErrorInfo } from '@relaybus/core';

// Comparing what a codec wrote with a captured message, for the codec contract helpers.

/** How message bodies are compared: byte for byte, or as JSON regardless of key order and spacing. */
export type BodyComparison = 'bytes' | 'json';

/** Bytes as text or bytes, as captured or encoded. */
export type WireBytes = string | Uint8Array;

export function toBuffer(bytes: WireBytes): Buffer {
  return typeof bytes === 'string' ? Buffer.from(bytes, 'utf8') : Buffer.from(bytes);
}

/** JSON with object keys sorted at every level, so key order does not matter. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortKeys(JSON.parse(JSON.stringify(value) ?? 'null') as unknown));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (typeof value !== 'object' || value === null) {
    return value;
  }
  return Object.fromEntries(
    Object.keys(value)
      .sort()
      .map((name) => [name, sortKeys((value as Record<string, unknown>)[name])]),
  );
}

/** A mismatch line when the two bodies differ under `comparison`; `null` stands for no body. */
export function compareBodies(
  label: string,
  {
    expected,
    encoded,
    comparison,
  }: {
    readonly expected: WireBytes | null;
    readonly encoded: WireBytes | null;
    readonly comparison: BodyComparison;
  },
): string[] {
  const text = (bytes: WireBytes | null): string =>
    bytes === null ? 'null' : toBuffer(bytes).toString('utf8');
  const same =
    expected === null || encoded === null
      ? expected === encoded
      : comparison === 'json'
        ? canonicalJson(JSON.parse(text(expected))) === canonicalJson(JSON.parse(text(encoded)))
        : toBuffer(expected).equals(toBuffer(encoded));
  return same ? [] : [`${label}: expected ${text(expected)}, encoded ${text(encoded)}`];
}

/** A mismatch line when the two objects differ as canonical JSON, leaving out `ignored` names. */
export function compareObjects(
  label: string,
  {
    expected,
    encoded,
    ignored,
  }: {
    readonly expected: Readonly<Record<string, unknown>>;
    readonly encoded: Readonly<Record<string, unknown>>;
    readonly ignored: readonly string[];
  },
): string[] {
  const kept = (object: Readonly<Record<string, unknown>>): string =>
    canonicalJson(
      Object.fromEntries(Object.entries(object).filter(([name]) => !ignored.includes(name))),
    );
  const [left, right] = [kept(expected), kept(encoded)];
  return left === right ? [] : [`${label}: expected ${left}, encoded ${right}`];
}

/** A mismatch line when the decoded event differs from the expected one. */
export function compareDecoded(expected: unknown, decoded: unknown): string[] {
  const [left, right] = [canonicalJson(expected), canonicalJson(decoded)];
  return left === right ? [] : [`decoded: expected ${left}, decoded ${right}`];
}

/** The result of a step, or why it failed. */
export type Attempt<TResult> = { readonly result?: TResult; readonly mismatches: string[] };

/** Runs `step`, turning what it throws into a mismatch line. */
export async function attempt<TResult>(
  label: string,
  step: () => TResult | Promise<TResult>,
): Promise<Attempt<TResult>> {
  try {
    return { result: await step(), mismatches: [] };
  } catch (error) {
    return { mismatches: [`${label} threw: ${toErrorInfo(error).message}`] };
  }
}
