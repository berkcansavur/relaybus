import { RelayBusError } from '@relaybus/core';

// Running the codec contract under any test framework: the cases as plain assertions, and the
// registration of one test per case through the framework's own `describe` and `it`.

/** The two functions of a test framework the codec contract registers its tests with. */
export interface TestFramework {
  readonly describe: (name: string, body: () => void) => void;
  readonly it: (name: string, body: () => Promise<void>) => void;
}

/** Thrown by a codec contract assertion: one line per mismatch between the codec and the case. */
export class CodecContractError extends RelayBusError {
  override name = 'CodecContractError';
  override readonly code: string = 'RELAYBUS_TESTING_CODEC_CONTRACT';
  readonly mismatches: readonly string[];

  constructor(message: string, mismatches: readonly string[] = []) {
    super(message);
    this.mismatches = mismatches;
  }
}

/** Throws a {@link CodecContractError} when `mismatches` is not empty. */
export function assertNoMismatches(caseName: string, mismatches: readonly string[]): void {
  if (mismatches.length > 0) {
    const lines = mismatches.map((line) => `- ${line}`).join('\n');
    throw new CodecContractError(`Codec case '${caseName}' does not match:\n${lines}`, mismatches);
  }
}

/** Registers one test per case under one `describe`, each failing with the case's mismatches. */
export function registerCases<TCase extends { readonly name: string }>(
  framework: TestFramework,
  {
    title,
    cases,
    check,
  }: {
    readonly title: string;
    readonly cases: readonly TCase[];
    readonly check: (testCase: TCase) => Promise<void>;
  },
): void {
  framework.describe(title, () => {
    for (const testCase of cases) {
      framework.it(testCase.name, () => check(testCase));
    }
  });
}
