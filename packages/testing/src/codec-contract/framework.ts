// Running the codec contract under any test framework: the cases as plain assertions, and the
// registration of one test per case through the framework's own `describe` and `it`.

/** The two functions of a test framework the codec contract registers its tests with. */
export interface TestFramework {
  readonly describe: (name: string, body: () => void) => void;
  readonly it: (name: string, body: () => Promise<void>) => void;
}

/** Thrown by a codec contract assertion: one line per mismatch between the codec and the case. */
export class CodecContractError extends Error {
  readonly mismatches: readonly string[];

  constructor(caseName: string, mismatches: readonly string[]) {
    super(
      `Codec case '${caseName}' does not match:\n${mismatches.map((line) => `- ${line}`).join('\n')}`,
    );
    this.name = 'CodecContractError';
    this.mismatches = mismatches;
  }
}

/** Throws a {@link CodecContractError} when `mismatches` is not empty. */
export function assertNoMismatches(caseName: string, mismatches: readonly string[]): void {
  if (mismatches.length > 0) {
    throw new CodecContractError(caseName, mismatches);
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
