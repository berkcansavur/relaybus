/** The claim a lane lease token names, as written in the token. */
export interface LaneLeaseTokenParts {
  readonly lane: number;
  /** The lane's epoch, as decimal text: PostgreSQL epochs exceed the safe integers. */
  readonly epoch: string;
  /** The claim generation of the delivery. */
  readonly claims: number;
}

/** How many digits each part of a token may have, and how many lanes there are. */
export interface LaneLeaseTokenLimits {
  readonly laneCount: number;
  readonly epochDigits: number;
  readonly claimsDigits: number;
}

/** Largest number of digits of a lane number. */
const LANE_DIGITS = 3;

/**
 * The fencing token of a claim in a lane-leased store: `<lane>:<epoch>:<claims>`. Every state
 * change checks the lane epoch and the delivery's claim generation it names. A store creates one
 * format with its own digit limits and parses only the tokens it can have issued.
 */
export class LaneLeaseTokenFormat {
  readonly #pattern: RegExp;
  readonly #laneCount: number;

  constructor(limits: LaneLeaseTokenLimits) {
    this.#laneCount = limits.laneCount;
    this.#pattern = new RegExp(
      `^(\\d{1,${LANE_DIGITS}}):(\\d{1,${limits.epochDigits}}):(\\d{1,${limits.claimsDigits}})$`,
    );
  }

  format(
    lane: number,
    { epoch, claims }: { readonly epoch: string | number; readonly claims: number },
  ): string {
    return `${lane}:${epoch}:${claims}`;
  }

  /** The parts of `token`; undefined when the store cannot have issued it. */
  parse(token: string): LaneLeaseTokenParts | undefined {
    const match = this.#pattern.exec(token);
    if (match === null || Number(match[1]) >= this.#laneCount) {
      return undefined;
    }
    return { lane: Number(match[1]), epoch: match[2]!, claims: Number(match[3]) };
  }
}
