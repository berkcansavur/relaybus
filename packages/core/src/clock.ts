/** Source of the current time. Replace it in tests to control retries and leases. */
export interface Clock {
  now(): Date;
}

export const systemClock: Clock = {
  now: () => new Date(),
};
