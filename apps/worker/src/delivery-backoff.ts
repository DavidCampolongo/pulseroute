export const DEFAULT_WEBHOOK_DELIVERY_BASE_DELAY_MS = 1_000;
export const DEFAULT_WEBHOOK_DELIVERY_MAX_DELAY_MS = 60_000;

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
}

export function calculateFullJitterDelay(options: {
  retryIndex: number;
  baseDelayMs: number;
  maxDelayMs: number;
  randomValue: number;
}): { exponentialCapMs: number; delayMs: number } {
  if (!Number.isInteger(options.retryIndex) || options.retryIndex < 0) {
    throw new Error("retryIndex must be a nonnegative integer");
  }

  assertPositiveInteger(options.baseDelayMs, "baseDelayMs");
  assertPositiveInteger(options.maxDelayMs, "maxDelayMs");

  if (options.maxDelayMs < options.baseDelayMs) {
    throw new Error("maxDelayMs must be greater than or equal to baseDelayMs");
  }

  if (
    !Number.isFinite(options.randomValue) ||
    options.randomValue < 0 ||
    options.randomValue >= 1
  ) {
    throw new Error("randomValue must be in the range [0, 1)");
  }

  const exponent = Math.min(options.retryIndex, 52);
  const exponentialCapMs = Math.min(
    options.maxDelayMs,
    options.baseDelayMs * 2 ** exponent,
  );

  return {
    exponentialCapMs,
    delayMs: Math.floor(options.randomValue * (exponentialCapMs + 1)),
  };
}
