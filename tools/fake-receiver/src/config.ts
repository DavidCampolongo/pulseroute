export const FAKE_RECEIVER_MODES = ["success", "failure", "timeout"] as const;

export type FakeReceiverMode = (typeof FAKE_RECEIVER_MODES)[number];

export const DEFAULT_RECEIVER_HOST = "127.0.0.1";
export const PRODUCTION_RECEIVER_HOST = "0.0.0.0";
export const DEFAULT_RECEIVER_PORT = 3_100;
export const DEFAULT_RECEIVER_DELAY_MS = 5_000;
export const MAX_RECEIVER_DELAY_MS = 60_000;

export type FakeReceiverConfig = {
  host: string;
  port: number;
  outboundWebhookSecret: string;
  mode: FakeReceiverMode;
  delayMs: number;
};

function isFakeReceiverMode(value: string): value is FakeReceiverMode {
  return (FAKE_RECEIVER_MODES as readonly string[]).includes(value);
}

function parseBoundedInteger(options: {
  name: string;
  rawValue: string | undefined;
  defaultValue: number;
  minimum: number;
  maximum: number;
  problems: string[];
}): number {
  const rawValue = options.rawValue ?? String(options.defaultValue);

  if (!/^(0|[1-9]\d*)$/.test(rawValue)) {
    options.problems.push(`${options.name}: must be a whole number`);

    return options.defaultValue;
  }

  const parsedValue = Number(rawValue);

  if (
    !Number.isSafeInteger(parsedValue) ||
    parsedValue < options.minimum ||
    parsedValue > options.maximum
  ) {
    options.problems.push(
      `${options.name}: must be between ${options.minimum} and ${options.maximum}`,
    );

    return options.defaultValue;
  }

  return parsedValue;
}

export function parseFakeReceiverConfig(
  environment: Record<string, string | undefined>,
): FakeReceiverConfig {
  const problems: string[] = [];
  const railwayPort = environment.PORT;

  const port = parseBoundedInteger({
    name: railwayPort === undefined ? "RECEIVER_PORT" : "PORT",
    rawValue: railwayPort ?? environment.RECEIVER_PORT,
    defaultValue: DEFAULT_RECEIVER_PORT,
    minimum: 1,
    maximum: 65_535,
    problems,
  });

  const outboundWebhookSecret = environment.OUTBOUND_WEBHOOK_SECRET;

  if (outboundWebhookSecret === undefined) {
    problems.push("OUTBOUND_WEBHOOK_SECRET: is required");
  } else if (outboundWebhookSecret.trim().length === 0) {
    problems.push("OUTBOUND_WEBHOOK_SECRET: must not be blank");
  } else if (outboundWebhookSecret !== outboundWebhookSecret.trim()) {
    problems.push(
      "OUTBOUND_WEBHOOK_SECRET: must not have leading or trailing whitespace",
    );
  } else if (outboundWebhookSecret.length < 32) {
    problems.push("OUTBOUND_WEBHOOK_SECRET: must be at least 32 characters");
  }

  const rawMode = environment.RECEIVER_MODE ?? "success";

  if (!isFakeReceiverMode(rawMode)) {
    problems.push(
      `RECEIVER_MODE: must be one of ${FAKE_RECEIVER_MODES.join(", ")}`,
    );
  }

  const delayMs = parseBoundedInteger({
    name: "RECEIVER_DELAY_MS",
    rawValue: environment.RECEIVER_DELAY_MS,
    defaultValue: DEFAULT_RECEIVER_DELAY_MS,
    minimum: 1,
    maximum: MAX_RECEIVER_DELAY_MS,
    problems,
  });

  if (problems.length > 0) {
    throw new Error(
      [
        "Invalid fake receiver environment configuration:",
        ...problems.map((problem) => `- ${problem}`),
      ].join("\n"),
    );
  }

  return {
    host:
      environment.NODE_ENV === "production"
        ? PRODUCTION_RECEIVER_HOST
        : DEFAULT_RECEIVER_HOST,
    port,
    outboundWebhookSecret: outboundWebhookSecret!,
    mode: rawMode as FakeReceiverMode,
    delayMs,
  };
}
