export const MAX_SIMULATOR_COUNT = 100;
export const MAX_SIMULATOR_DUPLICATES = 100;
export const DEFAULT_SIMULATOR_TIMEOUT_MS = 15_000;

const optionNames = ["--base-url", "--count", "--duplicates"] as const;

type OptionName = (typeof optionNames)[number];

export type SimulatorConfig = {
  baseUrl: string;
  endpoint: string;
  count: number;
  duplicates: number;
  webhookSecret: string;
  timeoutMs: number;
};

function isOptionName(value: string): value is OptionName {
  return (optionNames as readonly string[]).includes(value);
}

function readOptions(arguments_: readonly string[]): Map<OptionName, string> {
  const values = new Map<OptionName, string>();
  const problems: string[] = [];

  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index];

    if (argument === undefined) {
      continue;
    }

    const equalsIndex = argument.indexOf("=");
    const optionName =
      equalsIndex === -1 ? argument : argument.slice(0, equalsIndex);

    if (!isOptionName(optionName)) {
      problems.push(
        argument.startsWith("--")
          ? "Unknown simulator option"
          : "Unexpected positional simulator argument",
      );
      continue;
    }

    if (values.has(optionName)) {
      problems.push(`${optionName}: may only be provided once`);
      continue;
    }

    let value: string | undefined;

    if (equalsIndex === -1) {
      value = arguments_[index + 1];

      if (value !== undefined && !value.startsWith("--")) {
        index += 1;
      } else {
        value = undefined;
      }
    } else {
      value = argument.slice(equalsIndex + 1);
    }

    if (value === undefined || value.length === 0) {
      problems.push(`${optionName}: requires a value`);
      continue;
    }

    values.set(optionName, value);
  }

  for (const optionName of optionNames) {
    if (!values.has(optionName)) {
      problems.push(`${optionName}: is required`);
    }
  }

  if (problems.length > 0) {
    throw new Error(
      [
        "Invalid simulator command line:",
        ...problems.map((problem) => `- ${problem}`),
      ].join("\n"),
    );
  }

  return values;
}

function parseBoundedInteger(options: {
  name: OptionName;
  value: string;
  minimum: number;
  maximum: number;
}): number {
  if (!/^(0|[1-9]\d*)$/.test(options.value)) {
    throw new Error(`${options.name}: must be a whole number`);
  }

  const parsed = Number(options.value);

  if (
    !Number.isSafeInteger(parsed) ||
    parsed < options.minimum ||
    parsed > options.maximum
  ) {
    throw new Error(
      `${options.name}: must be between ${options.minimum} and ${options.maximum}`,
    );
  }

  return parsed;
}

function parseBaseUrl(rawValue: string): string {
  if (rawValue !== rawValue.trim()) {
    throw new Error("--base-url: must not contain surrounding whitespace");
  }

  let url: URL;

  try {
    url = new URL(rawValue);
  } catch {
    throw new Error("--base-url: must be a valid absolute URL");
  }

  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("--base-url: protocol must be HTTP or HTTPS");
  }

  if (url.username || url.password) {
    throw new Error("--base-url: embedded credentials are not allowed");
  }

  if (url.search || url.hash) {
    throw new Error("--base-url: query strings and fragments are not allowed");
  }

  if (url.pathname !== "/") {
    throw new Error("--base-url: must not include an application path");
  }

  return url.origin;
}

export function parseSimulatorConfig(
  arguments_: readonly string[],
  environment: Record<string, string | undefined>,
): SimulatorConfig {
  const values = readOptions(arguments_);
  const webhookSecret = environment.WEBHOOK_SECRET;

  if (webhookSecret === undefined) {
    throw new Error("WEBHOOK_SECRET: is required");
  }

  if (webhookSecret.length < 32) {
    throw new Error("WEBHOOK_SECRET: must be at least 32 characters");
  }

  const rawBaseUrl = values.get("--base-url");
  const rawCount = values.get("--count");
  const rawDuplicates = values.get("--duplicates");

  if (
    rawBaseUrl === undefined ||
    rawCount === undefined ||
    rawDuplicates === undefined
  ) {
    throw new Error("Simulator command-line parsing invariant failed");
  }

  const baseUrl = parseBaseUrl(rawBaseUrl);

  return {
    baseUrl,
    endpoint: `${baseUrl}/webhooks/service-requests`,
    count: parseBoundedInteger({
      name: "--count",
      value: rawCount,
      minimum: 1,
      maximum: MAX_SIMULATOR_COUNT,
    }),
    duplicates: parseBoundedInteger({
      name: "--duplicates",
      value: rawDuplicates,
      minimum: 0,
      maximum: MAX_SIMULATOR_DUPLICATES,
    }),
    webhookSecret,
    timeoutMs: DEFAULT_SIMULATOR_TIMEOUT_MS,
  };
}
