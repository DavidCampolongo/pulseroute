import { z } from "zod";

const environmentSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]),

  DATABASE_URL: z
    .string()
    .trim()
    .min(1, "DATABASE_URL is required")
    .refine((value) => {
      try {
        const url = new URL(value);

        return url.protocol === "postgresql:" || url.protocol === "postgres:";
      } catch {
        return false;
      }
    }, "DATABASE_URL must be a valid PostgreSQL URL"),

  REDIS_URL: z
    .string()
    .trim()
    .min(1, "REDIS_URL is required")
    .refine((value) => {
      try {
        const url = new URL(value);

        return url.protocol === "redis:" || url.protocol === "rediss:";
      } catch {
        return false;
      }
    }, "REDIS_URL must be a valid Redis URL"),

  LOG_LEVEL: z.enum([
    "fatal",
    "error",
    "warn",
    "info",
    "debug",
    "trace",
    "silent",
  ]),

  FAULT_INJECT_SCORING: z.enum(["true", "false"]).default("false"),

  WEBHOOK_DELIVERY_URL: z
    .string()
    .trim()
    .min(1, "WEBHOOK_DELIVERY_URL is required")
    .refine((value) => {
      try {
        const url = new URL(value);

        return url.protocol === "http:" || url.protocol === "https:";
      } catch {
        return false;
      }
    }, "WEBHOOK_DELIVERY_URL must be a valid HTTP or HTTPS URL"),

  OUTBOUND_WEBHOOK_SECRET: z
    .string()
    .min(32, "OUTBOUND_WEBHOOK_SECRET must be at least 32 characters")
    .refine(
      (value) => value.trim().length > 0,
      "OUTBOUND_WEBHOOK_SECRET must not be blank",
    )
    .refine(
      (value) => value === value.trim(),
      "OUTBOUND_WEBHOOK_SECRET must not have leading or trailing whitespace",
    ),

  WEBHOOK_DELIVERY_TIMEOUT_MS: z.coerce
    .number()
    .int("WEBHOOK_DELIVERY_TIMEOUT_MS must be a whole number")
    .min(1, "WEBHOOK_DELIVERY_TIMEOUT_MS must be positive")
    .max(120_000, "WEBHOOK_DELIVERY_TIMEOUT_MS must not exceed 120000")
    .default(5_000),
});

export type WorkerConfig = {
  nodeEnv: "development" | "test" | "production";
  databaseUrl: string;
  redisUrl: string;
  logLevel: "fatal" | "error" | "warn" | "info" | "debug" | "trace" | "silent";
  faultInjectScoring: boolean;
  webhookDeliveryUrl: string;
  outboundWebhookSecret: string;
  webhookDeliveryTimeoutMs: number;
};

export function parseWorkerConfig(
  environment: Record<string, string | undefined>,
): WorkerConfig {
  const result = environmentSchema.safeParse(environment);

  if (!result.success) {
    const problems = result.error.issues.map((issue) => {
      const variableName = issue.path.join(".");

      return `${variableName}: ${issue.message}`;
    });

    throw new Error(
      [
        "Invalid worker environment configuration:",
        ...problems.map((problem) => `- ${problem}`),
      ].join("\n"),
    );
  }

  const configurationProblems: string[] = [];
  const webhookDeliveryUrl = new URL(result.data.WEBHOOK_DELIVERY_URL);
  const isLoopbackHttpUrl =
    webhookDeliveryUrl.protocol === "http:" &&
    ["localhost", "127.0.0.1", "[::1]"].includes(
      webhookDeliveryUrl.hostname.toLowerCase(),
    );

  if (webhookDeliveryUrl.username || webhookDeliveryUrl.password) {
    configurationProblems.push(
      "WEBHOOK_DELIVERY_URL must not include embedded credentials",
    );
  }

  if (
    result.data.NODE_ENV === "production" &&
    webhookDeliveryUrl.protocol !== "https:"
  ) {
    configurationProblems.push(
      "WEBHOOK_DELIVERY_URL must use HTTPS in production",
    );
  } else if (webhookDeliveryUrl.protocol === "http:" && !isLoopbackHttpUrl) {
    configurationProblems.push(
      "WEBHOOK_DELIVERY_URL may use HTTP only for a loopback receiver",
    );
  }

  if (
    environment.WEBHOOK_SECRET !== undefined &&
    result.data.OUTBOUND_WEBHOOK_SECRET === environment.WEBHOOK_SECRET
  ) {
    configurationProblems.push(
      "OUTBOUND_WEBHOOK_SECRET must be distinct from WEBHOOK_SECRET",
    );
  }

  if (configurationProblems.length > 0) {
    throw new Error(
      [
        "Invalid worker environment configuration:",
        ...configurationProblems.map((problem) => `- ${problem}`),
      ].join("\n"),
    );
  }

  return {
    nodeEnv: result.data.NODE_ENV,
    databaseUrl: result.data.DATABASE_URL,
    redisUrl: result.data.REDIS_URL,
    logLevel: result.data.LOG_LEVEL,
    faultInjectScoring: result.data.FAULT_INJECT_SCORING === "true",
    webhookDeliveryUrl: result.data.WEBHOOK_DELIVERY_URL,
    outboundWebhookSecret: result.data.OUTBOUND_WEBHOOK_SECRET,
    webhookDeliveryTimeoutMs: result.data.WEBHOOK_DELIVERY_TIMEOUT_MS,
  };
}
