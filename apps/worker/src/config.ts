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
});

export type WorkerConfig = {
  nodeEnv: "development" | "test" | "production";
  databaseUrl: string;
  redisUrl: string;
  logLevel: "fatal" | "error" | "warn" | "info" | "debug" | "trace" | "silent";
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

  return {
    nodeEnv: result.data.NODE_ENV,
    databaseUrl: result.data.DATABASE_URL,
    redisUrl: result.data.REDIS_URL,
    logLevel: result.data.LOG_LEVEL,
  };
}
