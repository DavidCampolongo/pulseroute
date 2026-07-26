import { z } from "zod";

const environmentSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]),

  API_HOST: z.string().trim().min(1, "API_HOST is required"),

  API_PORT: z.coerce
    .number()
    .int("API_PORT must be a whole number")
    .min(1, "API_PORT must be at least 1")
    .max(65535, "API_PORT must be at most 65535"),

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

export type AppConfig = {
  nodeEnv: "development" | "test" | "production";
  host: string;
  port: number;
  databaseUrl: string;
  logLevel: "fatal" | "error" | "warn" | "info" | "debug" | "trace" | "silent";
};

export function parseConfig(
  environment: Record<string, string | undefined>,
): AppConfig {
  const result = environmentSchema.safeParse(environment);

  if (!result.success) {
    const problems = result.error.issues.map((issue) => {
      const variableName = issue.path.join(".");

      return `${variableName}: ${issue.message}`;
    });

    throw new Error(
      [
        "Invalid environment configuration:",
        ...problems.map((problem) => `- ${problem}`),
      ].join("\n"),
    );
  }

  return {
    nodeEnv: result.data.NODE_ENV,
    host: result.data.API_HOST,
    port: result.data.API_PORT,
    databaseUrl: result.data.DATABASE_URL,
    logLevel: result.data.LOG_LEVEL,
  };
}
