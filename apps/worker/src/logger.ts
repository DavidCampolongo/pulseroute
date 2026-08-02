import type { QueueName } from "@pulseroute/shared";
import pino, { type DestinationStream, type Logger } from "pino";

import type { WorkerConfig } from "./config.js";

export type JobLogContext = {
  queue: QueueName;
  jobName: string;
  jobId: string;
  attemptsMade: number;
  organizationId?: string | null;
  serviceRequestId?: string | null;
  correlationId?: string | null;
};

const sensitivePaths = [
  "databaseUrl",
  "redisUrl",
  "webhookSecret",
  "WEBHOOK_SECRET",
  "config.databaseUrl",
  "config.redisUrl",
  "config.webhookSecret",
  "config.WEBHOOK_SECRET",
];

export function createWorkerLogger(
  config: Pick<WorkerConfig, "nodeEnv" | "logLevel">,
  destination?: DestinationStream,
): Logger {
  return pino(
    {
      level: config.logLevel,
      base: {
        service: "pulseroute-worker",
        processType: "worker",
        environment: config.nodeEnv,
        pid: process.pid,
      },
      redact: {
        paths: sensitivePaths,
        remove: true,
      },
    },
    destination,
  );
}

export function createJobLogger(
  logger: Logger,
  context: JobLogContext,
): Logger {
  return logger.child(context);
}
