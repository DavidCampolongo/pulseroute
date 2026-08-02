import { createHash } from "node:crypto";

import {
  JOB_NAMES,
  type DeadLetteredJobData,
  type QueueName,
} from "@pulseroute/shared";
import type { Processor, Queue } from "bullmq";
import type { Logger } from "pino";

import { createJobLogger } from "./logger.js";

const MAX_FAILURE_REASON_LENGTH = 1_000;

type DeadLetterIdentity = {
  organizationId: string | null;
  serviceRequestId: string | null;
  correlationId: string | null;
};

type DeadLetteringProcessorOptions<
  DataType,
  ResultType,
  NameType extends string,
> = {
  sourceQueue: QueueName;
  deadLetterQueue: Queue<DeadLetteredJobData>;
  processor: Processor<DataType, ResultType, NameType>;
  logger: Logger;
  now?: () => Date;
};

function readOptionalIdentifier(
  data: unknown,
  field: keyof DeadLetterIdentity,
): string | null {
  if (typeof data !== "object" || data === null || Array.isArray(data)) {
    return null;
  }

  const value = (data as Record<string, unknown>)[field];

  if (typeof value !== "string") {
    return null;
  }

  const trimmedValue = value.trim();

  return trimmedValue.length > 0 ? trimmedValue : null;
}

function readIdentity(data: unknown): DeadLetterIdentity {
  return {
    organizationId: readOptionalIdentifier(data, "organizationId"),

    serviceRequestId: readOptionalIdentifier(data, "serviceRequestId"),

    correlationId: readOptionalIdentifier(data, "correlationId"),
  };
}

function normalizeFailureReason(error: unknown): string {
  const message =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : "Unknown worker failure";

  const normalizedMessage = message.trim() || "Unknown worker failure";

  return normalizedMessage.slice(0, MAX_FAILURE_REASON_LENGTH);
}

function getMaximumAttempts(configuredAttempts: number | undefined): number {
  if (
    typeof configuredAttempts === "number" &&
    Number.isInteger(configuredAttempts) &&
    configuredAttempts > 0
  ) {
    return configuredAttempts;
  }

  return 1;
}

export function createDeadLetterJobId(
  sourceQueue: QueueName,
  sourceJobId: string,
): string {
  const digest = createHash("sha256")
    .update(sourceQueue, "utf8")
    .update("\0", "utf8")
    .update(sourceJobId, "utf8")
    .digest("hex");

  return `dead-letter-${digest}`;
}

export function createDeadLetteringProcessor<
  DataType,
  ResultType,
  NameType extends string,
>(
  options: DeadLetteringProcessorOptions<DataType, ResultType, NameType>,
): Processor<DataType, ResultType, NameType> {
  const now = options.now ?? (() => new Date());

  return async (job, token, signal) => {
    try {
      return await options.processor(job, token, signal);
    } catch (error) {
      /*
       * Inside the processor, attemptsMade contains the number of completed
       * prior attempts. Add one to describe the execution currently failing.
       */
      const attemptsMade = job.attemptsMade + 1;

      const maximumAttempts = getMaximumAttempts(job.opts.attempts);

      if (attemptsMade >= maximumAttempts) {
        const sourceJobId = job.id ?? `generated-${job.timestamp}`;

        const identity = readIdentity(job.data);

        const failureReason = normalizeFailureReason(error);

        const failedAt = now().toISOString();

        const deadLetterJobId = createDeadLetterJobId(
          options.sourceQueue,
          sourceJobId,
        );

        const deadLetterData: DeadLetteredJobData = {
          sourceQueue: options.sourceQueue,
          sourceJobId,
          sourceJobName: job.name,
          organizationId: identity.organizationId,
          serviceRequestId: identity.serviceRequestId,
          correlationId: identity.correlationId,
          attemptsMade,
          failureReason,
          failedAt,
        };

        const jobLogger = createJobLogger(options.logger, {
          queue: options.sourceQueue,
          jobName: job.name,
          jobId: sourceJobId,
          attemptsMade,
          organizationId: identity.organizationId,
          serviceRequestId: identity.serviceRequestId,
          correlationId: identity.correlationId,
        });

        try {
          await options.deadLetterQueue.add(
            JOB_NAMES.deadLetteredJob,
            deadLetterData,
            {
              jobId: deadLetterJobId,
            },
          );

          jobLogger.warn(
            {
              outcome: "dead_letter_ready",
              deadLetterJobId,
              failureReason,
              failedAt,
            },
            "Job retries exhausted",
          );
        } catch (deadLetterError) {
          /*
           * Preserve the original processor failure as the source job's
           * failure reason. The publication failure is logged separately.
           */
          jobLogger.error(
            {
              err: deadLetterError,
              outcome: "dead_letter_publication_failed",
              deadLetterJobId,
            },
            "Dead-letter publication failed",
          );
        }
      }

      throw error;
    }
  };
}
