import { Prisma, type DatabaseClient } from "@pulseroute/db";
import {
  EVENT_TYPES,
  JOB_NAMES,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import type { Queue } from "bullmq";
import type { Logger } from "pino";
import { z } from "zod";

const DEFAULT_POLL_INTERVAL_MS = 1_000;
const DEFAULT_BATCH_SIZE = 25;

const outboxPayloadSchema = z.looseObject({
  serviceRequestId: z.uuid(),
  correlationId: z.string().trim().min(1),
});

export type OutboxPublishCycleResult = {
  selected: number;
  published: number;
  failed: number;
};

type InternalOutboxPublisherOptions = {
  database: DatabaseClient;
  incomingQueue: Queue<ServiceRequestIngestedJobData>;
  logger: Logger;
  pollIntervalMs?: number;
  batchSize?: number;
  now?: () => Date;
};

function toError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new Error("Unknown outbox publication failure");
}

function createErrorEvidence(
  error: unknown,
  recordedAt: Date,
): Prisma.InputJsonObject {
  const normalizedError = toError(error);

  return {
    name: normalizedError.name,
    message: normalizedError.message,
    recordedAt: recordedAt.toISOString(),
  };
}

function waitForDelay(
  milliseconds: number,
  signal: AbortSignal,
): Promise<void> {
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timeout);

      signal.removeEventListener("abort", finish);

      resolve();
    };

    const timeout = setTimeout(finish, milliseconds);

    signal.addEventListener("abort", finish, {
      once: true,
    });

    if (signal.aborted) {
      finish();
    }
  });
}

export function createIncomingJobId(outboxEventId: string): string {
  return `outbox-${outboxEventId}`;
}

export class InternalOutboxPublisher {
  private readonly database: DatabaseClient;
  private readonly incomingQueue: Queue<ServiceRequestIngestedJobData>;
  private readonly logger: Logger;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly now: () => Date;

  private abortController: AbortController | undefined;
  private loopPromise: Promise<void> | undefined;

  constructor(options: InternalOutboxPublisherOptions) {
    if (
      !Number.isInteger(options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS) ||
      (options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS) < 1
    ) {
      throw new Error(
        "Outbox publisher pollIntervalMs must be a positive integer",
      );
    }

    if (
      !Number.isInteger(options.batchSize ?? DEFAULT_BATCH_SIZE) ||
      (options.batchSize ?? DEFAULT_BATCH_SIZE) < 1
    ) {
      throw new Error("Outbox publisher batchSize must be a positive integer");
    }

    this.database = options.database;
    this.incomingQueue = options.incomingQueue;
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.now = options.now ?? (() => new Date());

    this.logger = options.logger.child({
      component: "internal-outbox-publisher",
      queue: options.incomingQueue.name,
    });
  }

  get isRunning(): boolean {
    return this.loopPromise !== undefined;
  }

  start(): void {
    if (this.loopPromise) {
      return;
    }

    const abortController = new AbortController();

    this.abortController = abortController;

    this.logger.info(
      {
        pollIntervalMs: this.pollIntervalMs,
        batchSize: this.batchSize,
      },
      "Outbox publisher started",
    );

    this.loopPromise = this.runLoop(abortController.signal).finally(() => {
      if (this.abortController === abortController) {
        this.abortController = undefined;
        this.loopPromise = undefined;
      }
    });
  }

  async stop(): Promise<void> {
    const loopPromise = this.loopPromise;

    if (!loopPromise) {
      return;
    }

    this.logger.info("Outbox publisher shutdown started");

    this.abortController?.abort();

    await loopPromise;

    this.logger.info("Outbox publisher stopped");
  }

  async publishOnce(): Promise<OutboxPublishCycleResult> {
    const selectedAt = this.now();

    const events = await this.database.outboxEvent.findMany({
      where: {
        eventType: EVENT_TYPES.serviceRequestCreated,
        status: "PENDING",
        nextAttemptAt: {
          lte: selectedAt,
        },
      },
      orderBy: [
        {
          createdAt: "asc",
        },
        {
          id: "asc",
        },
      ],
      take: this.batchSize,
      select: {
        id: true,
        organizationId: true,
        aggregateId: true,
        payload: true,
      },
    });

    let published = 0;
    let failed = 0;

    for (const event of events) {
      const startedAt = Date.now();
      const jobId = createIncomingJobId(event.id);

      try {
        const payloadResult = outboxPayloadSchema.safeParse(event.payload);

        if (!payloadResult.success) {
          throw new Error(
            "Outbox event payload is missing valid serviceRequestId or correlationId",
          );
        }

        if (payloadResult.data.serviceRequestId !== event.aggregateId) {
          throw new Error(
            "Outbox payload serviceRequestId does not match aggregateId",
          );
        }

        const jobData: ServiceRequestIngestedJobData = {
          outboxEventId: event.id,
          organizationId: event.organizationId,
          serviceRequestId: event.aggregateId,
          correlationId: payloadResult.data.correlationId,
        };

        await this.incomingQueue.add(
          JOB_NAMES.serviceRequestIngested,
          jobData,
          {
            jobId,
          },
        );

        const processedAt = this.now();

        await this.database.outboxEvent.updateMany({
          where: {
            id: event.id,
            status: "PENDING",
          },
          data: {
            status: "DELIVERED",
            attemptCount: {
              increment: 1,
            },
            processedAt,
            nextAttemptAt: processedAt,
            lastError: Prisma.DbNull,
          },
        });

        published += 1;

        this.logger.info(
          {
            outcome: "published",
            jobName: JOB_NAMES.serviceRequestIngested,
            jobId,
            outboxEventId: event.id,
            organizationId: event.organizationId,
            serviceRequestId: event.aggregateId,
            correlationId: payloadResult.data.correlationId,
            durationMs: Date.now() - startedAt,
          },
          "Outbox event published",
        );
      } catch (error) {
        failed += 1;

        const recordedAt = this.now();

        try {
          await this.database.outboxEvent.updateMany({
            where: {
              id: event.id,
              status: "PENDING",
            },
            data: {
              attemptCount: {
                increment: 1,
              },
              nextAttemptAt: new Date(
                recordedAt.getTime() + this.pollIntervalMs,
              ),
              lastError: createErrorEvidence(error, recordedAt),
            },
          });
        } catch (recordingError) {
          this.logger.error(
            {
              err: recordingError,
              outboxEventId: event.id,
            },
            "Failed to record outbox publication failure",
          );
        }

        this.logger.error(
          {
            err: error,
            outcome: "publication_failed",
            jobName: JOB_NAMES.serviceRequestIngested,
            jobId,
            outboxEventId: event.id,
            organizationId: event.organizationId,
            serviceRequestId: event.aggregateId,
            durationMs: Date.now() - startedAt,
          },
          "Outbox event publication failed",
        );
      }
    }

    return {
      selected: events.length,
      published,
      failed,
    };
  }

  private async runLoop(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      try {
        await this.publishOnce();
      } catch (error) {
        this.logger.error(
          {
            err: error,
            outcome: "poll_failed",
          },
          "Outbox publisher polling cycle failed",
        );
      }

      if (!signal.aborted) {
        await waitForDelay(this.pollIntervalMs, signal);
      }
    }
  }
}
