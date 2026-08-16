import { Prisma, type DatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  QUEUE_NAMES,
  type DeadLetteredJobData,
  type WebhookDeliveryJobData,
} from "@pulseroute/shared";
import type { Queue } from "bullmq";
import type { Logger } from "pino";
import { z } from "zod";

import { createDeadLetterJobId } from "./dead-letter.js";
import { readDatabaseNow } from "./database-clock.js";
import {
  DEFAULT_WEBHOOK_DELIVERY_BASE_DELAY_MS,
  DEFAULT_WEBHOOK_DELIVERY_MAX_DELAY_MS,
} from "./delivery-backoff.js";
import {
  claimDueWebhookDeliveries,
  claimExhaustedWebhookDeliveriesForDeadLetter,
  completeExhaustedWebhookDeliveryDeadLetterClaim,
  exhaustInvalidWebhookDeliveryClaim,
  releaseExhaustedWebhookDeliveryDeadLetterClaim,
  releaseWebhookDeliveryClaim,
} from "./delivery-claimer.js";

const DEFAULT_POLL_INTERVAL_MS = 1_000;
const DEFAULT_BATCH_SIZE = 25;
const DEFAULT_CLAIM_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_ATTEMPTS = 5;
const MAX_SAFE_ERROR_LENGTH = 500;

const assignedPayloadSchema = z.object({
  organizationId: z.uuid(),
  serviceRequestId: z.uuid(),
  operatorId: z.uuid(),
  assignmentId: z.uuid(),
  routingDecisionId: z.uuid(),
  scoringVersion: z.string().trim().min(1).max(100),
  correlationId: z.string().trim().min(1).max(200),
});

export type DeliverySchedulerCycleResult = {
  deliveryClaimed: number;
  deliveryPublished: number;
  deliveryPublicationFailed: number;
  staleClaimsRecovered: number;
  abandonedAttempts: number;
  staleAttemptsExhausted: number;
  deadLetterClaimed: number;
  deadLetterPublished: number;
  deadLetterPublicationFailed: number;
};

export type DeliverySchedulerOptions = {
  database: DatabaseClient;
  webhookDeliveryQueue: Queue<WebhookDeliveryJobData>;
  deadLetterQueue: Queue<DeadLetteredJobData>;
  logger: Logger;
  pollIntervalMs?: number;
  batchSize?: number;
  claimTimeoutMs?: number;
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  now?: () => Date;
  random?: () => number;
};

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
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

function normalizeError(error: unknown, recordedAt: Date) {
  const name = error instanceof Error ? error.name : "Error";
  const message =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : "Unknown queue publication failure";

  return {
    code: "QUEUE_PUBLICATION_FAILED",
    name: name.slice(0, 100),
    message: message.slice(0, MAX_SAFE_ERROR_LENGTH),
    recordedAt: recordedAt.toISOString(),
  } satisfies Prisma.InputJsonObject;
}

function createInvalidPayloadEvidence(
  exhaustedAt: Date,
): Prisma.InputJsonObject {
  return {
    code: "INVALID_ASSIGNED_OUTBOX_PAYLOAD",
    message:
      "Assigned OutboxEvent payload is invalid or does not match its durable identity",
    failedAt: exhaustedAt.toISOString(),
  };
}

function readOptionalCorrelationId(payload: Prisma.JsonValue): string | null {
  if (
    typeof payload !== "object" ||
    payload === null ||
    Array.isArray(payload) ||
    typeof payload.correlationId !== "string"
  ) {
    return null;
  }

  const correlationId = payload.correlationId.trim();

  if (correlationId.length === 0) {
    return null;
  }

  return correlationId.slice(0, 200);
}

function createWebhookDeliveryJobId(
  outboxEventId: string,
  expectedAttemptNumber: number,
  claimStartedAt: string,
): string {
  return [
    "webhook-delivery",
    outboxEventId,
    expectedAttemptNumber,
    new Date(claimStartedAt).getTime(),
  ].join("-");
}

function readFailureReason(lastError: Prisma.JsonValue | null): string {
  if (
    typeof lastError === "object" &&
    lastError !== null &&
    !Array.isArray(lastError) &&
    typeof lastError.message === "string"
  ) {
    return lastError.message.slice(0, MAX_SAFE_ERROR_LENGTH);
  }

  return "Webhook delivery attempts exhausted";
}

function readFailureTimestamp(
  lastError: Prisma.JsonValue | null,
  fallback: Date,
): string {
  if (
    typeof lastError === "object" &&
    lastError !== null &&
    !Array.isArray(lastError)
  ) {
    const candidate =
      typeof lastError.failedAt === "string"
        ? lastError.failedAt
        : typeof lastError.recoveredAt === "string"
          ? lastError.recoveredAt
          : null;

    if (candidate && !Number.isNaN(Date.parse(candidate))) {
      return new Date(candidate).toISOString();
    }
  }

  return fallback.toISOString();
}

export class DeliveryScheduler {
  private readonly database: DatabaseClient;
  private readonly webhookDeliveryQueue: Queue<WebhookDeliveryJobData>;
  private readonly deadLetterQueue: Queue<DeadLetteredJobData>;
  private readonly logger: Logger;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly claimTimeoutMs: number;
  private readonly maxAttempts: number;
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly now: (() => Date) | undefined;
  private readonly random: () => number;

  private abortController: AbortController | undefined;
  private loopPromise: Promise<void> | undefined;

  constructor(options: DeliverySchedulerOptions) {
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.claimTimeoutMs = options.claimTimeoutMs ?? DEFAULT_CLAIM_TIMEOUT_MS;
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    this.baseDelayMs =
      options.baseDelayMs ?? DEFAULT_WEBHOOK_DELIVERY_BASE_DELAY_MS;
    this.maxDelayMs =
      options.maxDelayMs ?? DEFAULT_WEBHOOK_DELIVERY_MAX_DELAY_MS;

    assertPositiveInteger(this.pollIntervalMs, "pollIntervalMs");
    assertPositiveInteger(this.batchSize, "batchSize");
    assertPositiveInteger(this.claimTimeoutMs, "claimTimeoutMs");
    assertPositiveInteger(this.maxAttempts, "maxAttempts");
    assertPositiveInteger(this.baseDelayMs, "baseDelayMs");
    assertPositiveInteger(this.maxDelayMs, "maxDelayMs");

    if (this.maxDelayMs < this.baseDelayMs) {
      throw new Error(
        "maxDelayMs must be greater than or equal to baseDelayMs",
      );
    }

    this.database = options.database;
    this.webhookDeliveryQueue = options.webhookDeliveryQueue;
    this.deadLetterQueue = options.deadLetterQueue;
    this.now = options.now;
    this.random = options.random ?? Math.random;
    this.logger = options.logger.child({
      component: "delivery-scheduler",
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
    this.loopPromise = this.runLoop(abortController.signal).finally(() => {
      if (this.abortController === abortController) {
        this.abortController = undefined;
        this.loopPromise = undefined;
      }
    });

    this.logger.info(
      {
        pollIntervalMs: this.pollIntervalMs,
        batchSize: this.batchSize,
        claimTimeoutMs: this.claimTimeoutMs,
      },
      "Delivery scheduler started",
    );
  }

  async stop(): Promise<void> {
    const loopPromise = this.loopPromise;

    if (!loopPromise) {
      return;
    }

    this.abortController?.abort();

    await loopPromise;

    this.logger.info("Delivery scheduler stopped");
  }

  async publishOnce(): Promise<DeliverySchedulerCycleResult> {
    const claimStartedAt = await this.currentTime();
    const staleBefore = new Date(
      claimStartedAt.getTime() - this.claimTimeoutMs,
    );
    const deliveryClaims = await claimDueWebhookDeliveries({
      database: this.database,
      batchSize: this.batchSize,
      claimStartedAt,
      staleBefore,
      maxAttempts: this.maxAttempts,
      baseDelayMs: this.baseDelayMs,
      maxDelayMs: this.maxDelayMs,
      random: this.random,
    });

    let deliveryPublished = 0;
    let deliveryPublicationFailed = 0;

    for (const claim of deliveryClaims.claimed) {
      const parsedPayload = assignedPayloadSchema.safeParse(claim.payload);
      const jobId = createWebhookDeliveryJobId(
        claim.outboxEventId,
        claim.expectedAttemptNumber,
        claim.claimStartedAt,
      );

      if (
        !parsedPayload.success ||
        parsedPayload.data.organizationId !== claim.organizationId ||
        parsedPayload.data.serviceRequestId !== claim.serviceRequestId
      ) {
        deliveryPublicationFailed += 1;

        const exhaustedAt = await this.currentTime();
        const exhausted = await exhaustInvalidWebhookDeliveryClaim({
          database: this.database,
          outboxEventId: claim.outboxEventId,
          organizationId: claim.organizationId,
          claimStartedAt: claim.claimStartedAt,
          expectedAttemptNumber: claim.expectedAttemptNumber,
          exhaustedAt,
          error: createInvalidPayloadEvidence(exhaustedAt),
        });

        this.logger.error(
          {
            outcome: "invalid_outbox_event_exhausted",
            outboxEventId: claim.outboxEventId,
            organizationId: claim.organizationId,
            serviceRequestId: claim.serviceRequestId,
            expectedAttemptNumber: claim.expectedAttemptNumber,
            claimSuperseded: !exhausted,
          },
          "Invalid assigned OutboxEvent was terminally exhausted",
        );

        continue;
      }

      try {
        const jobData: WebhookDeliveryJobData = {
          outboxEventId: claim.outboxEventId,
          organizationId: claim.organizationId,
          correlationId: parsedPayload.data.correlationId,
          expectedAttemptNumber: claim.expectedAttemptNumber,
          claimStartedAt: claim.claimStartedAt,
        };

        await this.webhookDeliveryQueue.add(JOB_NAMES.deliverWebhook, jobData, {
          jobId,
          attempts: 1,
        });

        deliveryPublished += 1;

        this.logger.info(
          {
            outcome: "claimed",
            queue: QUEUE_NAMES.webhookDelivery,
            jobName: JOB_NAMES.deliverWebhook,
            jobId,
            outboxEventId: claim.outboxEventId,
            organizationId: claim.organizationId,
            serviceRequestId: claim.serviceRequestId,
            correlationId: parsedPayload.data.correlationId,
            expectedAttemptNumber: claim.expectedAttemptNumber,
          },
          "Webhook delivery work published",
        );
      } catch (error) {
        deliveryPublicationFailed += 1;

        const recordedAt = await this.currentTime();

        await releaseWebhookDeliveryClaim({
          database: this.database,
          outboxEventId: claim.outboxEventId,
          organizationId: claim.organizationId,
          claimStartedAt: claim.claimStartedAt,
          expectedAttemptNumber: claim.expectedAttemptNumber,
          retryAt: new Date(recordedAt.getTime() + this.pollIntervalMs),
          error: normalizeError(error, recordedAt),
        });

        this.logger.error(
          {
            err: error,
            outcome: "delivery_publication_failed",
            queue: QUEUE_NAMES.webhookDelivery,
            jobId,
            outboxEventId: claim.outboxEventId,
            organizationId: claim.organizationId,
            serviceRequestId: claim.serviceRequestId,
            expectedAttemptNumber: claim.expectedAttemptNumber,
          },
          "Webhook delivery work publication failed",
        );
      }
    }

    const deadLetterClaimStartedAt = await this.currentTime();
    const deadLetterClaims = await claimExhaustedWebhookDeliveriesForDeadLetter(
      {
        database: this.database,
        batchSize: this.batchSize,
        claimStartedAt: deadLetterClaimStartedAt,
        staleBefore: new Date(
          deadLetterClaimStartedAt.getTime() - this.claimTimeoutMs,
        ),
      },
    );

    let deadLetterPublished = 0;
    let deadLetterPublicationFailed = 0;

    for (const claim of deadLetterClaims.claimed) {
      const correlationId = readOptionalCorrelationId(claim.payload);
      const sourceJobId = `webhook-delivery-${claim.outboxEventId}-${claim.attemptCount}`;
      const deadLetterJobId = createDeadLetterJobId(
        QUEUE_NAMES.webhookDelivery,
        sourceJobId,
      );

      try {
        const deadLetterData: DeadLetteredJobData = {
          sourceQueue: QUEUE_NAMES.webhookDelivery,
          sourceJobId,
          sourceJobName: JOB_NAMES.deliverWebhook,
          outboxEventId: claim.outboxEventId,
          organizationId: claim.organizationId,
          serviceRequestId: claim.serviceRequestId,
          correlationId,
          attemptsMade: claim.attemptCount,
          failureReason: readFailureReason(claim.lastError),
          failedAt: readFailureTimestamp(
            claim.lastError,
            deadLetterClaimStartedAt,
          ),
        };

        await this.deadLetterQueue.add(
          JOB_NAMES.deadLetteredJob,
          deadLetterData,
          {
            jobId: deadLetterJobId,
            attempts: 1,
          },
        );

        const completedAt = await this.currentTime();
        const completed = await completeExhaustedWebhookDeliveryDeadLetterClaim(
          {
            database: this.database,
            outboxEventId: claim.outboxEventId,
            organizationId: claim.organizationId,
            claimStartedAt: claim.claimStartedAt,
            completedAt,
          },
        );

        if (!completed) {
          throw new Error("Exhausted OutboxEvent DLQ claim was superseded");
        }

        deadLetterPublished += 1;

        this.logger.warn(
          {
            outcome: "dead_lettered",
            queue: QUEUE_NAMES.deadLetter,
            deadLetterJobId,
            outboxEventId: claim.outboxEventId,
            organizationId: claim.organizationId,
            serviceRequestId: claim.serviceRequestId,
            correlationId,
            attemptCount: claim.attemptCount,
          },
          "Exhausted webhook delivery published to dead letter",
        );
      } catch (error) {
        deadLetterPublicationFailed += 1;

        await releaseExhaustedWebhookDeliveryDeadLetterClaim({
          database: this.database,
          outboxEventId: claim.outboxEventId,
          organizationId: claim.organizationId,
          claimStartedAt: claim.claimStartedAt,
        });

        this.logger.error(
          {
            err: error,
            outcome: "dead_letter_publication_failed",
            deadLetterJobId,
            outboxEventId: claim.outboxEventId,
            organizationId: claim.organizationId,
            serviceRequestId: claim.serviceRequestId,
          },
          "Exhausted webhook delivery publication failed",
        );
      }
    }

    return {
      deliveryClaimed: deliveryClaims.claimed.length,
      deliveryPublished,
      deliveryPublicationFailed,
      staleClaimsRecovered:
        deliveryClaims.recoveredStaleClaims +
        deadLetterClaims.recoveredStaleClaims,
      abandonedAttempts: deliveryClaims.abandonedAttempts,
      staleAttemptsExhausted: deliveryClaims.exhausted,
      deadLetterClaimed: deadLetterClaims.claimed.length,
      deadLetterPublished,
      deadLetterPublicationFailed,
    };
  }

  private async currentTime(): Promise<Date> {
    return this.now ? this.now() : readDatabaseNow(this.database);
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
          "Delivery scheduler polling cycle failed",
        );
      }

      if (!signal.aborted) {
        await waitForDelay(this.pollIntervalMs, signal);
      }
    }
  }
}
