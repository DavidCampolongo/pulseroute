import { createHmac, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  EVENT_TYPES,
  JOB_NAMES,
  QUEUE_NAMES,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { type Job, Queue } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { buildApp } from "../../api/src/app.js";
import {
  createIncomingWorker,
  createRoutingJobId,
} from "../src/incoming-worker.js";
import { createWorkerLogger } from "../src/logger.js";
import {
  createIncomingJobId,
  InternalOutboxPublisher,
} from "../src/outbox-publisher.js";
import {
  closePulseRouteQueues,
  createPulseRouteQueues,
  type PulseRouteQueues,
  waitForPulseRouteQueues,
} from "../src/queues.js";
import { createProducerRedisOptions } from "../src/redis.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const baseRedisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for Redis-outage recovery tests");
}

if (!baseRedisUrl) {
  throw new Error("REDIS_URL is required for Redis-outage recovery tests");
}

const recoveryRedisUrl = new URL(baseRedisUrl);

/*
 * This suite uses the fixed production queue names, so isolate them in a
 * dedicated logical Redis database.
 */
recoveryRedisUrl.pathname = "/13";

const redisUrl = recoveryRedisUrl.toString();

const unavailableRedisUrl = new URL(baseRedisUrl);

unavailableRedisUrl.hostname = "127.0.0.1";
unavailableRedisUrl.port = "6399";
unavailableRedisUrl.pathname = "/13";

const webhookSecret = "phase6-redis-outage-secret-at-least-32-characters";

const webhookToleranceSeconds = 300;

const webhookTimestampHeader = "x-pulseroute-timestamp";

const webhookSignatureHeader = "x-pulseroute-signature";

const organizationId = randomUUID();
const requiredSkillId = randomUUID();

const externalEventId = `phase6-redis-outage-event-${randomUUID()}`;

const externalRequestId = `phase6-redis-outage-request-${randomUUID()}`;

/*
 * The publisher scans all globally eligible events.
 *
 * An isolated historical clock and old createdAt value ensure this fixture is
 * selected without deleting or filtering unrelated durable application data.
 */
const outageNow = new Date("1980-01-01T00:00:00.000Z");

const eventCreatedAt = new Date("1979-12-31T23:58:00.000Z");

const eventDueAt = new Date("1979-12-31T23:59:00.000Z");

const retryDueAt = new Date(outageNow.getTime() + 100);

const recoveryNow = new Date(outageNow.getTime() + 1_000);

const app = buildApp({
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3000,
  databaseUrl,
  logLevel: "silent",
  webhookSecret,
  webhookToleranceSeconds,
});

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

let unavailableQueue: Queue<ServiceRequestIngestedJobData> | undefined;

let recoveryQueues: PulseRouteQueues | undefined;

let incomingWorker: ReturnType<typeof createIncomingWorker> | undefined;

function createSignedHeaders(rawBody: string): Record<string, string> {
  const timestamp = String(Math.floor(Date.now() / 1_000));

  const signature = createHmac("sha256", webhookSecret)
    .update(timestamp, "utf8")
    .update(".", "utf8")
    .update(Buffer.from(rawBody, "utf8"))
    .digest("hex");

  return {
    "content-type": "application/json",
    [webhookTimestampHeader]: timestamp,
    [webhookSignatureHeader]: signature,
  };
}

function createWebhookBody(): string {
  return JSON.stringify({
    organizationId,
    eventId: externalEventId,
    type: EVENT_TYPES.serviceRequestCreated,
    data: {
      externalId: externalRequestId,
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  });
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitForJob<DataType>(
  queue: Queue<DataType>,
  jobId: string,
  timeoutMs = 5_000,
): Promise<Job<DataType>> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const job = await queue.getJob(jobId);

    if (job) {
      return job;
    }

    await sleep(25);
  }

  throw new Error(`Timed out waiting for BullMQ job ${jobId}`);
}

async function waitForJobState<DataType>(
  job: Job<DataType>,
  expectedState: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const state = await job.getState();

    if (state === expectedState) {
      return;
    }

    await sleep(25);
  }

  const finalState = await job.getState();

  throw new Error(
    `Timed out waiting for job ${job.id} to reach ${expectedState}; final state was ${finalState}`,
  );
}

async function clearDatabaseFixture(): Promise<void> {
  await app.db.auditLog.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.outboxEvent.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.webhookEvent.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.serviceRequest.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.skill.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });
}

async function obliterateRecoveryQueues(
  queues: PulseRouteQueues,
): Promise<void> {
  await Promise.all([
    queues.incomingEvents
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    queues.routing
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    queues.notifications
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    queues.webhookDelivery
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    queues.deadLetter
      .obliterate({
        force: true,
      })
      .catch(() => undefined),
  ]);
}

beforeAll(async () => {
  await app.ready();

  await clearDatabaseFixture();

  await app.db.organization.create({
    data: {
      id: organizationId,
      name: "Phase 6 Redis-Outage Test Organization",
    },
  });

  await app.db.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Phase 6 Redis-Outage Test Skill",
    },
  });
});

afterAll(async () => {
  if (incomingWorker) {
    await incomingWorker.close();
  }

  if (unavailableQueue) {
    await unavailableQueue.close().catch(() => undefined);
  }

  if (recoveryQueues) {
    await obliterateRecoveryQueues(recoveryQueues);

    await closePulseRouteQueues(recoveryQueues);
  }

  await clearDatabaseFixture();

  await app.close();
});

describe("durable recovery while Redis publication is unavailable", () => {
  it("keeps the webhook durable and resumes queue processing after Redis recovery", async () => {
    const rawBody = createWebhookBody();

    /*
     * No working BullMQ queue or worker has been created yet.
     * The API nevertheless accepts and persists the webhook.
     */
    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: createSignedHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(202);

    const responseBody = response.json() as {
      status: string;
      requestId: string;
      serviceRequestId: string;
    };

    expect(responseBody).toMatchObject({
      status: "accepted",
      requestId: expect.any(String),
      serviceRequestId: expect.any(String),
    });

    const serviceRequest = await app.db.serviceRequest.findUniqueOrThrow({
      where: {
        id: responseBody.serviceRequestId,
      },
    });

    expect(serviceRequest).toMatchObject({
      organizationId,
      externalId: externalRequestId,
      status: "PENDING",
    });

    const outboxEvent = await app.db.outboxEvent.findFirstOrThrow({
      where: {
        organizationId,
        eventType: EVENT_TYPES.serviceRequestCreated,
        aggregateId: serviceRequest.id,
      },
    });

    expect(outboxEvent.status).toBe("PENDING");
    expect(outboxEvent.attemptCount).toBe(0);
    expect(outboxEvent.processedAt).toBeNull();

    await app.db.outboxEvent.update({
      where: {
        id: outboxEvent.id,
      },
      data: {
        createdAt: eventCreatedAt,
        nextAttemptAt: eventDueAt,
      },
    });

    unavailableQueue = new Queue<ServiceRequestIngestedJobData>(
      QUEUE_NAMES.incomingEvents,
      {
        connection: createProducerRedisOptions(unavailableRedisUrl.toString()),
        skipWaitingForReady: true,
      },
    );

    /*
     * Expected infrastructure failure. Register an error listener so the
     * connection refusal is observed rather than becoming unhandled noise.
     */
    unavailableQueue.on("error", () => undefined);

    const unavailablePublisher = new InternalOutboxPublisher({
      database: app.db,
      incomingQueue: unavailableQueue,
      logger,
      pollIntervalMs: 100,
      batchSize: 1,
      now: () => outageNow,
    });

    const failedPublication = await unavailablePublisher.publishOnce();

    expect(failedPublication).toEqual({
      selected: 1,
      published: 0,
      failed: 1,
    });

    const pendingOutbox = await app.db.outboxEvent.findUniqueOrThrow({
      where: {
        id: outboxEvent.id,
      },
    });

    expect(pendingOutbox.status).toBe("PENDING");

    expect(pendingOutbox.processedAt).toBeNull();
    expect(pendingOutbox.attemptCount).toBe(1);
    expect(pendingOutbox.nextAttemptAt).toEqual(retryDueAt);

    expect(pendingOutbox.lastError).toMatchObject({
      recordedAt: outageNow.toISOString(),
    });

    await unavailableQueue.close();
    unavailableQueue = undefined;

    /*
     * Restore access to real Redis and start the real Phase 6 queue/worker
     * path.
     */
    recoveryQueues = createPulseRouteQueues(redisUrl);

    await waitForPulseRouteQueues(recoveryQueues);

    await obliterateRecoveryQueues(recoveryQueues);

    incomingWorker = createIncomingWorker({
      database: app.db,
      routingQueue: recoveryQueues.routing,
      deadLetterQueue: recoveryQueues.deadLetter,
      logger,
      redisUrl,
      concurrency: 1,
    });

    incomingWorker.on("error", (error) => {
      logger.error(
        {
          err: error,
        },
        "Redis recovery test worker error",
      );
    });

    await incomingWorker.waitUntilReady();

    const recoveryPublisher = new InternalOutboxPublisher({
      database: app.db,
      incomingQueue: recoveryQueues.incomingEvents,
      logger,
      pollIntervalMs: 100,
      batchSize: 1,
      now: () => recoveryNow,
    });

    const recoveredPublication = await recoveryPublisher.publishOnce();

    expect(recoveredPublication).toEqual({
      selected: 1,
      published: 1,
      failed: 0,
    });

    const incomingJobId = createIncomingJobId(outboxEvent.id);

    const incomingJob = await waitForJob<ServiceRequestIngestedJobData>(
      recoveryQueues.incomingEvents,
      incomingJobId,
    );

    await waitForJobState(incomingJob, "completed");

    expect(incomingJob.name).toBe(JOB_NAMES.serviceRequestIngested);

    expect(incomingJob.data).toEqual({
      outboxEventId: outboxEvent.id,
      organizationId,
      serviceRequestId: serviceRequest.id,
      correlationId: responseBody.requestId,
    });

    const routingJobId = createRoutingJobId(serviceRequest.id);

    const routingJob = await waitForJob<RouteServiceRequestJobData>(
      recoveryQueues.routing,
      routingJobId,
    );

    expect(routingJob.name).toBe(JOB_NAMES.routeServiceRequest);

    expect(routingJob.data).toEqual({
      organizationId,
      serviceRequestId: serviceRequest.id,
      correlationId: responseBody.requestId,
    });

    expect(await routingJob.getState()).toBe("waiting");

    const deliveredOutbox = await app.db.outboxEvent.findUniqueOrThrow({
      where: {
        id: outboxEvent.id,
      },
    });

    expect(deliveredOutbox.status).toBe("DELIVERED");

    expect(deliveredOutbox.attemptCount).toBe(2);
    expect(deliveredOutbox.processedAt).toEqual(recoveryNow);
    expect(deliveredOutbox.nextAttemptAt).toEqual(recoveryNow);
    expect(deliveredOutbox.lastError).toBeNull();

    const unchangedRequest = await app.db.serviceRequest.findUniqueOrThrow({
      where: {
        id: serviceRequest.id,
      },
    });

    expect(unchangedRequest.status).toBe("PENDING");
  }, 10_000);
});
