import { createHmac, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  EVENT_TYPES,
  JOB_NAMES,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { type Job, type Queue } from "bullmq";
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

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const baseRedisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error(
    "DATABASE_URL is required for ingestion-worker end-to-end tests",
  );
}

if (!baseRedisUrl) {
  throw new Error(
    "REDIS_URL is required for ingestion-worker end-to-end tests",
  );
}

/*
 * Use a separate logical Redis database so this fixed-name production queue
 * test cannot collide with the other worker test files running in parallel.
 */
const testRedisUrl = new URL(baseRedisUrl);

testRedisUrl.pathname = "/15";

const redisUrl = testRedisUrl.toString();

const webhookSecret = "phase6-end-to-end-webhook-secret-at-least-32-characters";

const webhookToleranceSeconds = 300;

const webhookTimestampHeader = "x-pulseroute-timestamp";

const webhookSignatureHeader = "x-pulseroute-signature";

const organizationId = randomUUID();
const requiredSkillId = randomUUID();

const externalEventId = `phase6-e2e-event-${randomUUID()}`;

const externalRequestId = `phase6-e2e-request-${randomUUID()}`;

/*
 * The publisher normally evaluates events against the current time.
 *
 * This isolated test clock prevents the publisher from selecting unrelated
 * PENDING rows that may already exist in a developer's local database.
 * Only this test's outbox row is moved into this clock's eligible window.
 */
const testPublisherNow = new Date("2000-01-01T00:00:00.000Z");

const testEventDueAt = new Date("1999-12-31T23:59:00.000Z");

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

let queues: PulseRouteQueues | undefined;

let incomingWorker: ReturnType<typeof createIncomingWorker> | undefined;

let publisher: InternalOutboxPublisher | undefined;

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

async function waitForJob<Data>(
  queue: Queue<Data>,
  jobId: string,
  timeoutMs = 5_000,
): Promise<Job<Data>> {
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

async function waitForJobState<Data>(
  job: Job<Data>,
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

beforeAll(async () => {
  await app.ready();

  await clearDatabaseFixture();

  await app.db.organization.create({
    data: {
      id: organizationId,
      name: "Phase 6 End-to-End Test Organization",
    },
  });

  await app.db.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Phase 6 End-to-End Test Skill",
    },
  });

  queues = createPulseRouteQueues(redisUrl);

  await waitForPulseRouteQueues(queues);

  await queues.incomingEvents.obliterate({
    force: true,
  });

  await queues.routing.obliterate({
    force: true,
  });

  incomingWorker = createIncomingWorker({
    database: app.db,
    routingQueue: queues.routing,
    logger,
    redisUrl,
    concurrency: 1,
  });

  incomingWorker.on("error", (error) => {
    logger.error(
      {
        err: error,
      },
      "End-to-end incoming worker error",
    );
  });

  await incomingWorker.waitUntilReady();

  publisher = new InternalOutboxPublisher({
    database: app.db,
    incomingQueue: queues.incomingEvents,
    logger,
    pollIntervalMs: 100,
    batchSize: 10,
    now: () => testPublisherNow,
  });
});

afterAll(async () => {
  if (publisher) {
    await publisher.stop();
  }

  if (incomingWorker) {
    await incomingWorker.close();
  }

  if (queues) {
    await queues.incomingEvents
      .obliterate({
        force: true,
      })
      .catch(() => undefined);

    await queues.routing
      .obliterate({
        force: true,
      })
      .catch(() => undefined);

    await closePulseRouteQueues(queues);
  }

  await clearDatabaseFixture();

  await app.close();
});

describe("signed webhook to worker integration", () => {
  it("persists, publishes, consumes, and prepares routing work", async () => {
    if (!queues || !publisher) {
      throw new Error("End-to-end worker infrastructure was not initialized");
    }

    const rawBody = createWebhookBody();

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
      id: responseBody.serviceRequestId,
      organizationId,
      externalId: externalRequestId,
      requiredSkillId,
      status: "PENDING",
      priority: "HIGH",
      region: "WEST",
    });

    const webhookEvent = await app.db.webhookEvent.findFirstOrThrow({
      where: {
        organizationId,
        provider: "pulseroute",
        externalEventId,
      },
    });

    expect(webhookEvent).toMatchObject({
      status: "PROCESSED",
      serviceRequestId: serviceRequest.id,
    });

    const outboxEvent = await app.db.outboxEvent.findFirstOrThrow({
      where: {
        organizationId,
        eventType: EVENT_TYPES.serviceRequestCreated,
        aggregateType: "service_request",
        aggregateId: serviceRequest.id,
      },
    });

    expect(outboxEvent.status).toBe("PENDING");

    expect(outboxEvent.payload).toMatchObject({
      serviceRequestId: serviceRequest.id,
      correlationId: responseBody.requestId,
    });

    const auditLog = await app.db.auditLog.findFirstOrThrow({
      where: {
        organizationId,
        action: "service_request.ingested",
        entityType: "service_request",
        entityId: serviceRequest.id,
      },
    });

    expect(auditLog.correlationId).toBe(responseBody.requestId);

    /*
     * Move only this test row into the isolated publisher clock's eligible
     * window. Existing local PENDING events remain untouched.
     */
    await app.db.outboxEvent.update({
      where: {
        id: outboxEvent.id,
      },
      data: {
        nextAttemptAt: testEventDueAt,
      },
    });

    const publicationResult = await publisher.publishOnce();

    expect(publicationResult).toEqual({
      selected: 1,
      published: 1,
      failed: 0,
    });

    const incomingJobId = createIncomingJobId(outboxEvent.id);

    const incomingJob = await waitForJob<ServiceRequestIngestedJobData>(
      queues.incomingEvents,
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
      queues.routing,
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

    expect(deliveredOutbox.attemptCount).toBe(1);
    expect(deliveredOutbox.processedAt).not.toBeNull();
    expect(deliveredOutbox.lastError).toBeNull();

    const unchangedServiceRequest =
      await app.db.serviceRequest.findUniqueOrThrow({
        where: {
          id: serviceRequest.id,
        },
      });

    expect(unchangedServiceRequest.status).toBe("PENDING");
  });
});
