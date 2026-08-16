import { createHmac, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  EVENT_TYPES,
  JOB_NAMES,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
  type WebhookDeliveryJobData,
} from "@pulseroute/shared";
import { type Job, type Queue } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { buildApp } from "../../api/src/app.js";
import {
  startFakeReceiver,
  type RunningFakeReceiver,
} from "@pulseroute/fake-receiver";
import { DeliveryScheduler } from "../src/delivery-scheduler.js";
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
import { createRoutingWorker } from "../src/routing-worker.js";
import { createWebhookDeliveryWorker } from "../src/webhook-delivery-worker.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const baseRedisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error(
    "DATABASE_URL is required for the Phase 9 webhook-delivery E2E test",
  );
}

if (!baseRedisUrl) {
  throw new Error(
    "REDIS_URL is required for the Phase 9 webhook-delivery E2E test",
  );
}

/*
 * PulseRoute uses fixed queue names. Logical database 9 keeps this complete
 * pipeline isolated from the older real-Redis worker tests and local queues.
 */
const testRedisUrl = new URL(baseRedisUrl);

testRedisUrl.pathname = "/9";

const redisUrl = testRedisUrl.toString();
const inboundWebhookSecret =
  "phase-9-inbound-webhook-secret-is-at-least-32-characters";
const outboundWebhookSecret =
  "phase-9-outbound-webhook-secret-is-at-least-32-characters";
const webhookToleranceSeconds = 300;
const inboundTimestampHeader = "x-pulseroute-timestamp";
const inboundSignatureHeader = "x-pulseroute-signature";
const finalScoringVersion = "pulseroute-scoring-v1";

const organizationId = randomUUID();
const requiredSkillId = randomUUID();
const selectedOperatorId = randomUUID();

/*
 * Fixed old clocks make only rows deliberately moved into the corresponding
 * window eligible. This prevents unrelated developer data from entering
 * either PostgreSQL scheduler cycle.
 */
const publisherNow = new Date("2000-01-01T00:00:00.000Z");
const publisherDueAt = new Date("1999-12-31T23:59:00.000Z");
const deliveryNow = new Date("2000-01-02T00:00:00.000Z");
const deliveryDueAt = new Date("2000-01-01T12:00:00.000Z");

const app = buildApp({
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3000,
  databaseUrl,
  logLevel: "silent",
  webhookSecret: inboundWebhookSecret,
  webhookToleranceSeconds,
});

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

let queues: PulseRouteQueues | undefined;
let incomingWorker: ReturnType<typeof createIncomingWorker> | undefined;
let routingWorker: ReturnType<typeof createRoutingWorker> | undefined;
let deliveryWorker: ReturnType<typeof createWebhookDeliveryWorker> | undefined;
let publisher: InternalOutboxPublisher | undefined;
let scheduler: DeliveryScheduler | undefined;
let receiver: RunningFakeReceiver | undefined;

type AcceptedInbound = {
  requestId: string;
  serviceRequestId: string;
};

type CompletedAssignmentPath = AcceptedInbound & {
  assignmentId: string;
  routingDecisionId: string;
  assignedOutboxEventId: string;
  assignedOutboxCreatedAt: Date;
  assignedPayload: unknown;
};

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitForJob<Data>(
  queue: Queue<Data>,
  jobId: string,
  timeoutMs = 10_000,
): Promise<Job<Data>> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const job = await queue.getJob(jobId);

    if (job) {
      return job;
    }

    await delay(25);
  }

  throw new Error(`Timed out waiting for BullMQ job ${jobId}`);
}

async function waitForCompletedJob<Data>(
  job: Job<Data>,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const state = await job.getState();

    if (state === "completed") {
      return;
    }

    if (state === "failed") {
      throw new Error(
        `BullMQ job ${job.id} failed: ${job.failedReason ?? "unknown failure"}`,
      );
    }

    await delay(25);
  }

  throw new Error(
    `Timed out waiting for BullMQ job ${job.id} to complete; final state was ${await job.getState()}`,
  );
}

async function waitForDeliveryHistory(
  outboxEventId: string,
  expectedStatus: "SUCCEEDED" | "FAILED",
  timeoutMs = 10_000,
) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const delivery = await app.db.webhookDelivery.findFirst({
      where: {
        organizationId,
        outboxEventId,
        status: expectedStatus,
      },
      orderBy: {
        attemptNumber: "desc",
      },
    });

    if (delivery) {
      return delivery;
    }

    await delay(25);
  }

  throw new Error(
    `Timed out waiting for ${expectedStatus} WebhookDelivery for ${outboxEventId}`,
  );
}

async function findDeliveryJob(
  outboxEventId: string,
): Promise<Job<WebhookDeliveryJobData> | undefined> {
  if (!queues) {
    throw new Error("Phase 9 E2E queues were not initialized");
  }

  const jobs = await queues.webhookDelivery.getJobs([
    "waiting",
    "active",
    "completed",
    "failed",
    "delayed",
  ]);

  return jobs.find((job) => job.data.outboxEventId === outboxEventId);
}

async function waitForDeliveryJob(
  outboxEventId: string,
  timeoutMs = 10_000,
): Promise<Job<WebhookDeliveryJobData>> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const job = await findDeliveryJob(outboxEventId);

    if (job) {
      return job;
    }

    await delay(25);
  }

  throw new Error(`Timed out waiting for delivery job for ${outboxEventId}`);
}

function createInboundBody(options: {
  externalEventId: string;
  externalRequestId: string;
}): string {
  return JSON.stringify({
    organizationId,
    eventId: options.externalEventId,
    type: EVENT_TYPES.serviceRequestCreated,
    data: {
      externalId: options.externalRequestId,
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  });
}

/* Independent of the production inbound signature helper. */
function createIndependentInboundHeaders(
  rawBody: string,
): Record<string, string> {
  const timestamp = String(Math.floor(Date.now() / 1_000));
  const signature = createHmac("sha256", inboundWebhookSecret)
    .update(timestamp, "utf8")
    .update(".", "utf8")
    .update(Buffer.from(rawBody, "utf8"))
    .digest("hex");

  return {
    "content-type": "application/json",
    [inboundTimestampHeader]: timestamp,
    [inboundSignatureHeader]: signature,
  };
}

async function ingestAndAssign(options: {
  externalEventId: string;
  externalRequestId: string;
}): Promise<CompletedAssignmentPath> {
  if (!queues || !publisher) {
    throw new Error("Phase 9 E2E ingestion infrastructure was not initialized");
  }

  const rawBody = createInboundBody(options);
  const response = await app.inject({
    method: "POST",
    url: "/webhooks/service-requests",
    headers: createIndependentInboundHeaders(rawBody),
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

  const ingestionOutbox = await app.db.outboxEvent.findFirstOrThrow({
    where: {
      organizationId,
      eventType: EVENT_TYPES.serviceRequestCreated,
      aggregateId: responseBody.serviceRequestId,
    },
  });

  await app.db.outboxEvent.update({
    where: {
      id: ingestionOutbox.id,
    },
    data: {
      nextAttemptAt: publisherDueAt,
    },
  });

  expect(await publisher.publishOnce()).toEqual({
    selected: 1,
    published: 1,
    failed: 0,
  });

  const incomingJob = await waitForJob<ServiceRequestIngestedJobData>(
    queues.incomingEvents,
    createIncomingJobId(ingestionOutbox.id),
  );

  expect(incomingJob.name).toBe(JOB_NAMES.serviceRequestIngested);
  await waitForCompletedJob(incomingJob);

  const routingJob = await waitForJob<RouteServiceRequestJobData>(
    queues.routing,
    createRoutingJobId(responseBody.serviceRequestId),
  );

  expect(routingJob.name).toBe(JOB_NAMES.routeServiceRequest);
  await waitForCompletedJob(routingJob);

  const assignment = await app.db.assignment.findFirstOrThrow({
    where: {
      organizationId,
      serviceRequestId: responseBody.serviceRequestId,
    },
  });

  const routingDecision = await app.db.routingDecision.findFirstOrThrow({
    where: {
      organizationId,
      serviceRequestId: responseBody.serviceRequestId,
    },
  });

  const assignedOutbox = await app.db.outboxEvent.findFirstOrThrow({
    where: {
      organizationId,
      aggregateId: responseBody.serviceRequestId,
      eventType: EVENT_TYPES.serviceRequestAssigned,
    },
  });

  expect(assignment).toMatchObject({
    operatorId: selectedOperatorId,
    status: "ACTIVE",
  });
  expect(routingDecision).toMatchObject({
    assignmentId: assignment.id,
    scoringVersion: finalScoringVersion,
    outcome: "ASSIGNED",
  });
  expect(assignedOutbox).toMatchObject({
    status: "PENDING",
    attemptCount: 0,
  });
  expect(assignedOutbox.payload).toMatchObject({
    serviceRequestId: responseBody.serviceRequestId,
    organizationId,
    operatorId: selectedOperatorId,
    assignmentId: assignment.id,
    routingDecisionId: routingDecision.id,
    scoringVersion: finalScoringVersion,
    correlationId: responseBody.requestId,
  });

  await app.db.outboxEvent.update({
    where: {
      id: assignedOutbox.id,
    },
    data: {
      nextAttemptAt: deliveryDueAt,
    },
  });

  return {
    requestId: responseBody.requestId,
    serviceRequestId: responseBody.serviceRequestId,
    assignmentId: assignment.id,
    routingDecisionId: routingDecision.id,
    assignedOutboxEventId: assignedOutbox.id,
    assignedOutboxCreatedAt: assignedOutbox.createdAt,
    assignedPayload: assignedOutbox.payload,
  };
}

async function clearDatabaseFixture(): Promise<void> {
  await app.db.webhookDelivery.deleteMany({
    where: {
      organizationId,
    },
  });

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

  await app.db.routingDecision.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.assignment.deleteMany({
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

  await app.db.operatorSkill.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.operator.deleteMany({
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
      name: "Phase 9 Webhook Delivery E2E Organization",
    },
  });

  await app.db.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Phase 9 Webhook Delivery E2E Skill",
    },
  });

  await app.db.operator.create({
    data: {
      id: selectedOperatorId,
      organizationId,
      name: "Phase 9 Eligible Operator",
      status: "AVAILABLE",
      region: "WEST",
      maxConcurrentAssignments: 5,
    },
  });

  await app.db.operatorSkill.create({
    data: {
      organizationId,
      operatorId: selectedOperatorId,
      skillId: requiredSkillId,
      level: 5,
    },
  });

  receiver = await startFakeReceiver({
    secret: outboundWebhookSecret,
    mode: "success",
  });

  queues = createPulseRouteQueues(redisUrl);

  await waitForPulseRouteQueues(queues);

  await Promise.all([
    queues.incomingEvents.obliterate({ force: true }),
    queues.routing.obliterate({ force: true }),
    queues.webhookDelivery.obliterate({ force: true }),
    queues.deadLetter.obliterate({ force: true }),
  ]);

  incomingWorker = createIncomingWorker({
    database: app.db,
    routingQueue: queues.routing,
    deadLetterQueue: queues.deadLetter,
    logger,
    redisUrl,
    concurrency: 1,
  });

  routingWorker = createRoutingWorker({
    database: app.db,
    deadLetterQueue: queues.deadLetter,
    logger,
    redisUrl,
    concurrency: 1,
  });

  deliveryWorker = createWebhookDeliveryWorker({
    database: app.db,
    logger,
    redisUrl,
    webhookUrl: receiver.url,
    webhookSecret: outboundWebhookSecret,
    timeoutMs: 1_000,
    maxAttempts: 3,
    baseDelayMs: 10_000,
    maxDelayMs: 10_000,
    now: () => new Date(deliveryNow),
    random: () => 0.5,
    concurrency: 1,
  });

  await Promise.all([
    incomingWorker.waitUntilReady(),
    routingWorker.waitUntilReady(),
    deliveryWorker.waitUntilReady(),
  ]);

  publisher = new InternalOutboxPublisher({
    database: app.db,
    incomingQueue: queues.incomingEvents,
    logger,
    pollIntervalMs: 100,
    batchSize: 10,
    now: () => new Date(publisherNow),
  });

  scheduler = new DeliveryScheduler({
    database: app.db,
    webhookDeliveryQueue: queues.webhookDelivery,
    deadLetterQueue: queues.deadLetter,
    logger,
    pollIntervalMs: 100,
    batchSize: 10,
    claimTimeoutMs: 30_000,
    maxAttempts: 3,
    now: () => new Date(deliveryNow),
  });
}, 20_000);

afterAll(async () => {
  await scheduler?.stop();
  await publisher?.stop();
  await incomingWorker?.close();
  await routingWorker?.close();
  await deliveryWorker?.close();

  if (queues) {
    await Promise.all([
      queues.incomingEvents.obliterate({ force: true }).catch(() => undefined),
      queues.routing.obliterate({ force: true }).catch(() => undefined),
      queues.webhookDelivery.obliterate({ force: true }).catch(() => undefined),
      queues.deadLetter.obliterate({ force: true }).catch(() => undefined),
    ]);

    await closePulseRouteQueues(queues);
  }

  await receiver?.close();
  await clearDatabaseFixture();
  await app.close();
}, 20_000);

describe("Phase 9 signed inbound webhook to durable outbound delivery", () => {
  it("delivers one actual Phase 8 assignment with exact-byte outbound HMAC and successful history", async () => {
    if (!scheduler || !receiver) {
      throw new Error(
        "Phase 9 E2E delivery infrastructure was not initialized",
      );
    }

    receiver.setMode("success");

    const completedPath = await ingestAndAssign({
      externalEventId: `phase-9-success-event-${randomUUID()}`,
      externalRequestId: `phase-9-success-request-${randomUUID()}`,
    });

    await scheduler.publishOnce();

    const deliveryJob = await waitForDeliveryJob(
      completedPath.assignedOutboxEventId,
    );

    expect(deliveryJob.name).toBe(JOB_NAMES.deliverWebhook);
    await waitForCompletedJob(deliveryJob);

    const delivery = await waitForDeliveryHistory(
      completedPath.assignedOutboxEventId,
      "SUCCEEDED",
    );
    const deliveredOutbox = await app.db.outboxEvent.findUniqueOrThrow({
      where: {
        id: completedPath.assignedOutboxEventId,
      },
    });

    expect(delivery).toMatchObject({
      attemptNumber: 1,
      status: "SUCCEEDED",
      httpStatus: 200,
      errorDetails: null,
    });
    expect(delivery.completedAt).not.toBeNull();
    expect(deliveredOutbox).toMatchObject({
      status: "DELIVERED",
      attemptCount: 1,
      processingStartedAt: null,
      lastError: null,
    });
    expect(deliveredOutbox.processedAt).not.toBeNull();

    const receivedRequests = receiver.getRequests();
    const outboundRequest = receivedRequests.find((request) => {
      if (!request.signatureAccepted) {
        return false;
      }

      const parsed = JSON.parse(request.rawBody.toString("utf8")) as {
        data?: { serviceRequestId?: string };
      };

      return parsed.data?.serviceRequestId === completedPath.serviceRequestId;
    });

    expect(outboundRequest).toBeDefined();

    if (!outboundRequest || outboundRequest.timestamp === null) {
      throw new Error("Expected the signed assignment request at the receiver");
    }

    expect(outboundRequest).toMatchObject({
      method: "POST",
      path: "/webhooks",
      signatureAccepted: true,
      mode: "success",
      statusCode: 200,
      outcome: "success",
    });
    const expectedEnvelope = {
      eventId: completedPath.assignedOutboxEventId,
      type: EVENT_TYPES.serviceRequestAssigned,
      createdAt: completedPath.assignedOutboxCreatedAt.toISOString(),
      data: {
        organizationId,
        serviceRequestId: completedPath.serviceRequestId,
        operatorId: selectedOperatorId,
        assignmentId: completedPath.assignmentId,
        routingDecisionId: completedPath.routingDecisionId,
        scoringVersion: finalScoringVersion,
        correlationId: completedPath.requestId,
      },
    };
    const expectedExactBody = Buffer.from(
      JSON.stringify(expectedEnvelope),
      "utf8",
    );

    expect(outboundRequest.rawBody.equals(expectedExactBody)).toBe(true);
    expect(JSON.parse(outboundRequest.rawBody.toString("utf8"))).toEqual(
      expectedEnvelope,
    );
    expect(outboundRequest.signatureAccepted).toBe(true);

    expect(
      await app.db.webhookDelivery.count({
        where: {
          organizationId,
          outboxEventId: completedPath.assignedOutboxEventId,
        },
      }),
    ).toBe(1);
  }, 20_000);

  it("keeps the assignment and decision committed when the receiver returns 500 and schedules a durable retry", async () => {
    if (!scheduler || !receiver) {
      throw new Error(
        "Phase 9 E2E delivery infrastructure was not initialized",
      );
    }

    receiver.setMode("failure");

    const completedPath = await ingestAndAssign({
      externalEventId: `phase-9-failure-event-${randomUUID()}`,
      externalRequestId: `phase-9-failure-request-${randomUUID()}`,
    });

    const committedAssignment = await app.db.assignment.findUniqueOrThrow({
      where: {
        id: completedPath.assignmentId,
      },
    });
    const committedDecision = await app.db.routingDecision.findUniqueOrThrow({
      where: {
        id: completedPath.routingDecisionId,
      },
    });

    await scheduler.publishOnce();

    const deliveryJob = await waitForDeliveryJob(
      completedPath.assignedOutboxEventId,
    );

    expect(deliveryJob.name).toBe(JOB_NAMES.deliverWebhook);
    await waitForCompletedJob(deliveryJob);

    const failedDelivery = await waitForDeliveryHistory(
      completedPath.assignedOutboxEventId,
      "FAILED",
    );
    const retryingOutbox = await app.db.outboxEvent.findUniqueOrThrow({
      where: {
        id: completedPath.assignedOutboxEventId,
      },
    });
    const preservedAssignment = await app.db.assignment.findUniqueOrThrow({
      where: {
        id: completedPath.assignmentId,
      },
    });
    const preservedDecision = await app.db.routingDecision.findUniqueOrThrow({
      where: {
        id: completedPath.routingDecisionId,
      },
    });
    const assignedRequest = await app.db.serviceRequest.findUniqueOrThrow({
      where: {
        id: completedPath.serviceRequestId,
      },
    });

    expect(failedDelivery).toMatchObject({
      attemptNumber: 1,
      status: "FAILED",
      httpStatus: 500,
    });
    expect(failedDelivery.completedAt).not.toBeNull();
    expect(failedDelivery.errorDetails).not.toBeNull();
    expect(retryingOutbox).toMatchObject({
      status: "PENDING",
      attemptCount: 1,
      processingStartedAt: null,
      processedAt: null,
    });
    expect(retryingOutbox.lastError).not.toBeNull();
    expect(retryingOutbox.nextAttemptAt.getTime()).toBeGreaterThan(
      deliveryNow.getTime(),
    );

    expect(preservedAssignment).toEqual(committedAssignment);
    expect(preservedDecision).toEqual(committedDecision);
    expect(assignedRequest.status).toBe("ASSIGNED");

    expect(
      await app.db.webhookDelivery.count({
        where: {
          organizationId,
          outboxEventId: completedPath.assignedOutboxEventId,
        },
      }),
    ).toBe(1);
  }, 20_000);
});
