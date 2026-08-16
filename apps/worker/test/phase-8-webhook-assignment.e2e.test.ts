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
import { createRoutingWorker } from "../src/routing-worker.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const baseRedisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error(
    "DATABASE_URL is required for the Phase 8 webhook-assignment E2E test",
  );
}

if (!baseRedisUrl) {
  throw new Error(
    "REDIS_URL is required for the Phase 8 webhook-assignment E2E test",
  );
}

/*
 * PulseRoute's queues intentionally use fixed production names. A dedicated
 * logical Redis database keeps this E2E isolated from the older ingestion E2E
 * and from developer queues while still exercising the real queue topology.
 */
const testRedisUrl = new URL(baseRedisUrl);

testRedisUrl.pathname = "/14";

const redisUrl = testRedisUrl.toString();

const webhookSecret =
  "phase-8-webhook-assignment-secret-is-at-least-32-characters";
const webhookToleranceSeconds = 300;
const webhookTimestampHeader = "x-pulseroute-timestamp";
const webhookSignatureHeader = "x-pulseroute-signature";
const finalScoringVersion = "pulseroute-scoring-v1";

const organizationId = randomUUID();
const requiredSkillId = randomUUID();
const selectedOperatorId = randomUUID();
const rejectedOperatorId = randomUUID();
const externalEventId = `phase-8-e2e-event-${randomUUID()}`;
const externalRequestId = `phase-8-e2e-request-${randomUUID()}`;

/*
 * Only the ingestion event moved into this old clock can be selected. This
 * prevents unrelated PENDING rows in a developer database from entering the
 * real publisher cycle used by the test.
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
let routingWorker: ReturnType<typeof createRoutingWorker> | undefined;
let publisher: InternalOutboxPublisher | undefined;

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

/*
 * This deliberately does not import the production signature helper. The E2E
 * creates the protocol HMAC directly with Node crypto as an independent oracle.
 */
function createIndependentSignedHeaders(
  rawBody: string,
): Record<string, string> {
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

async function countDeadLetterJobs(): Promise<number> {
  if (!queues) {
    throw new Error("Phase 8 E2E queues were not initialized");
  }

  const counts = await queues.deadLetter.getJobCounts(
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
  );

  return Object.values(counts).reduce((total, count) => total + count, 0);
}

function readRecord(
  value: unknown,
  description: string,
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`Expected ${description} to be an object`);
  }

  return value as Record<string, unknown>;
}

function readArray(value: unknown, description: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`Expected ${description} to be an array`);
  }

  return value;
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
      name: "Phase 8 Webhook Assignment E2E Organization",
    },
  });

  await app.db.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Phase 8 Webhook Assignment E2E Skill",
    },
  });

  await app.db.operator.createMany({
    data: [
      {
        id: selectedOperatorId,
        organizationId,
        name: "Phase 8 Eligible Operator",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 2,
      },
      {
        id: rejectedOperatorId,
        organizationId,
        name: "Phase 8 Rejected Operator",
        status: "UNAVAILABLE",
        region: "EAST",
        maxConcurrentAssignments: 1,
      },
    ],
  });

  await app.db.operatorSkill.create({
    data: {
      organizationId,
      operatorId: selectedOperatorId,
      skillId: requiredSkillId,
      level: 5,
    },
  });

  queues = createPulseRouteQueues(redisUrl);

  await waitForPulseRouteQueues(queues);

  await Promise.all([
    queues.incomingEvents.obliterate({ force: true }),
    queues.routing.obliterate({ force: true }),
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

  incomingWorker.on("error", (error) => {
    logger.error({ err: error }, "Phase 8 E2E incoming worker error");
  });

  routingWorker = createRoutingWorker({
    database: app.db,
    deadLetterQueue: queues.deadLetter,
    logger,
    redisUrl,
    concurrency: 1,
  });

  routingWorker.on("error", (error) => {
    logger.error({ err: error }, "Phase 8 E2E routing worker error");
  });

  await Promise.all([
    incomingWorker.waitUntilReady(),
    routingWorker.waitUntilReady(),
  ]);

  publisher = new InternalOutboxPublisher({
    database: app.db,
    incomingQueue: queues.incomingEvents,
    logger,
    pollIntervalMs: 100,
    batchSize: 10,
    now: () => testPublisherNow,
  });
}, 15_000);

afterAll(async () => {
  await publisher?.stop();
  await incomingWorker?.close();
  await routingWorker?.close();

  if (queues) {
    await Promise.all([
      queues.incomingEvents.obliterate({ force: true }).catch(() => undefined),
      queues.routing.obliterate({ force: true }).catch(() => undefined),
      queues.deadLetter.obliterate({ force: true }).catch(() => undefined),
    ]);

    await closePulseRouteQueues(queues);
  }

  await clearDatabaseFixture();
  await app.close();
}, 15_000);

describe("Phase 8 signed webhook to explainable assignment", () => {
  it("runs the real ingestion, outbox, BullMQ, routing, and scoring path", async () => {
    if (!queues || !publisher) {
      throw new Error("Phase 8 E2E infrastructure was not initialized");
    }

    const rawBody = createWebhookBody();
    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: createIndependentSignedHeaders(rawBody),
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

    expect(
      await app.db.serviceRequest.count({
        where: {
          organizationId,
        },
      }),
    ).toBe(1);

    const ingestedRequest = await app.db.serviceRequest.findUniqueOrThrow({
      where: {
        id: responseBody.serviceRequestId,
      },
    });

    expect(ingestedRequest).toMatchObject({
      organizationId,
      externalId: externalRequestId,
      requiredSkillId,
      status: "PENDING",
      priority: "HIGH",
      region: "WEST",
    });

    expect(
      await app.db.webhookEvent.count({
        where: {
          organizationId,
          serviceRequestId: ingestedRequest.id,
          status: "PROCESSED",
        },
      }),
    ).toBe(1);

    const acceptedWebhookEvent = await app.db.webhookEvent.findFirstOrThrow({
      where: {
        organizationId,
        serviceRequestId: ingestedRequest.id,
        provider: "pulseroute",
        externalEventId,
      },
    });

    expect(acceptedWebhookEvent.status).toBe("PROCESSED");

    const ingestionOutbox = await app.db.outboxEvent.findFirstOrThrow({
      where: {
        organizationId,
        eventType: EVENT_TYPES.serviceRequestCreated,
        aggregateType: "service_request",
        aggregateId: ingestedRequest.id,
      },
    });

    expect(ingestionOutbox.status).toBe("PENDING");
    expect(ingestionOutbox.payload).toMatchObject({
      serviceRequestId: ingestedRequest.id,
      correlationId: responseBody.requestId,
    });

    await app.db.outboxEvent.update({
      where: {
        id: ingestionOutbox.id,
      },
      data: {
        nextAttemptAt: testEventDueAt,
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

    const routingJobId = createRoutingJobId(ingestedRequest.id);
    const routingJob = await waitForJob<RouteServiceRequestJobData>(
      queues.routing,
      routingJobId,
    );

    expect(routingJob.name).toBe(JOB_NAMES.routeServiceRequest);
    await waitForCompletedJob(routingJob);

    const completedRoutingJob = await queues.routing.getJob(routingJobId);

    expect(completedRoutingJob?.returnvalue).toMatchObject({
      kind: "assigned",
      organizationId,
      serviceRequestId: ingestedRequest.id,
      operatorId: selectedOperatorId,
      scoringVersion: finalScoringVersion,
    });

    const deliveredIngestionOutbox = await app.db.outboxEvent.findUniqueOrThrow(
      {
        where: {
          id: ingestionOutbox.id,
        },
      },
    );

    expect(deliveredIngestionOutbox).toMatchObject({
      status: "DELIVERED",
      attemptCount: 1,
      lastError: null,
    });
    expect(deliveredIngestionOutbox.processedAt).not.toBeNull();

    const assignments = await app.db.assignment.findMany({
      where: {
        organizationId,
        serviceRequestId: ingestedRequest.id,
      },
    });

    expect(assignments).toHaveLength(1);
    expect(assignments[0]).toMatchObject({
      operatorId: selectedOperatorId,
      status: "ACTIVE",
    });

    const routingDecisions = await app.db.routingDecision.findMany({
      where: {
        organizationId,
        serviceRequestId: ingestedRequest.id,
      },
    });

    expect(routingDecisions).toHaveLength(1);

    const routingDecision = routingDecisions[0];

    expect(routingDecision).toMatchObject({
      assignmentId: assignments[0]?.id,
      scoringVersion: finalScoringVersion,
      outcome: "ASSIGNED",
    });

    if (!routingDecision) {
      throw new Error("Expected the assigned RoutingDecision");
    }

    const snapshot = readRecord(
      routingDecision.decisionSnapshot,
      "RoutingDecision snapshot",
    );
    const scoring = readRecord(snapshot.scoring, "snapshot scoring result");
    const rankedCandidates = readArray(
      scoring.rankedEligibleCandidates,
      "ranked candidates",
    );
    const rejectedCandidates = readArray(
      scoring.rejectedCandidates,
      "rejected candidates",
    );

    expect(snapshot).toMatchObject({
      scoringVersion: finalScoringVersion,
      evaluatedAt: expect.any(String),
      result: {
        outcome: "ASSIGNED",
        selectedOperatorId,
      },
    });

    expect(scoring).toMatchObject({
      initialSelectedOperatorId: selectedOperatorId,
      weightProfile: {
        profileCode: "HIGH_PRIORITY",
        requestPriority: "HIGH",
      },
    });

    expect(rankedCandidates).toHaveLength(1);

    const selectedCandidate = readRecord(
      rankedCandidates[0],
      "selected ranked candidate",
    );
    const factors = readArray(
      selectedCandidate.factors,
      "selected candidate factors",
    );

    expect(selectedCandidate).toMatchObject({
      operatorId: selectedOperatorId,
      rank: 1,
      totalScore: expect.any(Number),
      observedFacts: expect.any(Object),
    });

    expect(factors).toHaveLength(4);

    for (const factor of factors) {
      expect(factor).toMatchObject({
        factorCode: expect.any(String),
        normalizedValue: expect.any(Number),
        weight: expect.any(Number),
        contribution: expect.any(Number),
      });

      expect(readRecord(factor, "factor breakdown")).toHaveProperty("rawValue");
    }

    expect(rejectedCandidates).toHaveLength(1);
    expect(rejectedCandidates[0]).toMatchObject({
      operatorId: rejectedOperatorId,
      reasons: [
        "OPERATOR_NOT_AVAILABLE",
        "REGION_INCOMPATIBLE",
        "MISSING_REQUIRED_SKILL",
      ],
      observedFacts: {
        status: "UNAVAILABLE",
        region: "EAST",
        requiredSkillLevel: null,
      },
    });

    const assignedRequest = await app.db.serviceRequest.findUniqueOrThrow({
      where: {
        id: ingestedRequest.id,
      },
    });

    expect(assignedRequest.status).toBe("ASSIGNED");

    const assignmentNotifications = await app.db.outboxEvent.findMany({
      where: {
        organizationId,
        eventType: "service_request.assigned",
        aggregateType: "service_request",
        aggregateId: ingestedRequest.id,
      },
    });

    expect(assignmentNotifications).toHaveLength(1);
    expect(assignmentNotifications[0]).toMatchObject({
      status: "PENDING",
      attemptCount: 0,
    });
    expect(assignmentNotifications[0]?.payload).toMatchObject({
      serviceRequestId: ingestedRequest.id,
      organizationId,
      operatorId: selectedOperatorId,
      assignmentId: assignments[0]?.id,
      routingDecisionId: routingDecision.id,
      scoringVersion: finalScoringVersion,
      correlationId: responseBody.requestId,
    });

    expect(await countDeadLetterJobs()).toBe(0);
  }, 20_000);
});
