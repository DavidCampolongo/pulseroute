import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  QUEUE_NAMES,
  type DeadLetteredJobData,
  type RouteServiceRequestJobData,
} from "@pulseroute/shared";
import { Queue, QueueEvents, Worker } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createDeadLetteringProcessor } from "../src/dead-letter.js";
import { createWorkerLogger } from "../src/logger.js";
import {
  createProducerRedisOptions,
  createWorkerRedisOptions,
} from "../src/redis.js";
import { createRoutingProcessor } from "../src/routing-worker.js";
import type { RoutingAssignmentResult } from "../src/routing-workflow.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const redisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for routing race tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for routing race tests");
}

const testRunId = randomUUID();

const workerAApplicationName = `pr-request-race-a-${testRunId}`;
const workerBApplicationName = `pr-request-race-b-${testRunId}`;

function addApplicationName(
  connectionUrl: string,
  applicationName: string,
): string {
  const url = new URL(connectionUrl);

  url.searchParams.set("application_name", applicationName);

  return url.toString();
}

const fixtureDatabase = createDatabaseClient(databaseUrl);
const blockerDatabase = createDatabaseClient(databaseUrl);

const workerADatabase = createDatabaseClient(
  addApplicationName(databaseUrl, workerAApplicationName),
);

const workerBDatabase = createDatabaseClient(
  addApplicationName(databaseUrl, workerBApplicationName),
);

const routingQueueName = `phase7-routing-race-${testRunId}`;
const deadLetterQueueName = `phase7-routing-race-dlq-${testRunId}`;

const routingQueue = new Queue<RouteServiceRequestJobData>(routingQueueName, {
  connection: createProducerRedisOptions(redisUrl),
  skipWaitingForReady: true,
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: true,
    removeOnFail: false,
  },
});

const routingQueueEvents = new QueueEvents(routingQueueName, {
  connection: createWorkerRedisOptions(redisUrl),
});

const deadLetterQueue = new Queue<DeadLetteredJobData>(deadLetterQueueName, {
  connection: createProducerRedisOptions(redisUrl),
  skipWaitingForReady: true,
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  },
});

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

let workerA:
  | Worker<RouteServiceRequestJobData, RoutingAssignmentResult, string>
  | undefined;

let workerB:
  | Worker<RouteServiceRequestJobData, RoutingAssignmentResult, string>
  | undefined;

type RoutingFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
};

type BlockedWorkerRow = {
  applicationName: string;
};

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitForBothWorkersToBlockOnRequestLock(): Promise<void> {
  const deadline = Date.now() + 3_000;

  while (Date.now() < deadline) {
    const blockedRows = await fixtureDatabase.$queryRaw<BlockedWorkerRow[]>`
      SELECT
        application_name AS "applicationName"
      FROM pg_stat_activity
      WHERE wait_event_type = 'Lock'
        AND (
          application_name = ${workerAApplicationName}
          OR application_name = ${workerBApplicationName}
        )
    `;

    const blockedApplicationNames = new Set(
      blockedRows.map((row) => row.applicationName),
    );

    if (
      blockedApplicationNames.has(workerAApplicationName) &&
      blockedApplicationNames.has(workerBApplicationName)
    ) {
      return;
    }

    await delay(10);
  }

  throw new Error(
    "Both routing workers did not reach the blocked ServiceRequest-lock state",
  );
}

async function countDeadLetterJobs(): Promise<number> {
  const counts = await deadLetterQueue.getJobCounts(
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
  );

  return Object.values(counts).reduce((total, count) => total + count, 0);
}

async function createRoutingFixture(): Promise<RoutingFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await fixtureDatabase.organization.create({
    data: {
      id: organizationId,
      name: `Routing Race Test Org ${organizationId}`,
    },
  });

  await fixtureDatabase.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Race Test Skill ${skillId}`,
    },
  });

  await fixtureDatabase.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Routing Race Test Operator ${operatorId}`,
      status: "AVAILABLE",
      region: "WEST",
      maxConcurrentAssignments: 2,
    },
  });

  await fixtureDatabase.operatorSkill.create({
    data: {
      organizationId,
      operatorId,
      skillId,
      level: 4,
    },
  });

  await fixtureDatabase.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `routing-race-${serviceRequestId}`,
      requiredSkillId: skillId,
      status: "PENDING",
      priority: "NORMAL",
      region: "WEST",
    },
  });

  return {
    organizationId,
    skillId,
    operatorId,
    serviceRequestId,
  };
}

async function clearRoutingFixture(fixture: RoutingFixture): Promise<void> {
  await fixtureDatabase.webhookDelivery.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.outboxEvent.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.routingDecision.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.assignment.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.serviceRequest.deleteMany({
    where: {
      id: fixture.serviceRequestId,
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.operatorSkill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
      operatorId: fixture.operatorId,
      skillId: fixture.skillId,
    },
  });

  await fixtureDatabase.operator.deleteMany({
    where: {
      id: fixture.operatorId,
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.skill.deleteMany({
    where: {
      id: fixture.skillId,
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.organization.deleteMany({
    where: {
      id: fixture.organizationId,
    },
  });
}

beforeAll(async () => {
  await Promise.all([
    routingQueue.waitUntilReady(),
    routingQueueEvents.waitUntilReady(),
    deadLetterQueue.waitUntilReady(),
  ]);

  await Promise.all([
    routingQueue.obliterate({
      force: true,
    }),
    deadLetterQueue.obliterate({
      force: true,
    }),
  ]);

  const workerAProcessor = createDeadLetteringProcessor({
    sourceQueue: QUEUE_NAMES.routing,
    deadLetterQueue,
    processor: createRoutingProcessor({
      database: workerADatabase,
      logger,
    }),
    logger,
  });

  const workerBProcessor = createDeadLetteringProcessor({
    sourceQueue: QUEUE_NAMES.routing,
    deadLetterQueue,
    processor: createRoutingProcessor({
      database: workerBDatabase,
      logger,
    }),
    logger,
  });

  workerA = new Worker<
    RouteServiceRequestJobData,
    RoutingAssignmentResult,
    string
  >(routingQueueName, workerAProcessor, {
    connection: createWorkerRedisOptions(redisUrl),
    concurrency: 1,
  });

  workerB = new Worker<
    RouteServiceRequestJobData,
    RoutingAssignmentResult,
    string
  >(routingQueueName, workerBProcessor, {
    connection: createWorkerRedisOptions(redisUrl),
    concurrency: 1,
  });

  workerA.on("error", (error) => {
    logger.error({ err: error }, "Routing worker A error");
  });

  workerB.on("error", (error) => {
    logger.error({ err: error }, "Routing worker B error");
  });

  await Promise.all([workerA.waitUntilReady(), workerB.waitUntilReady()]);
});

afterAll(async () => {
  if (workerA) {
    await workerA.close();
  }

  if (workerB) {
    await workerB.close();
  }

  await routingQueueEvents.close();

  await routingQueue
    .obliterate({
      force: true,
    })
    .catch(() => undefined);

  await deadLetterQueue
    .obliterate({
      force: true,
    })
    .catch(() => undefined);

  await routingQueue.close();
  await deadLetterQueue.close();

  await fixtureDatabase.$disconnect();
  await blockerDatabase.$disconnect();
  await workerADatabase.$disconnect();
  await workerBDatabase.$disconnect();
});

describe("routing race", () => {
  it("keeps exactly one durable routing outcome after both workers contend for the same request lock", async () => {
    const fixture = await createRoutingFixture();

    let releaseBlocker = (): void => undefined;
    let reportBlockerLocked = (): void => undefined;

    const blockerReleased = new Promise<void>((resolve) => {
      releaseBlocker = resolve;
    });

    const blockerHasLockedRequest = new Promise<void>((resolve) => {
      reportBlockerLocked = resolve;
    });

    const blockerTransaction = blockerDatabase.$transaction(
      async (transaction) => {
        await transaction.$queryRaw`
          SELECT id
          FROM service_requests
          WHERE id = ${fixture.serviceRequestId}::uuid
          FOR UPDATE
        `;

        reportBlockerLocked();

        await blockerReleased;
      },
      {
        timeout: 10_000,
      },
    );

    await blockerHasLockedRequest;

    try {
      const firstJobData: RouteServiceRequestJobData = {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `routing-race-a-${randomUUID()}`,
      };

      const secondJobData: RouteServiceRequestJobData = {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `routing-race-b-${randomUUID()}`,
      };

      const firstJob = await routingQueue.add(
        JOB_NAMES.routeServiceRequest,
        firstJobData,
        {
          jobId: `routing-race-a-${randomUUID()}`,
        },
      );

      const secondJob = await routingQueue.add(
        JOB_NAMES.routeServiceRequest,
        secondJobData,
        {
          jobId: `routing-race-b-${randomUUID()}`,
        },
      );

      await waitForBothWorkersToBlockOnRequestLock();

      releaseBlocker();

      const [firstResult, secondResult] = await Promise.all([
        firstJob.waitUntilFinished(routingQueueEvents, 5_000),
        secondJob.waitUntilFinished(routingQueueEvents, 5_000),
      ]);

      await blockerTransaction;

      expect([firstResult.kind, secondResult.kind].sort()).toEqual([
        "already_processed",
        "assigned",
      ]);

      const [
        assignmentCount,
        routingDecisionCount,
        serviceRequest,
        outboxCount,
        deadLetterJobCount,
      ] = await Promise.all([
        fixtureDatabase.assignment.count({
          where: {
            organizationId: fixture.organizationId,
            serviceRequestId: fixture.serviceRequestId,
            status: "ACTIVE",
          },
        }),
        fixtureDatabase.routingDecision.count({
          where: {
            organizationId: fixture.organizationId,
            serviceRequestId: fixture.serviceRequestId,
          },
        }),
        fixtureDatabase.serviceRequest.findUniqueOrThrow({
          where: {
            id: fixture.serviceRequestId,
          },
        }),
        fixtureDatabase.outboxEvent.count({
          where: {
            organizationId: fixture.organizationId,
            aggregateId: fixture.serviceRequestId,
            eventType: "service_request.assigned",
          },
        }),
        countDeadLetterJobs(),
      ]);

      expect(assignmentCount).toBe(1);
      expect(routingDecisionCount).toBe(1);
      expect(serviceRequest.status).toBe("ASSIGNED");
      expect(outboxCount).toBe(1);
      expect(deadLetterJobCount).toBe(0);

      const assignment = await fixtureDatabase.assignment.findFirstOrThrow({
        where: {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
        },
      });

      expect(assignment.operatorId).toBe(fixture.operatorId);
    } finally {
      releaseBlocker();

      await blockerTransaction.catch(() => undefined);

      await clearRoutingFixture(fixture);
    }
  }, 10_000);
});
