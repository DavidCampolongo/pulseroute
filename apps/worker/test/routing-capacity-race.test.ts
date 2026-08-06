import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { JOB_NAMES, type RouteServiceRequestJobData } from "@pulseroute/shared";
import { Queue, QueueEvents, Worker } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

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
  throw new Error("DATABASE_URL is required for routing capacity race tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for routing capacity race tests");
}

const testRunId = randomUUID();

const workerAApplicationName = `pr-capacity-a-${testRunId}`;
const workerBApplicationName = `pr-capacity-b-${testRunId}`;

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

const queueName = `phase7-capacity-race-${testRunId}`;

const routingQueue = new Queue<RouteServiceRequestJobData>(queueName, {
  connection: createProducerRedisOptions(redisUrl),
  skipWaitingForReady: true,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: false,
  },
});

const routingQueueEvents = new QueueEvents(queueName, {
  connection: createWorkerRedisOptions(redisUrl),
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

type CapacityRaceFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  firstServiceRequestId: string;
  secondServiceRequestId: string;
};

type BlockedWorkerRow = {
  applicationName: string;
};

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function createCapacityRaceFixture(): Promise<CapacityRaceFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const firstServiceRequestId = randomUUID();
  const secondServiceRequestId = randomUUID();

  await fixtureDatabase.organization.create({
    data: {
      id: organizationId,
      name: `Capacity Race Test Org ${organizationId}`,
    },
  });

  await fixtureDatabase.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Capacity Race Test Skill ${skillId}`,
    },
  });

  await fixtureDatabase.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Capacity Race Test Operator ${operatorId}`,
      status: "AVAILABLE",
      region: "WEST",
      maxConcurrentAssignments: 1,
    },
  });

  await fixtureDatabase.operatorSkill.create({
    data: {
      organizationId,
      operatorId,
      skillId,
      level: 5,
    },
  });

  await fixtureDatabase.serviceRequest.create({
    data: {
      id: firstServiceRequestId,
      organizationId,
      externalId: `capacity-race-a-${firstServiceRequestId}`,
      requiredSkillId: skillId,
      status: "PENDING",
      priority: "NORMAL",
      region: "WEST",
    },
  });

  await fixtureDatabase.serviceRequest.create({
    data: {
      id: secondServiceRequestId,
      organizationId,
      externalId: `capacity-race-b-${secondServiceRequestId}`,
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
    firstServiceRequestId,
    secondServiceRequestId,
  };
}

async function clearCapacityRaceFixture(
  fixture: CapacityRaceFixture,
): Promise<void> {
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
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.operatorSkill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.operator.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.skill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.organization.deleteMany({
    where: {
      id: fixture.organizationId,
    },
  });
}

async function waitForBothWorkersToBlockOnDatabaseLock(): Promise<void> {
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
    "Both routing workers did not reach the blocked Operator-lock state",
  );
}

beforeAll(async () => {
  await Promise.all([
    routingQueue.waitUntilReady(),
    routingQueueEvents.waitUntilReady(),
  ]);

  await routingQueue.obliterate({
    force: true,
  });

  workerA = new Worker<
    RouteServiceRequestJobData,
    RoutingAssignmentResult,
    string
  >(
    queueName,
    createRoutingProcessor({
      database: workerADatabase,
      logger,
    }),
    {
      connection: createWorkerRedisOptions(redisUrl),
      concurrency: 1,
    },
  );

  workerB = new Worker<
    RouteServiceRequestJobData,
    RoutingAssignmentResult,
    string
  >(
    queueName,
    createRoutingProcessor({
      database: workerBDatabase,
      logger,
    }),
    {
      connection: createWorkerRedisOptions(redisUrl),
      concurrency: 1,
    },
  );

  workerA.on("error", (error) => {
    logger.error(
      {
        err: error,
      },
      "Routing capacity worker A error",
    );
  });

  workerB.on("error", (error) => {
    logger.error(
      {
        err: error,
      },
      "Routing capacity worker B error",
    );
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

  await routingQueue.close();

  await fixtureDatabase.$disconnect();
  await blockerDatabase.$disconnect();
  await workerADatabase.$disconnect();
  await workerBDatabase.$disconnect();
});

describe("routing Operator-capacity race", () => {
  it("does not oversubscribe a capacity-one Operator when two requests race", async () => {
    const fixture = await createCapacityRaceFixture();

    let releaseBlocker = (): void => undefined;
    let reportBlockerLocked = (): void => undefined;

    const blockerReleased = new Promise<void>((resolve) => {
      releaseBlocker = resolve;
    });

    const blockerHasLockedOperator = new Promise<void>((resolve) => {
      reportBlockerLocked = resolve;
    });

    const blockerTransaction = blockerDatabase.$transaction(
      async (transaction) => {
        await transaction.$queryRaw`
            SELECT id
            FROM operators
            WHERE id = ${fixture.operatorId}::uuid
            FOR UPDATE
          `;

        reportBlockerLocked();

        await blockerReleased;
      },
      {
        timeout: 10_000,
      },
    );

    await blockerHasLockedOperator;

    try {
      const firstJobData: RouteServiceRequestJobData = {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.firstServiceRequestId,
        correlationId: `capacity-race-a-${randomUUID()}`,
      };

      const secondJobData: RouteServiceRequestJobData = {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.secondServiceRequestId,
        correlationId: `capacity-race-b-${randomUUID()}`,
      };

      const firstJob = await routingQueue.add(
        JOB_NAMES.routeServiceRequest,
        firstJobData,
        {
          jobId: `capacity-race-a-${randomUUID()}`,
        },
      );

      const secondJob = await routingQueue.add(
        JOB_NAMES.routeServiceRequest,
        secondJobData,
        {
          jobId: `capacity-race-b-${randomUUID()}`,
        },
      );

      await waitForBothWorkersToBlockOnDatabaseLock();

      releaseBlocker();

      const [firstResult, secondResult] = await Promise.all([
        firstJob.waitUntilFinished(routingQueueEvents, 5_000),
        secondJob.waitUntilFinished(routingQueueEvents, 5_000),
      ]);

      await blockerTransaction;

      const routingResults = [firstResult, secondResult];

      expect(routingResults.map((result) => result.kind).sort()).toEqual([
        "assigned",
        "unroutable",
      ]);

      const unroutableResult = routingResults.find(
        (result) => result.kind === "unroutable",
      );

      expect(unroutableResult).toBeDefined();

      if (!unroutableResult || unroutableResult.kind !== "unroutable") {
        throw new Error("Expected one request to become unroutable");
      }

      expect(unroutableResult.rejectionReasons).toContain("AT_CAPACITY");

      const serviceRequestIds = [
        fixture.firstServiceRequestId,
        fixture.secondServiceRequestId,
      ];

      const [
        activeAssignments,
        serviceRequests,
        routingDecisions,
        assignmentOutboxEvents,
      ] = await Promise.all([
        fixtureDatabase.assignment.findMany({
          where: {
            organizationId: fixture.organizationId,
            operatorId: fixture.operatorId,
            status: "ACTIVE",
          },
          select: {
            id: true,
            operatorId: true,
            serviceRequestId: true,
            status: true,
          },
        }),
        fixtureDatabase.serviceRequest.findMany({
          where: {
            organizationId: fixture.organizationId,
            id: {
              in: serviceRequestIds,
            },
          },
          select: {
            id: true,
            status: true,
          },
        }),
        fixtureDatabase.routingDecision.findMany({
          where: {
            organizationId: fixture.organizationId,
            serviceRequestId: {
              in: serviceRequestIds,
            },
          },
          select: {
            serviceRequestId: true,
            outcome: true,
          },
        }),
        fixtureDatabase.outboxEvent.findMany({
          where: {
            organizationId: fixture.organizationId,
            aggregateId: {
              in: serviceRequestIds,
            },
            eventType: "service_request.assigned",
          },
          select: {
            aggregateId: true,
            eventType: true,
            status: true,
          },
        }),
      ]);

      expect(activeAssignments).toHaveLength(1);

      expect(activeAssignments[0]).toMatchObject({
        operatorId: fixture.operatorId,
        status: "ACTIVE",
      });

      expect(serviceRequests).toHaveLength(2);

      expect(serviceRequests.map((request) => request.status).sort()).toEqual([
        "ASSIGNED",
        "UNROUTABLE",
      ]);

      expect(routingDecisions).toHaveLength(2);

      expect(
        routingDecisions.map((decision) => decision.outcome).sort(),
      ).toEqual(["ASSIGNED", "UNROUTABLE"]);

      expect(assignmentOutboxEvents).toHaveLength(1);

      expect(assignmentOutboxEvents[0]).toMatchObject({
        eventType: "service_request.assigned",
        status: "PENDING",
      });

      const assignedRequest = serviceRequests.find(
        (request) => request.status === "ASSIGNED",
      );

      const unroutableRequest = serviceRequests.find(
        (request) => request.status === "UNROUTABLE",
      );

      expect(assignedRequest).toBeDefined();
      expect(unroutableRequest).toBeDefined();

      expect(activeAssignments[0]?.serviceRequestId).toBe(assignedRequest?.id);

      expect(assignmentOutboxEvents[0]?.aggregateId).toBe(assignedRequest?.id);

      expect(unroutableRequest?.id).not.toBe(
        activeAssignments[0]?.serviceRequestId,
      );
    } finally {
      releaseBlocker();

      await blockerTransaction.catch(() => undefined);

      await clearCapacityRaceFixture(fixture);
    }
  }, 10_000);
});
