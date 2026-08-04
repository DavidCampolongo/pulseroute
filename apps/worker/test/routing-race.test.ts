import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { JOB_NAMES, type RouteServiceRequestJobData } from "@pulseroute/shared";
import { Queue, QueueEvents, Worker } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createRoutingProcessor } from "../src/routing-worker.js";
import { createWorkerLogger } from "../src/logger.js";
import {
  createProducerRedisOptions,
  createWorkerRedisOptions,
} from "../src/redis.js";
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

const fixtureDatabase = createDatabaseClient(databaseUrl);
const workerDatabase = createDatabaseClient(databaseUrl);
const blockerDatabase = createDatabaseClient(databaseUrl);

const queueName = `phase7-routing-race-${randomUUID()}`;

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

type RoutingFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
};

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
      database: workerDatabase,
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
      database: workerDatabase,
      logger,
    }),
    {
      connection: createWorkerRedisOptions(redisUrl),
      concurrency: 1,
    },
  );

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

  await routingQueue.close();

  await fixtureDatabase.$disconnect();
  await workerDatabase.$disconnect();
  await blockerDatabase.$disconnect();
});

describe("routing race", () => {
  it("keeps exactly one durable routing outcome when two workers compete for the same request", async () => {
    const fixture = await createRoutingFixture();

    try {
      let releaseBlocker: (() => void) | undefined;
      let blockerLocked: (() => void) | undefined;

      const blockerReleased = new Promise<void>((resolve) => {
        releaseBlocker = resolve;
      });

      const blockerHasLocked = new Promise<void>((resolve) => {
        blockerLocked = resolve;
      });

      const blocker = blockerDatabase.$transaction(async (tx) => {
        await tx.$queryRaw`
          SELECT id
          FROM service_requests
          WHERE id = ${fixture.serviceRequestId}::uuid
          FOR UPDATE
        `;

        blockerLocked?.();

        await blockerReleased;
      });

      await blockerHasLocked;

      const jobData: RouteServiceRequestJobData = {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `request-${randomUUID()}`,
      };

      const firstJob = await routingQueue.add(
        JOB_NAMES.routeServiceRequest,
        jobData,
        {
          jobId: `routing-race-a-${randomUUID()}`,
        },
      );

      const secondJob = await routingQueue.add(
        JOB_NAMES.routeServiceRequest,
        jobData,
        {
          jobId: `routing-race-b-${randomUUID()}`,
        },
      );

      releaseBlocker?.();

      const [firstResult, secondResult] = await Promise.all([
        firstJob.waitUntilFinished(routingQueueEvents, 5_000),
        secondJob.waitUntilFinished(routingQueueEvents, 5_000),
      ]);

      await blocker;

      expect([firstResult.kind, secondResult.kind].sort()).toEqual([
        "already_processed",
        "assigned",
      ]);

      const [
        assignmentCount,
        routingDecisionCount,
        serviceRequest,
        outboxCount,
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
      ]);

      expect(assignmentCount).toBe(1);
      expect(routingDecisionCount).toBe(1);
      expect(serviceRequest.status).toBe("ASSIGNED");
      expect(outboxCount).toBe(1);

      const assignment = await fixtureDatabase.assignment.findFirstOrThrow({
        where: {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
        },
      });

      expect(assignment.operatorId).toBe(fixture.operatorId);
    } finally {
      await clearRoutingFixture(fixture);
    }
  });
});
