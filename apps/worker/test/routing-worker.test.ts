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
  throw new Error("DATABASE_URL is required for routing worker tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for routing worker tests");
}

const database = createDatabaseClient(databaseUrl);

const routingQueueName = `phase7-routing-worker-${randomUUID()}`;
const routingQueue = new Queue<RouteServiceRequestJobData>(routingQueueName, {
  connection: createProducerRedisOptions(redisUrl),
  skipWaitingForReady: true,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: false,
  },
});

const routingQueueEvents = new QueueEvents(routingQueueName, {
  connection: createWorkerRedisOptions(redisUrl),
});

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

let worker:
  | Worker<RouteServiceRequestJobData, RoutingAssignmentResult, string>
  | undefined;

async function createRoutingFixture(): Promise<{
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
}> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Routing Worker Test Org ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Worker Test Skill ${skillId}`,
    },
  });

  await database.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Routing Worker Test Operator ${operatorId}`,
      status: "AVAILABLE",
      region: "WEST",
      maxConcurrentAssignments: 2,
    },
  });

  await database.operatorSkill.create({
    data: {
      organizationId,
      operatorId,
      skillId,
      level: 4,
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `routing-worker-${serviceRequestId}`,
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

async function clearRoutingFixture(fixture: {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
}): Promise<void> {
  await database.routingDecision.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.outboxEvent.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.assignment.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.serviceRequest.deleteMany({
    where: {
      id: fixture.serviceRequestId,
      organizationId: fixture.organizationId,
    },
  });

  await database.operatorSkill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
      operatorId: fixture.operatorId,
      skillId: fixture.skillId,
    },
  });

  await database.operator.deleteMany({
    where: {
      id: fixture.operatorId,
      organizationId: fixture.organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      id: fixture.skillId,
      organizationId: fixture.organizationId,
    },
  });

  await database.organization.deleteMany({
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

  worker = new Worker<
    RouteServiceRequestJobData,
    RoutingAssignmentResult,
    string
  >(
    routingQueueName,
    createRoutingProcessor({
      database,
      logger,
    }),
    {
      connection: createWorkerRedisOptions(redisUrl),
      concurrency: 1,
    },
  );

  worker.on("error", (error) => {
    logger.error(
      {
        err: error,
      },
      "Routing worker error",
    );
  });

  await worker.waitUntilReady();
});

afterAll(async () => {
  if (worker) {
    await worker.close();
  }

  await routingQueueEvents.close();

  await routingQueue
    .obliterate({
      force: true,
    })
    .catch(() => undefined);

  await routingQueue.close();

  await database.$disconnect();
});

describe("routing worker processor", () => {
  it("consumes a routing job and creates the assignment transactionally", async () => {
    const fixture = await createRoutingFixture();

    try {
      const jobData: RouteServiceRequestJobData = {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `request-${randomUUID()}`,
      };

      const job = await routingQueue.add(
        JOB_NAMES.routeServiceRequest,
        jobData,
        {
          jobId: `routing-${randomUUID()}`,
        },
      );

      const result = await job.waitUntilFinished(routingQueueEvents, 5_000);

      expect(result.kind).toBe("assigned");

      const assignment = await database.assignment.findFirstOrThrow({
        where: {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
        },
      });

      const routingDecision = await database.routingDecision.findFirstOrThrow({
        where: {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
        },
      });

      const serviceRequest = await database.serviceRequest.findUniqueOrThrow({
        where: {
          id: fixture.serviceRequestId,
        },
      });

      expect(assignment.operatorId).toBe(fixture.operatorId);
      expect(routingDecision.outcome).toBe("ASSIGNED");
      expect(serviceRequest.status).toBe("ASSIGNED");
    } finally {
      await clearRoutingFixture(fixture);
    }
  });
});
