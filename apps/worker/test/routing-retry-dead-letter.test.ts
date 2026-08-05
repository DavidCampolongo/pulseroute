import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  QUEUE_NAMES,
  type DeadLetteredJobData,
  type RouteServiceRequestJobData,
} from "@pulseroute/shared";
import { type Job, type Processor, Queue, QueueEvents, Worker } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  createDeadLetteringProcessor,
  createDeadLetterJobId,
} from "../src/dead-letter.js";
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
  throw new Error(
    "DATABASE_URL is required for routing retry/dead-letter tests",
  );
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for routing retry/dead-letter tests");
}

const RETRY_DELAY_MS = 50;

const database = createDatabaseClient(databaseUrl);

const routingQueueName = `phase7-routing-retry-${randomUUID()}`;
const deadLetterQueueName = `phase7-routing-dlq-${randomUUID()}`;

const routingQueue = new Queue<RouteServiceRequestJobData>(routingQueueName, {
  connection: createProducerRedisOptions(redisUrl),
  skipWaitingForReady: true,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: RETRY_DELAY_MS,
    },
    removeOnComplete: false,
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

type FaultMode = "none" | "transient" | "persistent";

type RoutingFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
};

type DurableRoutingState = {
  serviceRequestStatus: string;
  assignmentCount: number;
  routingDecisionCount: number;
  outboxEventCount: number;
};

const faultModes = new Map<string, FaultMode>();
const attemptObservations = new Map<string, number[]>();

let worker:
  | Worker<RouteServiceRequestJobData, RoutingAssignmentResult, string>
  | undefined;

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

  throw new Error(`Timed out waiting for job ${jobId}`);
}

async function removeQueueJob<DataType>(
  queue: Queue<DataType>,
  jobId: string,
): Promise<void> {
  const job = await queue.getJob(jobId);

  if (job) {
    await job.remove().catch(() => undefined);
  }
}

async function countQueueJobs(
  queue: Queue<DeadLetteredJobData>,
): Promise<number> {
  const counts = await queue.getJobCounts(
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
  );

  return Object.values(counts).reduce((total, count) => total + count, 0);
}

async function createRoutingFixture(options?: {
  atCapacity?: boolean;
}): Promise<RoutingFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Routing Retry Test Org ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Retry Test Skill ${skillId}`,
    },
  });

  await database.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Routing Retry Test Operator ${operatorId}`,
      status: "AVAILABLE",
      region: "WEST",
      maxConcurrentAssignments: options?.atCapacity ? 1 : 2,
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
      externalId: `routing-retry-target-${serviceRequestId}`,
      requiredSkillId: skillId,
      status: "PENDING",
      priority: "NORMAL",
      region: "WEST",
    },
  });

  if (options?.atCapacity) {
    const activeRequestId = randomUUID();

    await database.serviceRequest.create({
      data: {
        id: activeRequestId,
        organizationId,
        externalId: `routing-retry-active-${activeRequestId}`,
        requiredSkillId: skillId,
        status: "ASSIGNED",
        priority: "NORMAL",
        region: "WEST",
      },
    });

    await database.assignment.create({
      data: {
        id: randomUUID(),
        organizationId,
        serviceRequestId: activeRequestId,
        operatorId,
        status: "ACTIVE",
      },
    });
  }

  return {
    organizationId,
    skillId,
    operatorId,
    serviceRequestId,
  };
}

async function clearRoutingFixture(fixture: RoutingFixture): Promise<void> {
  await database.webhookDelivery.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.auditLog.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.outboxEvent.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.routingDecision.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.assignment.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.webhookEvent.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.serviceRequest.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.operatorSkill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.operator.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.user.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.organization.deleteMany({
    where: {
      id: fixture.organizationId,
    },
  });
}

async function readDurableRoutingState(
  fixture: RoutingFixture,
): Promise<DurableRoutingState> {
  const [
    serviceRequest,
    assignmentCount,
    routingDecisionCount,
    outboxEventCount,
  ] = await Promise.all([
    database.serviceRequest.findUniqueOrThrow({
      where: {
        id: fixture.serviceRequestId,
      },
    }),

    database.assignment.count({
      where: {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      },
    }),

    database.routingDecision.count({
      where: {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      },
    }),

    database.outboxEvent.count({
      where: {
        organizationId: fixture.organizationId,
        aggregateId: fixture.serviceRequestId,
        eventType: "service_request.assigned",
      },
    }),
  ]);

  return {
    serviceRequestStatus: serviceRequest.status,
    assignmentCount,
    routingDecisionCount,
    outboxEventCount,
  };
}

async function addRoutingJob(options: {
  fixture: RoutingFixture;
  sourceJobId: string;
  correlationId: string;
}) {
  return routingQueue.add(
    JOB_NAMES.routeServiceRequest,
    {
      organizationId: options.fixture.organizationId,
      serviceRequestId: options.fixture.serviceRequestId,
      correlationId: options.correlationId,
    },
    {
      jobId: options.sourceJobId,
    },
  );
}

beforeAll(async () => {
  await Promise.all([
    routingQueue.waitUntilReady(),
    routingQueueEvents.waitUntilReady(),
    deadLetterQueue.waitUntilReady(),
  ]);

  await routingQueue.obliterate({
    force: true,
  });

  await deadLetterQueue.obliterate({
    force: true,
  });

  const routingProcessor = createRoutingProcessor({
    database,
    logger,
  });

  const faultInjectingProcessor: Processor<
    RouteServiceRequestJobData,
    RoutingAssignmentResult,
    string
  > = async (job, token, signal) => {
    const correlationId = job.data.correlationId;

    const observations = attemptObservations.get(correlationId) ?? [];

    observations.push(job.attemptsMade);

    attemptObservations.set(correlationId, observations);

    const faultMode = faultModes.get(correlationId) ?? "none";

    if (faultMode === "transient" && job.attemptsMade < 2) {
      throw new Error(
        `Injected transient routing failure ${job.attemptsMade + 1}`,
      );
    }

    if (faultMode === "persistent") {
      throw new Error("Injected persistent routing failure");
    }

    return routingProcessor(job, token, signal);
  };

  const processor = createDeadLetteringProcessor({
    sourceQueue: QUEUE_NAMES.routing,
    deadLetterQueue,
    processor: faultInjectingProcessor,
    logger,
  });

  worker = new Worker<
    RouteServiceRequestJobData,
    RoutingAssignmentResult,
    string
  >(routingQueueName, processor, {
    connection: createWorkerRedisOptions(redisUrl),
    concurrency: 1,
  });

  worker.on("error", (error) => {
    logger.error(
      {
        err: error,
      },
      "Routing retry/dead-letter worker error",
    );
  });

  routingQueueEvents.on("error", (error) => {
    logger.error(
      {
        err: error,
      },
      "Routing retry/dead-letter QueueEvents error",
    );
  });

  await worker.waitUntilReady();
}, 15_000);

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

  await deadLetterQueue
    .obliterate({
      force: true,
    })
    .catch(() => undefined);

  await Promise.all([
    routingQueue.close(),
    deadLetterQueue.close(),
    database.$disconnect(),
  ]);
}, 15_000);

describe("routing retry and dead-letter behavior", () => {
  it("retries a transient routing failure and eventually assigns", async () => {
    const fixture = await createRoutingFixture();

    const sourceJobId = `routing-transient-${randomUUID()}`;

    const correlationId = `request-transient-${randomUUID()}`;

    const deadLetterJobId = createDeadLetterJobId(
      QUEUE_NAMES.routing,
      sourceJobId,
    );

    faultModes.set(correlationId, "transient");

    try {
      const sourceJob = await addRoutingJob({
        fixture,
        sourceJobId,
        correlationId,
      });

      const result = await sourceJob.waitUntilFinished(
        routingQueueEvents,
        5_000,
      );

      expect(result.kind).toBe("assigned");

      expect(attemptObservations.get(correlationId)).toEqual([0, 1, 2]);

      const savedSourceJob = await routingQueue.getJob(sourceJobId);

      expect(savedSourceJob).not.toBeNull();

      if (!savedSourceJob) {
        throw new Error(
          "Expected the completed routing job to remain available",
        );
      }

      expect(await savedSourceJob.getState()).toBe("completed");

      expect(savedSourceJob.attemptsMade).toBe(3);
      expect(savedSourceJob.attemptsStarted).toBe(3);

      expect(await deadLetterQueue.getJob(deadLetterJobId)).toBeNull();

      expect(await readDurableRoutingState(fixture)).toEqual({
        serviceRequestStatus: "ASSIGNED",
        assignmentCount: 1,
        routingDecisionCount: 1,
        outboxEventCount: 1,
      });
    } finally {
      faultModes.delete(correlationId);
      attemptObservations.delete(correlationId);

      await removeQueueJob(routingQueue, sourceJobId);

      await removeQueueJob(deadLetterQueue, deadLetterJobId);

      await clearRoutingFixture(fixture);
    }
  }, 10_000);

  it("dead-letters a persistent unexpected routing failure", async () => {
    const fixture = await createRoutingFixture();

    const sourceJobId = `routing-persistent-${randomUUID()}`;

    const correlationId = `request-persistent-${randomUUID()}`;

    const deadLetterJobId = createDeadLetterJobId(
      QUEUE_NAMES.routing,
      sourceJobId,
    );

    faultModes.set(correlationId, "persistent");

    try {
      const sourceJob = await addRoutingJob({
        fixture,
        sourceJobId,
        correlationId,
      });

      await expect(
        sourceJob.waitUntilFinished(routingQueueEvents, 5_000),
      ).rejects.toThrow("Injected persistent routing failure");

      expect(attemptObservations.get(correlationId)).toEqual([0, 1, 2]);

      const savedSourceJob = await routingQueue.getJob(sourceJobId);

      expect(savedSourceJob).not.toBeNull();

      if (!savedSourceJob) {
        throw new Error("Expected the failed routing job to remain available");
      }

      expect(await savedSourceJob.getState()).toBe("failed");

      expect(savedSourceJob.attemptsMade).toBe(3);
      expect(savedSourceJob.attemptsStarted).toBe(3);

      expect(savedSourceJob.failedReason).toBe(
        "Injected persistent routing failure",
      );

      const deadLetterJob = await waitForJob<DeadLetteredJobData>(
        deadLetterQueue,
        deadLetterJobId,
      );

      expect(deadLetterJob.name).toBe(JOB_NAMES.deadLetteredJob);

      expect(deadLetterJob.data).toMatchObject({
        sourceQueue: QUEUE_NAMES.routing,
        sourceJobId,
        sourceJobName: JOB_NAMES.routeServiceRequest,
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId,
        attemptsMade: 3,
        failureReason: "Injected persistent routing failure",
      });

      expect(await deadLetterJob.getState()).toBe("waiting");

      expect(await countQueueJobs(deadLetterQueue)).toBe(1);

      expect(await readDurableRoutingState(fixture)).toEqual({
        serviceRequestStatus: "PENDING",
        assignmentCount: 0,
        routingDecisionCount: 0,
        outboxEventCount: 0,
      });
    } finally {
      faultModes.delete(correlationId);
      attemptObservations.delete(correlationId);

      await removeQueueJob(routingQueue, sourceJobId);

      await removeQueueJob(deadLetterQueue, deadLetterJobId);

      await clearRoutingFixture(fixture);
    }
  }, 10_000);

  it("completes an unroutable result without retrying or dead-lettering", async () => {
    const fixture = await createRoutingFixture({
      atCapacity: true,
    });

    const sourceJobId = `routing-unroutable-${randomUUID()}`;

    const correlationId = `request-unroutable-${randomUUID()}`;

    const deadLetterJobId = createDeadLetterJobId(
      QUEUE_NAMES.routing,
      sourceJobId,
    );

    try {
      const sourceJob = await addRoutingJob({
        fixture,
        sourceJobId,
        correlationId,
      });

      const result = await sourceJob.waitUntilFinished(
        routingQueueEvents,
        5_000,
      );

      expect(result.kind).toBe("unroutable");

      expect(attemptObservations.get(correlationId)).toEqual([0]);

      const savedSourceJob = await routingQueue.getJob(sourceJobId);

      expect(savedSourceJob).not.toBeNull();

      if (!savedSourceJob) {
        throw new Error("Expected the unroutable job to remain available");
      }

      expect(await savedSourceJob.getState()).toBe("completed");

      expect(savedSourceJob.attemptsMade).toBe(1);
      expect(savedSourceJob.attemptsStarted).toBe(1);

      expect(await deadLetterQueue.getJob(deadLetterJobId)).toBeNull();

      expect(await readDurableRoutingState(fixture)).toEqual({
        serviceRequestStatus: "UNROUTABLE",
        assignmentCount: 0,
        routingDecisionCount: 1,
        outboxEventCount: 0,
      });
    } finally {
      attemptObservations.delete(correlationId);

      await removeQueueJob(routingQueue, sourceJobId);

      await removeQueueJob(deadLetterQueue, deadLetterJobId);

      await clearRoutingFixture(fixture);
    }
  }, 10_000);

  it("completes an assigned replay without retrying or dead-lettering", async () => {
    const fixture = await createRoutingFixture();

    const initialSourceJobId = `routing-initial-${randomUUID()}`;

    const replaySourceJobId = `routing-replay-${randomUUID()}`;

    const correlationId = `request-replay-${randomUUID()}`;

    const initialDeadLetterJobId = createDeadLetterJobId(
      QUEUE_NAMES.routing,
      initialSourceJobId,
    );

    const replayDeadLetterJobId = createDeadLetterJobId(
      QUEUE_NAMES.routing,
      replaySourceJobId,
    );

    try {
      const initialJob = await addRoutingJob({
        fixture,
        sourceJobId: initialSourceJobId,
        correlationId,
      });

      const initialResult = await initialJob.waitUntilFinished(
        routingQueueEvents,
        5_000,
      );

      expect(initialResult.kind).toBe("assigned");

      const replayJob = await addRoutingJob({
        fixture,
        sourceJobId: replaySourceJobId,
        correlationId,
      });

      const replayResult = await replayJob.waitUntilFinished(
        routingQueueEvents,
        5_000,
      );

      expect(replayResult).toEqual({
        kind: "already_processed",
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        terminalStatus: "ASSIGNED",
      });

      expect(attemptObservations.get(correlationId)).toEqual([0, 0]);

      const savedReplayJob = await routingQueue.getJob(replaySourceJobId);

      expect(savedReplayJob).not.toBeNull();

      if (!savedReplayJob) {
        throw new Error("Expected the replay job to remain available");
      }

      expect(await savedReplayJob.getState()).toBe("completed");

      expect(savedReplayJob.attemptsMade).toBe(1);
      expect(savedReplayJob.attemptsStarted).toBe(1);

      expect(await deadLetterQueue.getJob(initialDeadLetterJobId)).toBeNull();

      expect(await deadLetterQueue.getJob(replayDeadLetterJobId)).toBeNull();

      expect(await readDurableRoutingState(fixture)).toEqual({
        serviceRequestStatus: "ASSIGNED",
        assignmentCount: 1,
        routingDecisionCount: 1,
        outboxEventCount: 1,
      });
    } finally {
      attemptObservations.delete(correlationId);

      await removeQueueJob(routingQueue, initialSourceJobId);

      await removeQueueJob(routingQueue, replaySourceJobId);

      await removeQueueJob(deadLetterQueue, initialDeadLetterJobId);

      await removeQueueJob(deadLetterQueue, replayDeadLetterJobId);

      await clearRoutingFixture(fixture);
    }
  }, 10_000);
});
