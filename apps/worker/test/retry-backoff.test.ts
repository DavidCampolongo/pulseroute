import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { Queue, QueueEvents, Worker, type Processor } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  createIncomingProcessor,
  createRoutingJobId,
  type IncomingProcessorResult,
} from "../src/incoming-worker.js";
import { createWorkerLogger } from "../src/logger.js";
import {
  createProducerRedisOptions,
  createWorkerRedisOptions,
} from "../src/redis.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const redisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for retry/backoff tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for retry/backoff tests");
}

const RETRY_DELAY_MS = 150;

const database = createDatabaseClient(databaseUrl);

const incomingQueueName = `phase6-retry-incoming-${randomUUID()}`;

const routingQueueName = `phase6-retry-routing-${randomUUID()}`;

const incomingQueue = new Queue<ServiceRequestIngestedJobData>(
  incomingQueueName,
  {
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
  },
);

const routingQueue = new Queue<RouteServiceRequestJobData>(routingQueueName, {
  connection: createProducerRedisOptions(redisUrl),
  skipWaitingForReady: true,
  defaultJobOptions: {
    removeOnComplete: false,
    removeOnFail: false,
  },
});

const incomingQueueEvents = new QueueEvents(incomingQueueName, {
  connection: createWorkerRedisOptions(redisUrl),
});

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

type AttemptObservation = {
  attemptsMade: number;
  startedAt: number;
};

type FailureObservation = {
  message: string;
};

const attemptObservations: AttemptObservation[] = [];
const failureObservations: FailureObservation[] = [];

let worker:
  | Worker<ServiceRequestIngestedJobData, IncomingProcessorResult, string>
  | undefined;

const organizationId = randomUUID();
const requiredSkillId = randomUUID();
const serviceRequestId = randomUUID();

async function clearDatabaseFixture(): Promise<void> {
  await database.serviceRequest.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });
}

beforeAll(async () => {
  await Promise.all([
    incomingQueue.waitUntilReady(),
    routingQueue.waitUntilReady(),
    incomingQueueEvents.waitUntilReady(),
  ]);

  await incomingQueue.obliterate({
    force: true,
  });

  await routingQueue.obliterate({
    force: true,
  });

  await clearDatabaseFixture();

  await database.organization.create({
    data: {
      id: organizationId,
      name: "Phase 6 Retry Test Organization",
    },
  });

  await database.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Phase 6 Retry Test Skill",
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `retry-test-${randomUUID()}`,
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  });

  const incomingProcessor = createIncomingProcessor({
    database,
    routingQueue,
    logger,
  });

  const transientProcessor: Processor<
    ServiceRequestIngestedJobData,
    IncomingProcessorResult,
    string
  > = async (job, token) => {
    attemptObservations.push({
      attemptsMade: job.attemptsMade,
      startedAt: Date.now(),
    });

    if (job.attemptsMade < 2) {
      throw new Error(`Injected transient failure ${job.attemptsMade + 1}`);
    }

    return incomingProcessor(job, token);
  };

  worker = new Worker<
    ServiceRequestIngestedJobData,
    IncomingProcessorResult,
    string
  >(incomingQueueName, transientProcessor, {
    connection: createWorkerRedisOptions(redisUrl),
    concurrency: 1,
  });

  worker.on("failed", (_job, error) => {
    failureObservations.push({
      message: error.message,
    });
  });

  await worker.waitUntilReady();
});

afterAll(async () => {
  if (worker) {
    await worker.close();
  }

  await incomingQueueEvents.close();

  await incomingQueue
    .obliterate({
      force: true,
    })
    .catch(() => undefined);

  await routingQueue
    .obliterate({
      force: true,
    })
    .catch(() => undefined);

  await incomingQueue.close();
  await routingQueue.close();

  await clearDatabaseFixture();

  await database.$disconnect();
});

describe("incoming worker retry and backoff", () => {
  it("fails twice, backs off exponentially, and succeeds on the third attempt", async () => {
    const correlationId = `request-${randomUUID()}`;

    const incomingJob = await incomingQueue.add(
      JOB_NAMES.serviceRequestIngested,
      {
        outboxEventId: randomUUID(),
        organizationId,
        serviceRequestId,
        correlationId,
      },
      {
        jobId: `retry-${randomUUID()}`,
      },
    );

    const result = await incomingJob.waitUntilFinished(
      incomingQueueEvents,
      5_000,
    );

    const routingJobId = createRoutingJobId(serviceRequestId);

    expect(result).toEqual({
      routingJobId,
    });

    expect(
      attemptObservations.map((observation) => observation.attemptsMade),
    ).toEqual([0, 1, 2]);

    expect(failureObservations).toEqual([
      {
        message: "Injected transient failure 1",
      },
      {
        message: "Injected transient failure 2",
      },
    ]);

    const [firstAttempt, secondAttempt, thirdAttempt] = attemptObservations;

    expect(firstAttempt).toBeDefined();
    expect(secondAttempt).toBeDefined();
    expect(thirdAttempt).toBeDefined();

    if (!firstAttempt || !secondAttempt || !thirdAttempt) {
      throw new Error("Expected exactly three processor attempts");
    }

    const firstRetryDelay = secondAttempt.startedAt - firstAttempt.startedAt;

    const secondRetryDelay = thirdAttempt.startedAt - secondAttempt.startedAt;

    /*
     * Expected delays are approximately 150 ms and 300 ms.
     * Small tolerances account for timer and Redis scheduling precision.
     */
    expect(firstRetryDelay).toBeGreaterThanOrEqual(100);

    expect(secondRetryDelay).toBeGreaterThanOrEqual(250);

    const savedIncomingJob = await incomingQueue.getJob(incomingJob.id!);

    expect(savedIncomingJob).not.toBeNull();

    if (!savedIncomingJob) {
      throw new Error("Expected completed incoming job to remain available");
    }

    expect(await savedIncomingJob.getState()).toBe("completed");

    expect(savedIncomingJob.attemptsMade).toBe(3);
    expect(savedIncomingJob.attemptsStarted).toBe(3);

    const routingJob = await routingQueue.getJob(routingJobId);

    expect(routingJob).not.toBeNull();

    if (!routingJob) {
      throw new Error("Expected eventual success to create the routing job");
    }

    expect(routingJob.data).toEqual({
      organizationId,
      serviceRequestId,
      correlationId,
    });

    const unchangedRequest = await database.serviceRequest.findUniqueOrThrow({
      where: {
        id: serviceRequestId,
      },
    });

    expect(unchangedRequest.status).toBe("PENDING");
  }, 5_000);
});
