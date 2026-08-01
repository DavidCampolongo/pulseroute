import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  QUEUE_NAMES,
  type DeadLetteredJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { QueueEvents, type Job, type Queue } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createDeadLetterJobId } from "../src/dead-letter.js";
import { createIncomingWorker } from "../src/incoming-worker.js";
import { createWorkerLogger } from "../src/logger.js";
import {
  closePulseRouteQueues,
  createPulseRouteQueues,
  waitForPulseRouteQueues,
} from "../src/queues.js";
import { createWorkerRedisOptions } from "../src/redis.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const baseRedisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for dead-letter tests");
}

if (!baseRedisUrl) {
  throw new Error("REDIS_URL is required for dead-letter tests");
}

/*
 * createIncomingWorker uses the fixed production queue name.
 * A separate logical Redis database isolates this suite from other tests.
 */
const testRedisUrl = new URL(baseRedisUrl);

testRedisUrl.pathname = "/14";

const redisUrl = testRedisUrl.toString();

const database = createDatabaseClient(databaseUrl);

const queues = createPulseRouteQueues(redisUrl);

const incomingQueueEvents = new QueueEvents(QUEUE_NAMES.incomingEvents, {
  connection: createWorkerRedisOptions(redisUrl),
});

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

let incomingWorker: ReturnType<typeof createIncomingWorker> | undefined;

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

async function countQueueJobs(queue: Queue<unknown>): Promise<number> {
  const counts = await queue.getJobCounts(
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
  );

  return Object.values(counts).reduce((total, count) => total + count, 0);
}

beforeAll(async () => {
  await Promise.all([
    waitForPulseRouteQueues(queues),

    incomingQueueEvents.waitUntilReady(),
  ]);

  await queues.incomingEvents.obliterate({
    force: true,
  });

  await queues.routing.obliterate({
    force: true,
  });

  await queues.deadLetter.obliterate({
    force: true,
  });

  incomingWorker = createIncomingWorker({
    database,
    routingQueue: queues.routing,
    deadLetterQueue: queues.deadLetter,
    logger,
    redisUrl,
    concurrency: 1,
  });

  incomingWorker.on("error", (error) => {
    logger.error(
      {
        err: error,
      },
      "Dead-letter test worker error",
    );
  });

  incomingQueueEvents.on("error", (error) => {
    logger.error(
      {
        err: error,
      },
      "Dead-letter test queue-events error",
    );
  });

  await incomingWorker.waitUntilReady();
});

afterAll(async () => {
  if (incomingWorker) {
    await incomingWorker.close();
  }

  await incomingQueueEvents.close();

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

  await queues.deadLetter
    .obliterate({
      force: true,
    })
    .catch(() => undefined);

  await closePulseRouteQueues(queues);

  await database.$disconnect();
});

describe("explicit dead-letter escalation", () => {
  it("creates one safe dead-letter job after all source attempts fail", async () => {
    const sourceJobId = `poison-${randomUUID()}`;

    const organizationId = randomUUID();
    const serviceRequestId = randomUUID();

    const correlationId = `request-${randomUUID()}`;

    const sourceData: ServiceRequestIngestedJobData = {
      outboxEventId: randomUUID(),
      organizationId,
      serviceRequestId,
      correlationId,
    };

    const startedAt = Date.now();

    const sourceJob = await queues.incomingEvents.add(
      JOB_NAMES.serviceRequestIngested,
      sourceData,
      {
        jobId: sourceJobId,

        attempts: 3,

        backoff: {
          type: "exponential",
          delay: 100,
        },

        removeOnFail: false,
      },
    );

    await expect(
      sourceJob.waitUntilFinished(incomingQueueEvents, 5_000),
    ).rejects.toThrow("ServiceRequest does not exist for the incoming job");

    const completedAt = Date.now();

    const savedSourceJob = await queues.incomingEvents.getJob(sourceJobId);

    expect(savedSourceJob).not.toBeNull();

    if (!savedSourceJob) {
      throw new Error("Expected failed source job to remain available");
    }

    expect(await savedSourceJob.getState()).toBe("failed");

    expect(savedSourceJob.attemptsMade).toBe(3);

    expect(savedSourceJob.attemptsStarted).toBe(3);

    expect(savedSourceJob.failedReason).toBe(
      "ServiceRequest does not exist for the incoming job",
    );

    const deadLetterJobId = createDeadLetterJobId(
      QUEUE_NAMES.incomingEvents,
      sourceJobId,
    );

    const deadLetterJob = await waitForJob<DeadLetteredJobData>(
      queues.deadLetter,
      deadLetterJobId,
    );

    expect(deadLetterJob.name).toBe(JOB_NAMES.deadLetteredJob);

    expect(deadLetterJob.data).toMatchObject({
      sourceQueue: QUEUE_NAMES.incomingEvents,

      sourceJobId,

      sourceJobName: JOB_NAMES.serviceRequestIngested,

      organizationId,

      serviceRequestId,

      correlationId,

      attemptsMade: 3,

      failureReason: "ServiceRequest does not exist for the incoming job",
    });

    expect(deadLetterJob.data).not.toHaveProperty("outboxEventId");

    expect(await deadLetterJob.getState()).toBe("waiting");

    const failedAt = Date.parse(deadLetterJob.data.failedAt);

    expect(Number.isNaN(failedAt)).toBe(false);

    expect(failedAt).toBeGreaterThanOrEqual(startedAt);

    expect(failedAt).toBeLessThanOrEqual(completedAt);

    expect(await countQueueJobs(queues.deadLetter)).toBe(1);

    expect(await countQueueJobs(queues.routing)).toBe(0);
  }, 5_000);
});
