import { randomUUID } from "node:crypto";
import { PassThrough } from "node:stream";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { Queue, QueueEvents, Worker } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

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
  throw new Error("DATABASE_URL is required for incoming worker tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for incoming worker tests");
}

const database = createDatabaseClient(databaseUrl);

const incomingQueueName = `phase6-incoming-processor-${randomUUID()}`;

const routingQueueName = `phase6-routing-processor-${randomUUID()}`;

const incomingQueue = new Queue<ServiceRequestIngestedJobData>(
  incomingQueueName,
  {
    connection: createProducerRedisOptions(redisUrl),
    skipWaitingForReady: true,
    defaultJobOptions: {
      attempts: 1,
      removeOnComplete: false,
      removeOnFail: false,
    },
  },
);

const routingQueue = new Queue<RouteServiceRequestJobData>(routingQueueName, {
  connection: createProducerRedisOptions(redisUrl),
  skipWaitingForReady: true,
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  },
});

const incomingQueueEvents = new QueueEvents(incomingQueueName, {
  connection: createWorkerRedisOptions(redisUrl),
});

const logStream = new PassThrough();
let logOutput = "";

logStream.on("data", (chunk: Buffer) => {
  logOutput += chunk.toString();
});

const logger = createWorkerLogger(
  {
    nodeEnv: "test",
    logLevel: "info",
  },
  logStream,
);

const incomingWorker = new Worker<
  ServiceRequestIngestedJobData,
  IncomingProcessorResult,
  string
>(
  incomingQueueName,
  createIncomingProcessor({
    database,
    routingQueue,
    logger,
  }),
  {
    connection: createWorkerRedisOptions(redisUrl),
    concurrency: 2,
  },
);

const createdOrganizationIds: string[] = [];

type ServiceRequestFixture = {
  organizationId: string;
  serviceRequestId: string;
};

async function createServiceRequestFixture(): Promise<ServiceRequestFixture> {
  const organizationId = randomUUID();
  const requiredSkillId = randomUUID();
  const serviceRequestId = randomUUID();

  createdOrganizationIds.push(organizationId);

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Incoming Worker Test ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: `Incoming Worker Skill ${requiredSkillId}`,
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `incoming-worker-${randomUUID()}`,
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  });

  return {
    organizationId,
    serviceRequestId,
  };
}

async function clearDatabaseFixtures(): Promise<void> {
  const organizationIds = createdOrganizationIds.splice(
    0,
    createdOrganizationIds.length,
  );

  if (organizationIds.length === 0) {
    return;
  }

  await database.serviceRequest.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.organization.deleteMany({
    where: {
      id: {
        in: organizationIds,
      },
    },
  });
}

function readLogEntries(): Record<string, unknown>[] {
  return logOutput
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

async function countRoutingJobs(): Promise<number> {
  const counts = await routingQueue.getJobCounts(
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
    incomingQueue.waitUntilReady(),
    routingQueue.waitUntilReady(),
    incomingQueueEvents.waitUntilReady(),
    incomingWorker.waitUntilReady(),
  ]);

  await incomingQueue.obliterate({
    force: true,
  });

  await routingQueue.obliterate({
    force: true,
  });
});

afterEach(async () => {
  await incomingQueue.obliterate({
    force: true,
  });

  await routingQueue.obliterate({
    force: true,
  });

  await clearDatabaseFixtures();

  logOutput = "";
});

afterAll(async () => {
  await incomingWorker.close();
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

  await database.$disconnect();
});

describe("incoming-events processor", () => {
  it("verifies durable state and prepares a correlated routing job", async () => {
    const fixture = await createServiceRequestFixture();

    const outboxEventId = randomUUID();
    const correlationId = `request-${randomUUID()}`;

    const incomingJob = await incomingQueue.add(
      JOB_NAMES.serviceRequestIngested,
      {
        outboxEventId,
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId,
      },
      {
        jobId: `incoming-${randomUUID()}`,
      },
    );

    const result = await incomingJob.waitUntilFinished(
      incomingQueueEvents,
      5_000,
    );

    const routingJobId = createRoutingJobId(fixture.serviceRequestId);

    expect(result).toEqual({
      routingJobId,
    });

    const routingJob = await routingQueue.getJob(routingJobId);

    expect(routingJob).not.toBeNull();

    if (!routingJob) {
      throw new Error("Expected the routing job to exist");
    }

    expect(routingJob.name).toBe(JOB_NAMES.routeServiceRequest);

    expect(routingJob.data).toEqual({
      organizationId: fixture.organizationId,
      serviceRequestId: fixture.serviceRequestId,
      correlationId,
    });

    const savedRequest = await database.serviceRequest.findUniqueOrThrow({
      where: {
        id: fixture.serviceRequestId,
      },
    });

    expect(savedRequest.status).toBe("PENDING");

    const completionLog = readLogEntries().find(
      (entry) => entry.msg === "Incoming job completed",
    );

    expect(completionLog).toMatchObject({
      queue: "incoming-events",
      jobName: "service-request-ingested",
      jobId: incomingJob.id,
      attemptsMade: 0,
      organizationId: fixture.organizationId,
      serviceRequestId: fixture.serviceRequestId,
      correlationId,
      outcome: "routing_job_ready",
      outboxEventId,
      routingJobId,
      serviceRequestStatus: "PENDING",
    });
  });

  it("rejects invalid runtime job data without creating routing work", async () => {
    const invalidData = {
      outboxEventId: randomUUID(),
      organizationId: randomUUID(),
      serviceRequestId: randomUUID(),
    } as unknown as ServiceRequestIngestedJobData;

    const incomingJob = await incomingQueue.add(
      JOB_NAMES.serviceRequestIngested,
      invalidData,
      {
        jobId: `invalid-${randomUUID()}`,
        attempts: 1,
      },
    );

    await expect(
      incomingJob.waitUntilFinished(incomingQueueEvents, 5_000),
    ).rejects.toThrow("Invalid incoming job data");

    expect(await countRoutingJobs()).toBe(0);
  });

  it("fails when the durable ServiceRequest does not exist", async () => {
    const incomingJob = await incomingQueue.add(
      JOB_NAMES.serviceRequestIngested,
      {
        outboxEventId: randomUUID(),
        organizationId: randomUUID(),
        serviceRequestId: randomUUID(),
        correlationId: `request-${randomUUID()}`,
      },
      {
        jobId: `missing-${randomUUID()}`,
        attempts: 1,
      },
    );

    await expect(
      incomingJob.waitUntilFinished(incomingQueueEvents, 5_000),
    ).rejects.toThrow("ServiceRequest does not exist for the incoming job");

    expect(await countRoutingJobs()).toBe(0);
  });

  it("converges duplicate incoming execution on one routing job", async () => {
    const fixture = await createServiceRequestFixture();

    const jobData: ServiceRequestIngestedJobData = {
      outboxEventId: randomUUID(),
      organizationId: fixture.organizationId,
      serviceRequestId: fixture.serviceRequestId,
      correlationId: `request-${randomUUID()}`,
    };

    const firstIncomingJob = await incomingQueue.add(
      JOB_NAMES.serviceRequestIngested,
      jobData,
      {
        jobId: `duplicate-a-${randomUUID()}`,
      },
    );

    const secondIncomingJob = await incomingQueue.add(
      JOB_NAMES.serviceRequestIngested,
      jobData,
      {
        jobId: `duplicate-b-${randomUUID()}`,
      },
    );

    await Promise.all([
      firstIncomingJob.waitUntilFinished(incomingQueueEvents, 5_000),
      secondIncomingJob.waitUntilFinished(incomingQueueEvents, 5_000),
    ]);

    const routingJobId = createRoutingJobId(fixture.serviceRequestId);

    expect(await routingQueue.getJob(routingJobId)).not.toBeNull();

    expect(await countRoutingJobs()).toBe(1);
  });
});
