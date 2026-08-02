import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient, type DatabaseClient } from "@pulseroute/db";
import {
  EVENT_TYPES,
  JOB_NAMES,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { Queue } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { createWorkerLogger } from "../src/logger.js";
import {
  createIncomingJobId,
  InternalOutboxPublisher,
} from "../src/outbox-publisher.js";
import { createProducerRedisOptions } from "../src/redis.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const redisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for outbox publisher tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for outbox publisher tests");
}

const TEST_NOW = new Date("2000-01-01T00:00:00.000Z");

const ELIGIBLE_AT = new Date("1999-12-31T23:59:00.000Z");

const FUTURE_AT = new Date("2000-01-01T00:01:00.000Z");

const database = createDatabaseClient(databaseUrl);

const incomingQueue = new Queue<ServiceRequestIngestedJobData>(
  `phase6-outbox-publisher-${randomUUID()}`,
  {
    connection: createProducerRedisOptions(redisUrl),
    skipWaitingForReady: true,
    defaultJobOptions: {
      removeOnComplete: false,
      removeOnFail: false,
    },
  },
);

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

const createdOrganizationIds: string[] = [];

type PendingFixture = {
  organizationId: string;
  serviceRequestId: string;
  outboxEventId: string;
  correlationId: string;
};

async function createPendingFixture(
  options: {
    eventType?: string;
    nextAttemptAt?: Date;
  } = {},
): Promise<PendingFixture> {
  const organizationId = randomUUID();
  const requiredSkillId = randomUUID();
  const serviceRequestId = randomUUID();
  const outboxEventId = randomUUID();
  const correlationId = `request-${randomUUID()}`;

  createdOrganizationIds.push(organizationId);

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Outbox Publisher Test ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: `Publisher Skill ${requiredSkillId}`,
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `publisher-request-${randomUUID()}`,
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  });

  await database.outboxEvent.create({
    data: {
      id: outboxEventId,
      organizationId,
      eventType: options.eventType ?? EVENT_TYPES.serviceRequestCreated,
      aggregateType: "service_request",
      aggregateId: serviceRequestId,
      status: "PENDING",
      nextAttemptAt: options.nextAttemptAt ?? ELIGIBLE_AT,
      payload: {
        serviceRequestId,
        correlationId,

        // These durable business fields remain in PostgreSQL.
        // They must not be copied into the BullMQ payload.
        externalId: "not-copied-to-redis",
        requiredSkillId,
        priority: "HIGH",
        region: "WEST",
      },
    },
  });

  return {
    organizationId,
    serviceRequestId,
    outboxEventId,
    correlationId,
  };
}

async function clearDatabaseFixtures(client: DatabaseClient): Promise<void> {
  const organizationIds = createdOrganizationIds.splice(
    0,
    createdOrganizationIds.length,
  );

  if (organizationIds.length === 0) {
    return;
  }

  await client.outboxEvent.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await client.serviceRequest.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await client.skill.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await client.organization.deleteMany({
    where: {
      id: {
        in: organizationIds,
      },
    },
  });
}

function createPublisher(
  options: {
    queue?: Queue<ServiceRequestIngestedJobData>;
    pollIntervalMs?: number;
  } = {},
): InternalOutboxPublisher {
  return new InternalOutboxPublisher({
    database,
    incomingQueue: options.queue ?? incomingQueue,
    logger,
    pollIntervalMs: options.pollIntervalMs ?? 100,
    now: () => TEST_NOW,
  });
}

beforeAll(async () => {
  await incomingQueue.waitUntilReady();

  await incomingQueue.obliterate({
    force: true,
  });
});

afterEach(async () => {
  await incomingQueue.obliterate({
    force: true,
  });

  await clearDatabaseFixtures(database);
});

afterAll(async () => {
  await incomingQueue
    .obliterate({
      force: true,
    })
    .catch(() => undefined);

  await incomingQueue.close();

  await database.$disconnect();
});

describe("InternalOutboxPublisher", () => {
  it("publishes a minimal deterministic job and marks the outbox event delivered", async () => {
    const fixture = await createPendingFixture();

    const publisher = createPublisher();

    const result = await publisher.publishOnce();

    expect(result).toEqual({
      selected: 1,
      published: 1,
      failed: 0,
    });

    const savedEvent = await database.outboxEvent.findUniqueOrThrow({
      where: {
        id: fixture.outboxEventId,
      },
    });

    expect(savedEvent.status).toBe("DELIVERED");
    expect(savedEvent.attemptCount).toBe(1);
    expect(savedEvent.processedAt).toEqual(TEST_NOW);
    expect(savedEvent.lastError).toBeNull();

    const jobId = createIncomingJobId(fixture.outboxEventId);

    const job = await incomingQueue.getJob(jobId);

    expect(job).not.toBeNull();

    if (!job) {
      throw new Error("Expected the incoming BullMQ job to exist");
    }

    expect(job.name).toBe(JOB_NAMES.serviceRequestIngested);

    expect(job.data).toEqual({
      outboxEventId: fixture.outboxEventId,
      organizationId: fixture.organizationId,
      serviceRequestId: fixture.serviceRequestId,
      correlationId: fixture.correlationId,
    });

    expect(job.data).not.toHaveProperty("externalId");

    expect(job.data).not.toHaveProperty("requiredSkillId");

    expect(job.data).not.toHaveProperty("priority");
    expect(job.data).not.toHaveProperty("region");
  });

  it("selects only eligible pending service-request events", async () => {
    const wrongType = await createPendingFixture({
      eventType: "unrelated.event",
    });

    const futureEvent = await createPendingFixture({
      nextAttemptAt: FUTURE_AT,
    });

    const publisher = createPublisher();

    const result = await publisher.publishOnce();

    expect(result).toEqual({
      selected: 0,
      published: 0,
      failed: 0,
    });

    const events = await database.outboxEvent.findMany({
      where: {
        id: {
          in: [wrongType.outboxEventId, futureEvent.outboxEventId],
        },
      },
    });

    expect(events).toHaveLength(2);

    for (const event of events) {
      expect(event.status).toBe("PENDING");
      expect(event.attemptCount).toBe(0);
      expect(event.processedAt).toBeNull();
    }

    const counts = await incomingQueue.getJobCounts(
      "waiting",
      "active",
      "delayed",
      "completed",
      "failed",
    );

    const totalJobs = Object.values(counts).reduce(
      (total, count) => total + count,
      0,
    );

    expect(totalJobs).toBe(0);
  });

  it("leaves the event recoverable when Redis publication fails", async () => {
    const fixture = await createPendingFixture();

    const unavailableQueue = new Queue<ServiceRequestIngestedJobData>(
      `phase6-unavailable-publisher-${randomUUID()}`,
      {
        connection: createProducerRedisOptions("redis://127.0.0.1:6399"),
        skipWaitingForReady: true,
      },
    );

    unavailableQueue.on("error", () => {
      // The durable database result is the assertion surface.
    });

    const pollIntervalMs = 250;

    const publisher = createPublisher({
      queue: unavailableQueue,
      pollIntervalMs,
    });

    const startedAt = Date.now();

    try {
      const result = await publisher.publishOnce();

      expect(result).toEqual({
        selected: 1,
        published: 0,
        failed: 1,
      });

      expect(Date.now() - startedAt).toBeLessThan(3_000);

      const savedEvent = await database.outboxEvent.findUniqueOrThrow({
        where: {
          id: fixture.outboxEventId,
        },
      });

      expect(savedEvent.status).toBe("PENDING");
      expect(savedEvent.processedAt).toBeNull();
      expect(savedEvent.attemptCount).toBe(1);

      expect(savedEvent.nextAttemptAt).toEqual(
        new Date(TEST_NOW.getTime() + pollIntervalMs),
      );

      expect(savedEvent.lastError).toMatchObject({
        name: expect.any(String),
        message: expect.any(String),
        recordedAt: TEST_NOW.toISOString(),
      });
    } finally {
      await unavailableQueue.disconnect().catch(() => undefined);
    }
  }, 5_000);

  it("starts and stops its polling loop cleanly", async () => {
    const publisher = createPublisher({
      pollIntervalMs: 60_000,
    });

    publisher.start();

    expect(publisher.isRunning).toBe(true);

    await publisher.stop();

    expect(publisher.isRunning).toBe(false);
  });
});
