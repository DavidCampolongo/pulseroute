import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import type {
  DeadLetteredJobData,
  WebhookDeliveryJobData,
} from "@pulseroute/shared";
import type { Queue } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it, vi } from "vitest";

import { DeliveryScheduler } from "../src/delivery-scheduler.js";
import { createWorkerLogger } from "../src/logger.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error(
    "DATABASE_URL is required for delivery scheduler failure tests",
  );
}

const database = createDatabaseClient(databaseUrl);
const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});
const NOW = new Date("1904-01-01T00:00:00.000Z");
const organizationId = randomUUID();

afterAll(async () => {
  await database.webhookDelivery.deleteMany({
    where: {
      organizationId,
    },
  });
  await database.outboxEvent.deleteMany({
    where: {
      organizationId,
    },
  });
  await database.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });
  await database.$disconnect();
});

describe("delivery scheduler queue failure", () => {
  it("releases the exact PostgreSQL claim without consuming an HTTP attempt", async () => {
    const outboxEventId = randomUUID();
    const serviceRequestId = randomUUID();

    await database.organization.create({
      data: {
        id: organizationId,
        name: "Delivery Scheduler Queue Failure",
      },
    });
    await database.outboxEvent.create({
      data: {
        id: outboxEventId,
        organizationId,
        eventType: "service_request.assigned",
        aggregateType: "service_request",
        aggregateId: serviceRequestId,
        nextAttemptAt: new Date("1903-12-31T23:00:00.000Z"),
        createdAt: new Date("1903-12-31T23:00:00.000Z"),
        payload: {
          organizationId,
          serviceRequestId,
          operatorId: randomUUID(),
          assignmentId: randomUUID(),
          routingDecisionId: randomUUID(),
          scoringVersion: "pulseroute-scoring-v1",
          correlationId: `scheduler-failure-${randomUUID()}`,
        },
      },
    });

    const addDelivery = vi
      .fn()
      .mockRejectedValue(new Error("Redis publication unavailable"));
    const deliveryQueue = {
      add: addDelivery,
    } as unknown as Queue<WebhookDeliveryJobData>;
    const deadLetterQueue = {
      add: vi.fn(),
    } as unknown as Queue<DeadLetteredJobData>;
    const scheduler = new DeliveryScheduler({
      database,
      webhookDeliveryQueue: deliveryQueue,
      deadLetterQueue,
      logger,
      pollIntervalMs: 250,
      batchSize: 1,
      claimTimeoutMs: 1_000,
      maxAttempts: 5,
      now: () => new Date(NOW),
    });

    const result = await scheduler.publishOnce();

    expect(result).toMatchObject({
      deliveryClaimed: 1,
      deliveryPublished: 0,
      deliveryPublicationFailed: 1,
    });
    expect(addDelivery).toHaveBeenCalledOnce();

    const [outbox, deliveryCount] = await Promise.all([
      database.outboxEvent.findUniqueOrThrow({
        where: {
          id: outboxEventId,
        },
      }),
      database.webhookDelivery.count({
        where: {
          outboxEventId,
        },
      }),
    ]);

    expect(outbox).toMatchObject({
      status: "PENDING",
      attemptCount: 0,
      processingStartedAt: null,
      processedAt: null,
      nextAttemptAt: new Date("1904-01-01T00:00:00.250Z"),
      lastError: {
        code: "QUEUE_PUBLICATION_FAILED",
        message: "Redis publication unavailable",
        recordedAt: NOW.toISOString(),
      },
    });
    expect(deliveryCount).toBe(0);
  });
});
