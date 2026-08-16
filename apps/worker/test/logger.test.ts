import { PassThrough } from "node:stream";

import { JOB_NAMES, QUEUE_NAMES } from "@pulseroute/shared";
import { describe, expect, it } from "vitest";

import { createJobLogger, createWorkerLogger } from "../src/logger.js";

function createCapture() {
  const stream = new PassThrough();
  let output = "";

  stream.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });

  return {
    stream,
    readEntries: (): Record<string, unknown>[] =>
      output
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line) as Record<string, unknown>),
    readRaw: (): string => output,
  };
}

describe("worker logger", () => {
  it("includes worker process context", () => {
    const capture = createCapture();

    const logger = createWorkerLogger(
      {
        nodeEnv: "test",
        logLevel: "info",
      },
      capture.stream,
    );

    logger.info("Worker started");

    expect(capture.readEntries()[0]).toMatchObject({
      service: "pulseroute-worker",
      processType: "worker",
      environment: "test",
      pid: process.pid,
      msg: "Worker started",
    });
  });

  it("preserves queue, job, durable ID, and correlation context", () => {
    const capture = createCapture();

    const logger = createWorkerLogger(
      {
        nodeEnv: "test",
        logLevel: "info",
      },
      capture.stream,
    );

    const jobLogger = createJobLogger(logger, {
      queue: QUEUE_NAMES.incomingEvents,
      jobName: JOB_NAMES.serviceRequestIngested,
      jobId: "job-123",
      attemptsMade: 2,
      organizationId: "organization-123",
      serviceRequestId: "request-123",
      correlationId: "req-42",
    });

    jobLogger.info(
      {
        outcome: "completed",
        durationMs: 25,
      },
      "Job processed",
    );

    expect(capture.readEntries()[0]).toMatchObject({
      queue: "incoming-events",
      jobName: "service-request-ingested",
      jobId: "job-123",
      attemptsMade: 2,
      organizationId: "organization-123",
      serviceRequestId: "request-123",
      correlationId: "req-42",
      outcome: "completed",
      durationMs: 25,
      msg: "Job processed",
    });
  });

  it("removes known sensitive configuration values", () => {
    const capture = createCapture();

    const logger = createWorkerLogger(
      {
        nodeEnv: "test",
        logLevel: "info",
      },
      capture.stream,
    );

    logger.info(
      {
        databaseUrl: "postgresql://user:database-secret@localhost/database",
        redisUrl: "redis://:redis-secret@localhost:6379",
        webhookDeliveryUrl: "https://url-secret@receiver.example/webhooks",
        outboundWebhookSecret: "top-level-outbound-secret",
        config: {
          databaseUrl:
            "postgresql://user:nested-database-secret@localhost/database",
          redisUrl: "redis://:nested-redis-secret@localhost:6379",
          webhookDeliveryUrl:
            "https://nested-url-secret@receiver.example/webhooks",
          outboundWebhookSecret: "nested-outbound-secret",
        },
      },
      "Sensitive-value test",
    );

    const rawOutput = capture.readRaw();
    const [entry] = capture.readEntries();

    expect(entry).toBeDefined();

    if (!entry) {
      throw new Error("Expected the logger to emit one entry");
    }

    expect(rawOutput).not.toContain("database-secret");
    expect(rawOutput).not.toContain("redis-secret");
    expect(rawOutput).not.toContain("outbound-secret");
    expect(rawOutput).not.toContain("url-secret");
    expect(entry).not.toHaveProperty("webhookDeliveryUrl");
    expect(entry).not.toHaveProperty("outboundWebhookSecret");
    expect(entry).not.toHaveProperty("databaseUrl");
    expect(entry).not.toHaveProperty("redisUrl");
    expect(entry.config).toEqual({});
  });
});
