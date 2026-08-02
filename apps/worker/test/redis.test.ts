import { randomUUID } from "node:crypto";

import { Queue } from "bullmq";
import { describe, expect, it } from "vitest";

import {
  createProducerRedisOptions,
  createWorkerRedisOptions,
} from "../src/redis.js";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";

describe("Redis connection configuration", () => {
  it("creates bounded producer connection options", () => {
    const options = createProducerRedisOptions("redis://127.0.0.1:6379");

    expect(options).toMatchObject({
      url: "redis://127.0.0.1:6379",
      connectTimeout: 5_000,
      enableOfflineQueue: true,
      maxRetriesPerRequest: 1,
    });

    expect(options).not.toHaveProperty("keyPrefix");
  });

  it("creates reconnecting worker connection options", () => {
    const options = createWorkerRedisOptions("redis://127.0.0.1:6379");

    expect(options).toMatchObject({
      url: "redis://127.0.0.1:6379",
      connectTimeout: 5_000,
      enableOfflineQueue: true,
      maxRetriesPerRequest: null,
    });

    expect(options).not.toHaveProperty("keyPrefix");
  });

  it("preserves the complete Redis URL for BullMQ parsing", () => {
    const url = "rediss://worker-user:worker-password@redis.example.com:6380/2";

    expect(createProducerRedisOptions(url).url).toBe(url);
    expect(createWorkerRedisOptions(url).url).toBe(url);
  });

  it("connects to real Redis and closes cleanly", async () => {
    const queue = new Queue(`phase6-redis-connection-${randomUUID()}`, {
      connection: createProducerRedisOptions(redisUrl),
      skipWaitingForReady: true,
    });

    const errors: Error[] = [];

    queue.on("error", (error) => {
      errors.push(error);
    });

    try {
      await queue.waitUntilReady();

      expect(errors).toEqual([]);

      await queue.obliterate({
        force: true,
      });
    } finally {
      await queue.close();
    }
  });

  it("fails a producer operation promptly when Redis is unavailable", async () => {
    const queue = new Queue(`phase6-unavailable-redis-${randomUUID()}`, {
      connection: createProducerRedisOptions("redis://127.0.0.1:6399"),
      skipWaitingForReady: true,
    });

    queue.on("error", () => {});

    const startedAt = Date.now();
    let publicationError: unknown;

    try {
      await queue.add("connection-probe", {});
    } catch (error) {
      publicationError = error;
    } finally {
      await queue.disconnect().catch(() => undefined);
    }

    expect(publicationError).toBeInstanceOf(Error);
    expect(Date.now() - startedAt).toBeLessThan(3_000);
  }, 5_000);
});
