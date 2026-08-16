import type { DatabaseClient } from "@pulseroute/db";
import type { Logger } from "pino";
import { describe, expect, it, vi } from "vitest";

import type { PulseRouteQueues } from "../src/queues.js";
import { WorkerRuntime } from "../src/runtime.js";

describe("delivery runtime shutdown", () => {
  it("starts delivery claiming and stops it before draining bounded HTTP work", async () => {
    const calls: string[] = [];
    const recordAsync = (name: string) =>
      vi.fn(async () => {
        calls.push(name);
      });
    const recordSync = (name: string) =>
      vi.fn(() => {
        calls.push(name);
      });
    const createQueue = (name: string) => ({
      name,
      close: recordAsync(`queue-close:${name}`),
    });
    const queues = {
      incomingEvents: createQueue("incoming-events"),
      routing: createQueue("routing"),
      notifications: createQueue("notifications"),
      webhookDelivery: createQueue("webhook-delivery"),
      deadLetter: createQueue("dead-letter"),
    } as unknown as PulseRouteQueues;
    const database = {
      $disconnect: recordAsync("database-disconnect"),
    } as unknown as DatabaseClient;
    const publisher = {
      start: recordSync("publisher-start"),
      stop: recordAsync("publisher-stop"),
    };
    const deliveryScheduler = {
      start: recordSync("delivery-scheduler-start"),
      stop: recordAsync("delivery-scheduler-stop"),
    };
    const incomingWorker = {
      close: recordAsync("incoming-worker-close"),
    };
    const routingWorker = {
      close: recordAsync("routing-worker-close"),
    };
    const webhookDeliveryWorker = {
      close: recordAsync("webhook-delivery-worker-close"),
    };
    const logger = {
      child: vi.fn().mockReturnThis(),
      info: vi.fn(),
      error: vi.fn(),
    } as unknown as Logger;
    const runtime = new WorkerRuntime({
      database,
      queues,
      incomingWorker,
      publisher,
      deliveryScheduler,
      webhookDeliveryWorker,
      logger,
    });

    runtime.registerRoutingWorker(routingWorker);

    await runtime.start();

    expect(calls).toEqual(["publisher-start", "delivery-scheduler-start"]);

    await runtime.shutdown("SIGTERM");

    expect(calls).toEqual([
      "publisher-start",
      "delivery-scheduler-start",
      "delivery-scheduler-stop",
      "publisher-stop",
      "webhook-delivery-worker-close",
      "incoming-worker-close",
      "routing-worker-close",
      "queue-close:incoming-events",
      "queue-close:routing",
      "queue-close:notifications",
      "queue-close:webhook-delivery",
      "queue-close:dead-letter",
      "database-disconnect",
    ]);

    expect(calls.indexOf("delivery-scheduler-stop")).toBeLessThan(
      calls.indexOf("webhook-delivery-worker-close"),
    );
    expect(calls.indexOf("webhook-delivery-worker-close")).toBeLessThan(
      calls.indexOf("queue-close:webhook-delivery"),
    );
  });
});
