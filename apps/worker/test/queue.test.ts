import { QUEUE_NAMES } from "@pulseroute/shared";
import { describe, expect, it } from "vitest";

import {
  closePulseRouteQueues,
  createPulseRouteQueues,
  type PulseRouteQueues,
  waitForPulseRouteQueues,
} from "../src/queues.js";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";

function queueList(queues: PulseRouteQueues) {
  return [
    queues.incomingEvents,
    queues.routing,
    queues.notifications,
    queues.webhookDelivery,
    queues.deadLetter,
  ];
}

describe("PulseRoute queues", () => {
  it("creates all five queues with stable shared names", async () => {
    const queues = createPulseRouteQueues(redisUrl);

    try {
      const names = queueList(queues).map((queue) => queue.name);

      expect(names).toHaveLength(5);
      expect(new Set(names)).toEqual(new Set(Object.values(QUEUE_NAMES)));
    } finally {
      await closePulseRouteQueues(queues);
    }
  });

  it("uses deliberate retry and retention defaults", async () => {
    const queues = createPulseRouteQueues(redisUrl);

    try {
      expect(queues.incomingEvents.defaultJobOptions).toMatchObject({
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 1_000,
        },
        removeOnComplete: {
          age: 86_400,
          count: 1_000,
        },
        removeOnFail: {
          age: 604_800,
          count: 5_000,
        },
      });

      expect(queues.routing.defaultJobOptions).toMatchObject({
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 1_000,
        },
      });

      expect(queues.notifications.defaultJobOptions).toMatchObject({
        attempts: 1,
      });

      expect(queues.webhookDelivery.defaultJobOptions).toMatchObject({
        attempts: 1,
      });

      expect(queues.deadLetter.defaultJobOptions).toMatchObject({
        attempts: 1,
        removeOnComplete: {
          age: 2_592_000,
          count: 10_000,
        },
        removeOnFail: {
          age: 2_592_000,
          count: 10_000,
        },
      });

      expect(queues.notifications.defaultJobOptions).not.toHaveProperty(
        "backoff",
      );

      expect(queues.webhookDelivery.defaultJobOptions).not.toHaveProperty(
        "backoff",
      );

      for (const queue of queueList(queues)) {
        expect(queue.defaultJobOptions).not.toHaveProperty("priority");
      }
    } finally {
      await closePulseRouteQueues(queues);
    }
  });

  it("connects every queue to real Redis and closes cleanly", async () => {
    const queues = createPulseRouteQueues(redisUrl);

    try {
      await waitForPulseRouteQueues(queues);

      const counts = await Promise.all(
        queueList(queues).map((queue) =>
          queue.getJobCounts(
            "waiting",
            "active",
            "delayed",
            "completed",
            "failed",
          ),
        ),
      );

      expect(counts).toHaveLength(5);
    } finally {
      await closePulseRouteQueues(queues);
    }
  });
});
