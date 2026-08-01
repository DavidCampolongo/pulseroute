import {
  QUEUE_NAMES,
  type DeadLetteredJobData,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { Queue, type DefaultJobOptions } from "bullmq";

import { createProducerRedisOptions } from "./redis.js";

const ONE_DAY_SECONDS = 24 * 60 * 60;
const SEVEN_DAYS_SECONDS = 7 * ONE_DAY_SECONDS;
const THIRTY_DAYS_SECONDS = 30 * ONE_DAY_SECONDS;

function createRetryableJobOptions(): DefaultJobOptions {
  return {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1_000,
    },
    removeOnComplete: {
      age: ONE_DAY_SECONDS,
      count: 1_000,
    },
    removeOnFail: {
      age: SEVEN_DAYS_SECONDS,
      count: 5_000,
    },
    stackTraceLimit: 20,
  };
}

function createFutureFacingJobOptions(): DefaultJobOptions {
  return {
    attempts: 1,
    removeOnComplete: {
      age: ONE_DAY_SECONDS,
      count: 1_000,
    },
    removeOnFail: {
      age: SEVEN_DAYS_SECONDS,
      count: 5_000,
    },
    stackTraceLimit: 20,
  };
}

function createDeadLetterJobOptions(): DefaultJobOptions {
  return {
    attempts: 1,
    removeOnComplete: {
      age: THIRTY_DAYS_SECONDS,
      count: 10_000,
    },
    removeOnFail: {
      age: THIRTY_DAYS_SECONDS,
      count: 10_000,
    },
    stackTraceLimit: 20,
  };
}

export type PulseRouteQueues = {
  incomingEvents: Queue<ServiceRequestIngestedJobData>;
  routing: Queue<RouteServiceRequestJobData>;
  notifications: Queue<Record<string, never>>;
  webhookDelivery: Queue<Record<string, never>>;
  deadLetter: Queue<DeadLetteredJobData>;
};

export function createPulseRouteQueues(redisUrl: string): PulseRouteQueues {
  return {
    incomingEvents: new Queue<ServiceRequestIngestedJobData>(
      QUEUE_NAMES.incomingEvents,
      {
        connection: createProducerRedisOptions(redisUrl),
        defaultJobOptions: createRetryableJobOptions(),
      },
    ),

    routing: new Queue<RouteServiceRequestJobData>(QUEUE_NAMES.routing, {
      connection: createProducerRedisOptions(redisUrl),
      defaultJobOptions: createRetryableJobOptions(),
    }),

    notifications: new Queue<Record<string, never>>(QUEUE_NAMES.notifications, {
      connection: createProducerRedisOptions(redisUrl),
      defaultJobOptions: createFutureFacingJobOptions(),
    }),

    webhookDelivery: new Queue<Record<string, never>>(
      QUEUE_NAMES.webhookDelivery,
      {
        connection: createProducerRedisOptions(redisUrl),
        defaultJobOptions: createFutureFacingJobOptions(),
      },
    ),

    deadLetter: new Queue<DeadLetteredJobData>(QUEUE_NAMES.deadLetter, {
      connection: createProducerRedisOptions(redisUrl),
      defaultJobOptions: createDeadLetterJobOptions(),
    }),
  };
}

export async function waitForPulseRouteQueues(
  queues: PulseRouteQueues,
): Promise<void> {
  await Promise.all([
    queues.incomingEvents.waitUntilReady(),
    queues.routing.waitUntilReady(),
    queues.notifications.waitUntilReady(),
    queues.webhookDelivery.waitUntilReady(),
    queues.deadLetter.waitUntilReady(),
  ]);
}

export async function closePulseRouteQueues(
  queues: PulseRouteQueues,
): Promise<void> {
  await Promise.all([
    queues.incomingEvents.close(),
    queues.routing.close(),
    queues.notifications.close(),
    queues.webhookDelivery.close(),
    queues.deadLetter.close(),
  ]);
}
