import process from "node:process";

import { createDatabaseClient } from "@pulseroute/db";

import { parseWorkerConfig } from "../../dist/config.js";

import { createIncomingWorker } from "../../dist/incoming-worker.js";

import { createWorkerLogger } from "../../dist/logger.js";

import {
  closePulseRouteQueues,
  createPulseRouteQueues,
  waitForPulseRouteQueues,
} from "../../dist/queues.js";

import { runWorkerProcess, WorkerRuntime } from "../../dist/runtime.js";

function readPositiveInteger(variableName, fallbackValue) {
  const rawValue = process.env[variableName];

  if (rawValue === undefined) {
    return fallbackValue;
  }

  const parsedValue = Number(rawValue);

  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    throw new Error(`${variableName} must be a positive integer`);
  }

  return parsedValue;
}

const config = parseWorkerConfig(process.env);

const logger = createWorkerLogger(config);

const database = createDatabaseClient(config.databaseUrl);

const lockDuration = readPositiveInteger("TEST_WORKER_LOCK_DURATION_MS", 500);

const stalledInterval = readPositiveInteger(
  "TEST_WORKER_STALLED_INTERVAL_MS",
  100,
);

let queues;
let incomingWorker;
let runtime;

try {
  queues = createPulseRouteQueues(config.redisUrl);

  await waitForPulseRouteQueues(queues);

  incomingWorker = createIncomingWorker({
    database,

    routingQueue: queues.routing,

    deadLetterQueue: queues.deadLetter,

    logger,

    redisUrl: config.redisUrl,

    concurrency: 1,

    lockDuration,

    stalledInterval,

    maxStalledCount: 1,
  });

  incomingWorker.on("error", (error) => {
    logger.error(
      {
        err: error,
        component: "ungraceful-shutdown-child",
      },
      "Child worker infrastructure error",
    );
  });

  await incomingWorker.waitUntilReady();

  runtime = new WorkerRuntime({
    database,
    queues,
    incomingWorker,

    /*
     * This drill targets consumer lock loss and stalled recovery. Outbox
     * polling is covered independently and is intentionally disabled here.
     */
    publisher: {
      start() {},

      async stop() {},
    },

    logger,
  });

  await runWorkerProcess(runtime);
} catch (error) {
  logger.fatal(
    {
      err: error,
    },
    "Ungraceful-shutdown child failed",
  );

  if (!runtime) {
    if (incomingWorker) {
      await incomingWorker.close(true).catch(() => undefined);
    }

    if (queues) {
      await closePulseRouteQueues(queues).catch(() => undefined);
    }

    await database.$disconnect().catch(() => undefined);
  }

  process.exitCode = 1;
}
