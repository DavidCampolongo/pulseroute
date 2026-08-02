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

const config = parseWorkerConfig(process.env);

const logger = createWorkerLogger(config);

const database = createDatabaseClient(config.databaseUrl);

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
  });

  incomingWorker.on("error", (error) => {
    logger.error(
      {
        err: error,
        component: "graceful-shutdown-child",
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
     * Outbox start/stop behavior already has dedicated integration coverage.
     * This child isolates the process signal and active-job drain behavior.
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
    "Graceful-shutdown child failed",
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
