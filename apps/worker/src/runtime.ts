import { createDatabaseClient, type DatabaseClient } from "@pulseroute/db";
import type {
  RouteServiceRequestJobData,
  ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import type { Worker } from "bullmq";
import type { Logger } from "pino";

import type { WorkerConfig } from "./config.js";
import {
  createIncomingWorker,
  type IncomingProcessorResult,
} from "./incoming-worker.js";
import { createRoutingWorker } from "./routing-worker.js";
import type { RoutingAssignmentResult } from "./routing-workflow.js";
import { InternalOutboxPublisher } from "./outbox-publisher.js";
import {
  closePulseRouteQueues,
  createPulseRouteQueues,
  type PulseRouteQueues,
  waitForPulseRouteQueues,
} from "./queues.js";

export type WorkerShutdownReason = NodeJS.Signals | "startup_failure";

export type RuntimePublisher = Pick<InternalOutboxPublisher, "start" | "stop">;

export type RuntimeIncomingWorker = Pick<
  Worker<ServiceRequestIngestedJobData, IncomingProcessorResult, string>,
  "close"
>;

export type RuntimeRoutingWorker = Pick<
  Worker<RouteServiceRequestJobData, RoutingAssignmentResult, string>,
  "close"
>;

export type WorkerRuntimeOptions = {
  database: DatabaseClient;
  queues: PulseRouteQueues;
  incomingWorker: RuntimeIncomingWorker;
  publisher: RuntimePublisher;
  logger: Logger;
};

async function attemptStartupCleanup(
  logger: Logger,
  resource: string,
  cleanup: () => Promise<void>,
): Promise<void> {
  try {
    await cleanup();
  } catch (error) {
    logger.error(
      {
        err: error,
        resource,
      },
      "Worker startup cleanup failed",
    );
  }
}

export class WorkerRuntime {
  private readonly database: DatabaseClient;
  private readonly queues: PulseRouteQueues;
  private readonly incomingWorker: RuntimeIncomingWorker;
  private readonly publisher: RuntimePublisher;
  private readonly logger: Logger;

  private routingWorker: RuntimeRoutingWorker | undefined;
  private started = false;
  private shutdownPromise: Promise<void> | undefined;

  constructor(options: WorkerRuntimeOptions) {
    this.database = options.database;
    this.queues = options.queues;
    this.incomingWorker = options.incomingWorker;
    this.publisher = options.publisher;
    this.logger = options.logger.child({
      component: "worker-runtime",
    });
  }

  registerRoutingWorker(worker: RuntimeRoutingWorker): void {
    this.routingWorker = worker;
  }

  async start(): Promise<void> {
    if (this.started) {
      return;
    }

    await this.publisher.start();

    this.started = true;

    this.logger.info(
      {
        incomingQueue: this.queues.incomingEvents.name,
        routingQueue: this.queues.routing.name,
        deadLetterQueue: this.queues.deadLetter.name,
      },
      "Worker runtime started",
    );
  }

  shutdown(reason: WorkerShutdownReason): Promise<void> {
    if (!this.shutdownPromise) {
      this.shutdownPromise = this.performShutdown(reason);
    }

    return this.shutdownPromise;
  }

  private async performShutdown(reason: WorkerShutdownReason): Promise<void> {
    const shutdownErrors: unknown[] = [];

    this.logger.info(
      {
        reason,
      },
      "Worker shutdown started",
    );

    try {
      await this.publisher.stop();

      this.logger.info(
        {
          reason,
        },
        "Outbox publisher intake stopped",
      );
    } catch (error) {
      shutdownErrors.push(error);
    }

    try {
      const drainPromise = this.incomingWorker.close();

      this.logger.info(
        {
          reason,
        },
        "Worker intake stopped; draining active jobs",
      );

      await drainPromise;

      this.logger.info(
        {
          reason,
        },
        "Worker active jobs drained",
      );
    } catch (error) {
      shutdownErrors.push(error);
    }

    try {
      if (this.routingWorker) {
        const routingDrainPromise = this.routingWorker.close();

        this.logger.info(
          {
            reason,
          },
          "Routing worker stopped; draining active jobs",
        );

        await routingDrainPromise;

        this.logger.info(
          {
            reason,
          },
          "Routing worker active jobs drained",
        );
      }
    } catch (error) {
      shutdownErrors.push(error);
    }

    try {
      await closePulseRouteQueues(this.queues);

      this.logger.info(
        {
          reason,
        },
        "Worker queues closed",
      );
    } catch (error) {
      shutdownErrors.push(error);
    }

    try {
      await this.database.$disconnect();

      this.logger.info(
        {
          reason,
        },
        "Worker database disconnected",
      );
    } catch (error) {
      shutdownErrors.push(error);
    }

    if (shutdownErrors.length > 0) {
      throw new AggregateError(
        shutdownErrors,
        "Worker shutdown did not complete cleanly",
      );
    }

    this.logger.info(
      {
        reason,
      },
      "Worker shutdown completed",
    );
  }
}

export async function createWorkerRuntime(
  config: WorkerConfig,
  logger: Logger,
): Promise<WorkerRuntime> {
  const database = createDatabaseClient(config.databaseUrl);

  let queues: PulseRouteQueues | undefined;
  let incomingWorker: ReturnType<typeof createIncomingWorker> | undefined;
  let routingWorker: ReturnType<typeof createRoutingWorker> | undefined;

  try {
    queues = createPulseRouteQueues(config.redisUrl);

    await waitForPulseRouteQueues(queues);

    incomingWorker = createIncomingWorker({
      database,
      routingQueue: queues.routing,
      deadLetterQueue: queues.deadLetter,
      logger,
      redisUrl: config.redisUrl,
    });

    incomingWorker.on("error", (error) => {
      logger.error(
        {
          err: error,
        },
        "Incoming worker error",
      );
    });

    routingWorker = createRoutingWorker({
      database,
      deadLetterQueue: queues.deadLetter,
      logger,
      redisUrl: config.redisUrl,
    });

    routingWorker.on("error", (error) => {
      logger.error(
        {
          err: error,
        },
        "Routing worker error",
      );
    });

    await Promise.all([
      incomingWorker.waitUntilReady(),
      routingWorker.waitUntilReady(),
    ]);

    const publisher = new InternalOutboxPublisher({
      database,
      incomingQueue: queues.incomingEvents,
      logger,
    });

    const runtime = new WorkerRuntime({
      database,
      queues,
      incomingWorker,
      publisher,
      logger,
    });

    runtime.registerRoutingWorker(routingWorker);

    return runtime;
  } catch (error) {
    if (incomingWorker) {
      const workerToClose = incomingWorker;

      await attemptStartupCleanup(logger, "incoming-worker", () =>
        workerToClose.close(true),
      );
    }

    if (routingWorker) {
      const workerToClose = routingWorker;

      await attemptStartupCleanup(logger, "routing-worker", () =>
        workerToClose.close(true),
      );
    }

    if (queues) {
      const queuesToClose = queues;

      await attemptStartupCleanup(logger, "queues", () =>
        closePulseRouteQueues(queuesToClose),
      );
    }

    await database.$disconnect().catch(() => undefined);

    throw error;
  }
}

export async function runWorkerProcess(
  runtime: WorkerRuntime,
  signalSource: NodeJS.Process = process,
): Promise<void> {
  try {
    await runtime.start();
  } catch (error) {
    signalSource.exitCode = 1;
    throw error;
  }

  const handleSignal = (signal: NodeJS.Signals) => {
    void runtime.shutdown(signal).catch((error) => {
      signalSource.exitCode = 1;
      runtime["logger"].error(
        {
          err: error,
          signal,
        },
        "Worker shutdown failed",
      );
    });
  };

  signalSource.once("SIGTERM", handleSignal);
  signalSource.once("SIGINT", handleSignal);
}
