import { createDatabaseClient, type DatabaseClient } from "@pulseroute/db";
import type { ServiceRequestIngestedJobData } from "@pulseroute/shared";
import type { Worker } from "bullmq";
import type { Logger } from "pino";

import type { WorkerConfig } from "./config.js";
import {
  createIncomingWorker,
  type IncomingProcessorResult,
} from "./incoming-worker.js";
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

export type WorkerRuntimeOptions = {
  database: DatabaseClient;
  queues: PulseRouteQueues;
  incomingWorker: RuntimeIncomingWorker;
  publisher: RuntimePublisher;
  logger: Logger;
};

type SignalSource = {
  once(signal: "SIGTERM" | "SIGINT", listener: () => void): unknown;

  removeListener(signal: "SIGTERM" | "SIGINT", listener: () => void): unknown;
};

async function attemptStartupCleanup(
  logger: Logger,
  resource: string,
  action: () => Promise<void>,
): Promise<void> {
  try {
    await action();
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

  get isStarted(): boolean {
    return this.started;
  }

  start(): void {
    if (this.started) {
      return;
    }

    this.publisher.start();

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

      this.logger.error(
        {
          err: error,
          reason,
        },
        "Failed to stop outbox publisher",
      );
    }

    try {
      /*
       * Calling close() first marks the worker as closing. It stops accepting
       * new jobs while allowing the currently active processor to finish.
       */
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

      this.logger.error(
        {
          err: error,
          reason,
        },
        "Failed to drain incoming worker",
      );
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

      this.logger.error(
        {
          err: error,
          reason,
        },
        "Failed to close worker queues",
      );
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

      this.logger.error(
        {
          err: error,
          reason,
        },
        "Failed to disconnect worker database",
      );
    }

    this.started = false;

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
          component: "incoming-worker",
        },
        "Incoming worker infrastructure error",
      );
    });

    await incomingWorker.waitUntilReady();

    const publisher = new InternalOutboxPublisher({
      database,
      incomingQueue: queues.incomingEvents,
      logger,
    });

    return new WorkerRuntime({
      database,
      queues,
      incomingWorker,
      publisher,
      logger,
    });
  } catch (error) {
    if (incomingWorker) {
      const workerToClose = incomingWorker;

      await attemptStartupCleanup(logger, "incoming-worker", () =>
        workerToClose.close(true),
      );
    }

    if (queues) {
      const queuesToClose = queues;

      await attemptStartupCleanup(logger, "queues", () =>
        closePulseRouteQueues(queuesToClose),
      );
    }

    await attemptStartupCleanup(logger, "database", () =>
      database.$disconnect(),
    );

    throw error;
  }
}

export async function runWorkerProcess(
  runtime: WorkerRuntime,
  signalSource: SignalSource = process,
): Promise<void> {
  try {
    runtime.start();
  } catch (error) {
    await runtime.shutdown("startup_failure").catch(() => undefined);

    throw error;
  }

  await new Promise<void>((resolve, reject) => {
    let shutdownStarted = false;

    const handleSignal = (signal: NodeJS.Signals) => {
      if (shutdownStarted) {
        return;
      }

      shutdownStarted = true;

      signalSource.removeListener("SIGTERM", onSigterm);

      signalSource.removeListener("SIGINT", onSigint);

      void runtime.shutdown(signal).then(resolve, reject);
    };

    const onSigterm = () => {
      handleSignal("SIGTERM");
    };

    const onSigint = () => {
      handleSignal("SIGINT");
    };

    signalSource.once("SIGTERM", onSigterm);

    signalSource.once("SIGINT", onSigint);
  });
}
