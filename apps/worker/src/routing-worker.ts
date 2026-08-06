import { type DatabaseClient } from "@pulseroute/db";
import {
  QUEUE_NAMES,
  type DeadLetteredJobData,
  type RouteServiceRequestJobData,
} from "@pulseroute/shared";
import { type Processor, Queue, Worker } from "bullmq";
import type { Logger } from "pino";
import { z } from "zod";

import { createDeadLetteringProcessor } from "./dead-letter.js";
import { createJobLogger } from "./logger.js";
import { createWorkerRedisOptions } from "./redis.js";
import {
  executeRouteServiceRequest,
  type RoutingAssignmentResult,
} from "./routing-workflow.js";

const routingJobSchema = z.object({
  organizationId: z.uuid(),
  serviceRequestId: z.uuid(),
  correlationId: z.string().trim().min(1).max(200),
});

export type RoutingProcessorOptions = {
  database: DatabaseClient;
  logger: Logger;
};

export function createRoutingProcessor(
  options: RoutingProcessorOptions,
): Processor<RouteServiceRequestJobData, RoutingAssignmentResult, string> {
  return async (job) => {
    const startedAt = Date.now();
    const parsedJobData = routingJobSchema.parse(job.data);

    const jobLogger = createJobLogger(options.logger, {
      queue: QUEUE_NAMES.routing,
      jobName: job.name,
      jobId: job.id ?? "unknown",
      attemptsMade: job.attemptsMade,
      organizationId: parsedJobData.organizationId,
      serviceRequestId: parsedJobData.serviceRequestId,
      correlationId: parsedJobData.correlationId,
    });

    const result = await executeRouteServiceRequest(options.database, job.data);

    if (result.kind === "assigned") {
      jobLogger.info(
        {
          routingOutcome: result.kind,
          selectedOperatorId: result.operatorId,
          assignmentId: result.assignmentId,
          routingDecisionId: result.routingDecisionId,
          outboxEventId: result.outboxEventId,
          scoringVersion: result.scoringVersion,
          durationMs: Date.now() - startedAt,
        },
        "Routing job assigned",
      );
    } else if (result.kind === "unroutable") {
      jobLogger.info(
        {
          routingOutcome: result.kind,
          routingDecisionId: result.routingDecisionId,
          rejectionReasons: result.rejectionReasons,
          scoringVersion: result.scoringVersion,
          durationMs: Date.now() - startedAt,
        },
        "Routing job unroutable",
      );
    } else {
      jobLogger.info(
        {
          routingOutcome: result.kind,
          terminalStatus: result.terminalStatus,
          durationMs: Date.now() - startedAt,
        },
        "Routing job already processed",
      );
    }

    return result;
  };
}

export type RoutingWorkerOptions = RoutingProcessorOptions & {
  redisUrl: string;
  deadLetterQueue: Queue<DeadLetteredJobData>;
  concurrency?: number;
};

export function createRoutingWorker(
  options: RoutingWorkerOptions,
): Worker<RouteServiceRequestJobData, RoutingAssignmentResult, string> {
  const processor = createDeadLetteringProcessor({
    sourceQueue: QUEUE_NAMES.routing,
    deadLetterQueue: options.deadLetterQueue,
    processor: createRoutingProcessor({
      database: options.database,
      logger: options.logger,
    }),
    logger: options.logger,
  });

  return new Worker<
    RouteServiceRequestJobData,
    RoutingAssignmentResult,
    string
  >(QUEUE_NAMES.routing, processor, {
    connection: createWorkerRedisOptions(options.redisUrl),
    concurrency: options.concurrency ?? 5,
  });
}
