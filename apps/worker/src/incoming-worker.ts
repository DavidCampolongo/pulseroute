import type { DatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  QUEUE_NAMES,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { type Job, type Processor, type Queue, Worker } from "bullmq";
import type { Logger } from "pino";
import { z } from "zod";

import { createJobLogger } from "./logger.js";
import { createWorkerRedisOptions } from "./redis.js";

const DEFAULT_INCOMING_CONCURRENCY = 5;

const incomingJobDataSchema = z.strictObject({
  outboxEventId: z.uuid(),
  organizationId: z.uuid(),
  serviceRequestId: z.uuid(),
  correlationId: z.string().trim().min(1).max(200),
});

export type IncomingProcessorResult = {
  routingJobId: string;
};

type IncomingProcessorOptions = {
  database: DatabaseClient;
  routingQueue: Queue<RouteServiceRequestJobData>;
  logger: Logger;
};

type IncomingWorkerOptions = IncomingProcessorOptions & {
  redisUrl: string;
  concurrency?: number;
};

export function createRoutingJobId(serviceRequestId: string): string {
  return `route-${serviceRequestId}`;
}

function parseIncomingJobData(
  job: Job<ServiceRequestIngestedJobData>,
): ServiceRequestIngestedJobData {
  if (job.name !== JOB_NAMES.serviceRequestIngested) {
    throw new Error(`Unsupported incoming job name: ${job.name}`);
  }

  const result = incomingJobDataSchema.safeParse(job.data);

  if (!result.success) {
    const problems = result.error.issues.map((issue) => {
      const field = issue.path.length > 0 ? issue.path.join(".") : "jobData";

      return `${field}: ${issue.message}`;
    });

    throw new Error(
      [
        "Invalid incoming job data:",
        ...problems.map((problem) => `- ${problem}`),
      ].join("\n"),
    );
  }

  return result.data;
}

export function createIncomingProcessor(
  options: IncomingProcessorOptions,
): Processor<ServiceRequestIngestedJobData, IncomingProcessorResult, string> {
  return async (job) => {
    const startedAt = Date.now();
    const jobData = parseIncomingJobData(job);
    const jobId = job.id ?? "unknown";

    const jobLogger = createJobLogger(options.logger, {
      queue: QUEUE_NAMES.incomingEvents,
      jobName: job.name,
      jobId,
      attemptsMade: job.attemptsMade,
      organizationId: jobData.organizationId,
      serviceRequestId: jobData.serviceRequestId,
      correlationId: jobData.correlationId,
    });

    const serviceRequest = await options.database.serviceRequest.findFirst({
      where: {
        id: jobData.serviceRequestId,
        organizationId: jobData.organizationId,
      },
      select: {
        id: true,
        status: true,
      },
    });

    if (!serviceRequest) {
      throw new Error("ServiceRequest does not exist for the incoming job");
    }

    const routingJobId = createRoutingJobId(serviceRequest.id);

    const routingJobData: RouteServiceRequestJobData = {
      organizationId: jobData.organizationId,
      serviceRequestId: serviceRequest.id,
      correlationId: jobData.correlationId,
    };

    await options.routingQueue.add(
      JOB_NAMES.routeServiceRequest,
      routingJobData,
      {
        jobId: routingJobId,
      },
    );

    jobLogger.info(
      {
        outcome: "routing_job_ready",
        outboxEventId: jobData.outboxEventId,
        routingJobId,
        serviceRequestStatus: serviceRequest.status,
        durationMs: Date.now() - startedAt,
      },
      "Incoming job completed",
    );

    return {
      routingJobId,
    };
  };
}

export function createIncomingWorker(
  options: IncomingWorkerOptions,
): Worker<ServiceRequestIngestedJobData, IncomingProcessorResult, string> {
  const concurrency = options.concurrency ?? DEFAULT_INCOMING_CONCURRENCY;

  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new Error("Incoming worker concurrency must be a positive integer");
  }

  return new Worker<
    ServiceRequestIngestedJobData,
    IncomingProcessorResult,
    string
  >(QUEUE_NAMES.incomingEvents, createIncomingProcessor(options), {
    connection: createWorkerRedisOptions(options.redisUrl),
    concurrency,
  });
}
