export const PROJECT_NAME = "PulseRoute";

export const EVENT_TYPES = {
  serviceRequestCreated: "service_request.created",
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

export const QUEUE_NAMES = {
  incomingEvents: "incoming-events",
  routing: "routing",
  notifications: "notifications",
  webhookDelivery: "webhook-delivery",
  deadLetter: "dead-letter",
} as const;

export type QueueName = (typeof QUEUE_NAMES)[keyof typeof QUEUE_NAMES];

export const JOB_NAMES = {
  serviceRequestIngested: "service-request-ingested",
  routeServiceRequest: "route-service-request",
  deadLetteredJob: "dead-lettered-job",
} as const;

export type JobName = (typeof JOB_NAMES)[keyof typeof JOB_NAMES];

export type ServiceRequestIngestedJobData = {
  outboxEventId: string;
  organizationId: string;
  serviceRequestId: string;
  correlationId: string;
};

export type RouteServiceRequestJobData = {
  organizationId: string;
  serviceRequestId: string;
  correlationId: string;
};

export type DeadLetteredJobData = {
  sourceQueue: QueueName;
  sourceJobId: string;
  sourceJobName: string;
  organizationId: string | null;
  serviceRequestId: string | null;
  correlationId: string | null;
  attemptsMade: number;
  failureReason: string;
  failedAt: string;
};
