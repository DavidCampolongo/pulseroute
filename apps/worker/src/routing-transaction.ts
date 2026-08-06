import { Prisma, type DatabaseClient } from "@pulseroute/db";
import { type RouteServiceRequestJobData } from "@pulseroute/shared";
import { z } from "zod";

const routeServiceRequestJobSchema = z.object({
  organizationId: z.uuid(),
  serviceRequestId: z.uuid(),
  correlationId: z.string().trim().min(1).max(200),
});

export type RouteServiceRequestJobInput = RouteServiceRequestJobData;

export type LockedServiceRequestStatus =
  "PENDING" | "ASSIGNED" | "UNROUTABLE" | "CANCELLED";

export type LockedServiceRequest = {
  id: string;
  organizationId: string;
  status: LockedServiceRequestStatus;
  requiredSkillId: string;
  priority: "LOW" | "NORMAL" | "HIGH";
  region: string;
};

export type RouteServiceRequestOutcome =
  | {
      kind: "pending_locked";
      request: LockedServiceRequest;
    }
  | {
      kind: "terminal_no_op";
      request: LockedServiceRequest;
      terminalStatus: Exclude<LockedServiceRequestStatus, "PENDING">;
    };

export class RouteServiceRequestJobDataError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "RouteServiceRequestJobDataError";
  }
}

export class RouteServiceRequestMissingError extends Error {
  constructor(serviceRequestId: string) {
    super(`ServiceRequest ${serviceRequestId} does not exist`);
    this.name = "RouteServiceRequestMissingError";
  }
}

export class RouteServiceRequestTenantMismatchError extends Error {
  constructor(serviceRequestId: string, organizationId: string) {
    super(
      `ServiceRequest ${serviceRequestId} belongs to a different organization than ${organizationId}`,
    );
    this.name = "RouteServiceRequestTenantMismatchError";
  }
}

export function parseRouteServiceRequestJobData(
  data: unknown,
): RouteServiceRequestJobInput {
  const result = routeServiceRequestJobSchema.safeParse(data);

  if (!result.success) {
    throw new RouteServiceRequestJobDataError(
      "Invalid route-service-request job data",
      {
        cause: result.error,
      },
    );
  }

  return result.data;
}

function isTerminalStatus(
  status: LockedServiceRequestStatus,
): status is Exclude<LockedServiceRequestStatus, "PENDING"> {
  return status !== "PENDING";
}

type QueryableDatabase = Pick<DatabaseClient, "$queryRaw">;

type LockedServiceRequestRow = {
  id: string;
  organizationId: string;
  status: LockedServiceRequestStatus;
  requiredSkillId: string;
  priority: "LOW" | "NORMAL" | "HIGH";
  region: string;
};

export async function lockRouteServiceRequest(
  database: QueryableDatabase,
  jobData: RouteServiceRequestJobInput,
): Promise<RouteServiceRequestOutcome> {
  const lockedRows = await database.$queryRaw<LockedServiceRequestRow[]>(
    Prisma.sql`
      SELECT
        id,
        organization_id AS "organizationId",
        status,
        required_skill_id AS "requiredSkillId",
        priority,
        region
      FROM service_requests
      WHERE id = ${jobData.serviceRequestId}::uuid
        AND organization_id = ${jobData.organizationId}::uuid
      FOR UPDATE
    `,
  );

  const lockedRow = lockedRows[0];

  if (lockedRow) {
    if (isTerminalStatus(lockedRow.status)) {
      return {
        kind: "terminal_no_op",
        request: lockedRow,
        terminalStatus: lockedRow.status,
      };
    }

    return {
      kind: "pending_locked",
      request: lockedRow,
    };
  }

  const existingRequest = await database.$queryRaw<
    { id: string; organizationId: string }[]
  >(Prisma.sql`
      SELECT
        id,
        organization_id AS "organizationId"
      FROM service_requests
      WHERE id = ${jobData.serviceRequestId}::uuid
      LIMIT 1
    `);

  if (existingRequest[0]) {
    throw new RouteServiceRequestTenantMismatchError(
      jobData.serviceRequestId,
      jobData.organizationId,
    );
  }

  throw new RouteServiceRequestMissingError(jobData.serviceRequestId);
}

export async function prepareRouteServiceRequest(
  database: DatabaseClient,
  jobData: unknown,
): Promise<RouteServiceRequestOutcome> {
  const parsedJobData = parseRouteServiceRequestJobData(jobData);

  return database.$transaction(async (tx) =>
    lockRouteServiceRequest(tx, parsedJobData),
  );
}
