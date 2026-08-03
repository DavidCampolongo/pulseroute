import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it } from "vitest";

import { executeRouteServiceRequest } from "../src/routing-workflow.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for routing replay tests");
}

const database = createDatabaseClient(databaseUrl);

const organizationId = "00000001-0000-4000-8000-000000000001";
const serviceRequestId = "00000005-0000-4000-8000-000000000012";
const operatorId = "00000004-0000-4000-8000-000000000016";

async function resetSeedRequestState(): Promise<void> {
  await database.webhookDelivery.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.outboxEvent.deleteMany({
    where: {
      organizationId,
      aggregateId: serviceRequestId,
    },
  });

  await database.routingDecision.deleteMany({
    where: {
      organizationId,
      serviceRequestId,
    },
  });

  await database.assignment.deleteMany({
    where: {
      organizationId,
      serviceRequestId,
    },
  });

  await database.serviceRequest.update({
    where: {
      id: serviceRequestId,
    },
    data: {
      status: "PENDING",
    },
  });
}

afterAll(async () => {
  await resetSeedRequestState().catch(() => undefined);
  await database.$disconnect();
});

describe("routing replay", () => {
  it("routes once and then no-ops on the duplicate request", async () => {
    const correlationId = `request-${randomUUID()}`;
    const jobData = {
      organizationId,
      serviceRequestId,
      correlationId,
    };

    try {
      const firstResult = await executeRouteServiceRequest(database, jobData);

      expect(firstResult.kind).toBe("assigned");

      if (firstResult.kind !== "assigned") {
        throw new Error("Expected the first routing attempt to assign");
      }

      const secondResult = await executeRouteServiceRequest(database, jobData);

      expect(secondResult).toEqual({
        kind: "already_processed",
        organizationId,
        serviceRequestId,
        terminalStatus: "ASSIGNED",
      });

      const [
        assignmentCount,
        routingDecisionCount,
        outboxEventCount,
        serviceRequest,
      ] = await Promise.all([
        database.assignment.count({
          where: {
            organizationId,
            serviceRequestId,
            status: "ACTIVE",
          },
        }),
        database.routingDecision.count({
          where: {
            organizationId,
            serviceRequestId,
          },
        }),
        database.outboxEvent.count({
          where: {
            organizationId,
            aggregateId: serviceRequestId,
            eventType: "service_request.assigned",
          },
        }),
        database.serviceRequest.findUniqueOrThrow({
          where: {
            id: serviceRequestId,
          },
        }),
      ]);

      expect(assignmentCount).toBe(1);
      expect(routingDecisionCount).toBe(1);
      expect(outboxEventCount).toBe(1);
      expect(serviceRequest.status).toBe("ASSIGNED");

      const assignment = await database.assignment.findFirstOrThrow({
        where: {
          organizationId,
          serviceRequestId,
        },
      });

      expect(assignment.operatorId).toBe(operatorId);
    } finally {
      await resetSeedRequestState();
    }
  });
});
