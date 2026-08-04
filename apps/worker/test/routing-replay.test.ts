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

type ReplayFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
};

async function createReplayFixture(): Promise<ReplayFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Routing Replay Test Org ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Replay Test Skill ${skillId}`,
    },
  });

  await database.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Routing Replay Test Operator ${operatorId}`,
      status: "AVAILABLE",
      region: "WEST",
      maxConcurrentAssignments: 2,
    },
  });

  await database.operatorSkill.create({
    data: {
      organizationId,
      operatorId,
      skillId,
      level: 4,
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `routing-replay-${serviceRequestId}`,
      requiredSkillId: skillId,
      status: "PENDING",
      priority: "NORMAL",
      region: "WEST",
    },
  });

  return {
    organizationId,
    skillId,
    operatorId,
    serviceRequestId,
  };
}

async function clearReplayFixture(fixture: ReplayFixture): Promise<void> {
  await database.webhookDelivery.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.outboxEvent.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.routingDecision.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.assignment.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.serviceRequest.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.operatorSkill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.operator.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.organization.deleteMany({
    where: {
      id: fixture.organizationId,
    },
  });
}

afterAll(async () => {
  await database.$disconnect();
});

describe("routing replay", () => {
  it("routes once and then no-ops on the duplicate request", async () => {
    const fixture = await createReplayFixture();

    try {
      const jobData = {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `request-${randomUUID()}`,
      };

      const firstResult = await executeRouteServiceRequest(database, jobData);

      expect(firstResult.kind).toBe("assigned");

      if (firstResult.kind !== "assigned") {
        throw new Error("Expected the first routing attempt to assign");
      }

      const secondResult = await executeRouteServiceRequest(database, jobData);

      expect(secondResult).toEqual({
        kind: "already_processed",
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
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
            organizationId: fixture.organizationId,
            serviceRequestId: fixture.serviceRequestId,
            status: "ACTIVE",
          },
        }),
        database.routingDecision.count({
          where: {
            organizationId: fixture.organizationId,
            serviceRequestId: fixture.serviceRequestId,
          },
        }),
        database.outboxEvent.count({
          where: {
            organizationId: fixture.organizationId,
            aggregateId: fixture.serviceRequestId,
            eventType: "service_request.assigned",
          },
        }),
        database.serviceRequest.findUniqueOrThrow({
          where: {
            id: fixture.serviceRequestId,
          },
        }),
      ]);

      expect(assignmentCount).toBe(1);
      expect(routingDecisionCount).toBe(1);
      expect(outboxEventCount).toBe(1);
      expect(serviceRequest.status).toBe("ASSIGNED");

      const assignment = await database.assignment.findFirstOrThrow({
        where: {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
        },
      });

      expect(assignment.operatorId).toBe(fixture.operatorId);
    } finally {
      await clearReplayFixture(fixture);
    }
  });
});
