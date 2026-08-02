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
  throw new Error("DATABASE_URL is required for routing assignment tests");
}

const database = createDatabaseClient(databaseUrl);

async function createRoutingFixture(): Promise<{
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
}> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Routing Assignment Test Org ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Assignment Test Skill ${skillId}`,
    },
  });

  await database.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Routing Assignment Test Operator ${operatorId}`,
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
      externalId: `routing-assignment-${serviceRequestId}`,
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

async function clearRoutingFixture(fixture: {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
}): Promise<void> {
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
      id: fixture.serviceRequestId,
      organizationId: fixture.organizationId,
    },
  });

  await database.operatorSkill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
      operatorId: fixture.operatorId,
      skillId: fixture.skillId,
    },
  });

  await database.operator.deleteMany({
    where: {
      id: fixture.operatorId,
      organizationId: fixture.organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      id: fixture.skillId,
      organizationId: fixture.organizationId,
    },
  });

  await database.organization.deleteMany({
    where: {
      id: fixture.organizationId,
    },
  });
}

async function clearSeedRoutingOutcome(
  serviceRequestId: string,
): Promise<void> {
  const organizationId = "00000001-0000-4000-8000-000000000001";

  await database.webhookDelivery.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.outboxEvent.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.routingDecision.deleteMany({
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
  await database.$disconnect();
});

describe("executeRouteServiceRequest", () => {
  it("creates the assignment, routing decision, request transition, and outbox intent in one transaction", async () => {
    const fixture = await createRoutingFixture();

    try {
      const result = await executeRouteServiceRequest(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `request-${randomUUID()}`,
      });

      expect(result.kind).toBe("assigned");

      if (result.kind !== "assigned") {
        throw new Error("Expected an assigned routing result");
      }

      const assignment = await database.assignment.findUniqueOrThrow({
        where: {
          id: result.assignmentId,
        },
      });

      const routingDecision = await database.routingDecision.findUniqueOrThrow({
        where: {
          id: result.routingDecisionId,
        },
      });

      const serviceRequest = await database.serviceRequest.findUniqueOrThrow({
        where: {
          id: fixture.serviceRequestId,
        },
      });

      const outboxEvent = await database.outboxEvent.findUniqueOrThrow({
        where: {
          id: result.outboxEventId,
        },
      });

      expect(assignment).toMatchObject({
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        operatorId: fixture.operatorId,
        status: "ACTIVE",
      });

      expect(routingDecision).toMatchObject({
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        assignmentId: assignment.id,
        scoringVersion: "phase-7-stub-v1",
        outcome: "ASSIGNED",
      });

      expect(serviceRequest.status).toBe("ASSIGNED");

      expect(outboxEvent).toMatchObject({
        organizationId: fixture.organizationId,
        eventType: "service_request.assigned",
        aggregateType: "service_request",
        aggregateId: fixture.serviceRequestId,
        status: "PENDING",
      });

      expect(outboxEvent.payload).toMatchObject({
        serviceRequestId: fixture.serviceRequestId,
        organizationId: fixture.organizationId,
        operatorId: fixture.operatorId,
        assignmentId: assignment.id,
        routingDecisionId: routingDecision.id,
        scoringVersion: "phase-7-stub-v1",
      });

      expect(routingDecision.decisionSnapshot).toMatchObject({
        scoringVersion: "phase-7-stub-v1",
        request: {
          id: fixture.serviceRequestId,
          organizationId: fixture.organizationId,
          requiredSkillId: fixture.skillId,
          priority: "NORMAL",
          region: "WEST",
          status: "PENDING",
        },
        result: {
          outcome: "ASSIGNED",
          selectedOperatorId: fixture.operatorId,
        },
      });
    } finally {
      await clearRoutingFixture(fixture);
    }
  });

  it("creates an unroutable routing decision with no assignment and no notification outbox event", async () => {
    const serviceRequestId = "00000005-0000-4000-8000-000000000010";
    const organizationId = "00000001-0000-4000-8000-000000000001";

    const result = await executeRouteServiceRequest(database, {
      organizationId,
      serviceRequestId,
      correlationId: `request-${randomUUID()}`,
    });

    expect(result.kind).toBe("unroutable");

    if (result.kind !== "unroutable") {
      throw new Error("Expected an unroutable routing result");
    }

    const routingDecision = await database.routingDecision.findUniqueOrThrow({
      where: {
        id: result.routingDecisionId,
      },
    });

    const serviceRequest = await database.serviceRequest.findUniqueOrThrow({
      where: {
        id: serviceRequestId,
      },
    });

    const assignments = await database.assignment.findMany({
      where: {
        organizationId,
        serviceRequestId,
      },
    });

    const outboxEvents = await database.outboxEvent.findMany({
      where: {
        organizationId,
        aggregateId: serviceRequestId,
      },
    });

    expect(result.rejectionReasons).toContain("AT_CAPACITY");
    expect(routingDecision.outcome).toBe("UNROUTABLE");
    expect(routingDecision.assignmentId).toBeNull();
    expect(serviceRequest.status).toBe("UNROUTABLE");
    expect(assignments).toHaveLength(0);
    expect(outboxEvents).toHaveLength(0);

    expect(routingDecision.decisionSnapshot).toMatchObject({
      scoringVersion: "phase-7-stub-v1",
      request: {
        id: serviceRequestId,
        organizationId,
        status: "PENDING",
        region: "WEST",
      },
      result: {
        outcome: "UNROUTABLE",
        selectedOperatorId: null,
      },
    });

    await clearSeedRoutingOutcome(serviceRequestId);
  });
});
