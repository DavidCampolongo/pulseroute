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

type RoutingFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
};

type RoutingFixtureOptions = {
  maxConcurrentAssignments: number;
  requiredSkillLevel: number;
  activeAssignmentCount: number;
};

async function createRoutingFixture(
  options: RoutingFixtureOptions,
): Promise<RoutingFixture> {
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
      maxConcurrentAssignments: options.maxConcurrentAssignments,
    },
  });

  await database.operatorSkill.create({
    data: {
      organizationId,
      operatorId,
      skillId,
      level: options.requiredSkillLevel,
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `routing-assignment-target-${serviceRequestId}`,
      requiredSkillId: skillId,
      status: "PENDING",
      priority: "NORMAL",
      region: "WEST",
    },
  });

  for (let index = 0; index < options.activeAssignmentCount; index += 1) {
    const activeRequestId = randomUUID();

    await database.serviceRequest.create({
      data: {
        id: activeRequestId,
        organizationId,
        externalId: `routing-assignment-active-${activeRequestId}`,
        requiredSkillId: skillId,
        status: "ASSIGNED",
        priority: "NORMAL",
        region: "WEST",
      },
    });

    await database.assignment.create({
      data: {
        id: randomUUID(),
        organizationId,
        serviceRequestId: activeRequestId,
        operatorId,
        status: "ACTIVE",
      },
    });
  }

  return {
    organizationId,
    skillId,
    operatorId,
    serviceRequestId,
  };
}

async function clearRoutingFixture(fixture: RoutingFixture): Promise<void> {
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

describe("executeRouteServiceRequest", () => {
  it("creates the assignment, routing decision, request transition, and outbox intent in one transaction", async () => {
    const fixture = await createRoutingFixture({
      maxConcurrentAssignments: 2,
      requiredSkillLevel: 4,
      activeAssignmentCount: 0,
    });

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
    const fixture = await createRoutingFixture({
      maxConcurrentAssignments: 1,
      requiredSkillLevel: 5,
      activeAssignmentCount: 1,
    });

    try {
      const result = await executeRouteServiceRequest(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
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
          id: fixture.serviceRequestId,
        },
      });

      const targetAssignments = await database.assignment.findMany({
        where: {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
        },
      });

      const targetOutboxEvents = await database.outboxEvent.findMany({
        where: {
          organizationId: fixture.organizationId,
          aggregateId: fixture.serviceRequestId,
        },
      });

      const activeOperatorAssignments = await database.assignment.count({
        where: {
          organizationId: fixture.organizationId,
          operatorId: fixture.operatorId,
          status: "ACTIVE",
        },
      });

      expect(result.rejectionReasons).toContain("AT_CAPACITY");

      expect(routingDecision).toMatchObject({
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        assignmentId: null,
        scoringVersion: "phase-7-stub-v1",
        outcome: "UNROUTABLE",
      });

      expect(serviceRequest.status).toBe("UNROUTABLE");
      expect(targetAssignments).toHaveLength(0);
      expect(targetOutboxEvents).toHaveLength(0);
      expect(activeOperatorAssignments).toBe(1);

      expect(routingDecision.decisionSnapshot).toMatchObject({
        scoringVersion: "phase-7-stub-v1",
        request: {
          id: fixture.serviceRequestId,
          organizationId: fixture.organizationId,
          requiredSkillId: fixture.skillId,
          status: "PENDING",
          priority: "NORMAL",
          region: "WEST",
        },
        result: {
          outcome: "UNROUTABLE",
          selectedOperatorId: null,
        },
      });
    } finally {
      await clearRoutingFixture(fixture);
    }
  });
});
