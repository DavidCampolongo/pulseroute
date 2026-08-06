import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { executeRouteServiceRequest } from "../src/routing-workflow.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for routing rollback tests");
}

const database = createDatabaseClient(databaseUrl);

const rollbackCorrelationId = "phase7-routing-rollback-injection";

type RoutingRollbackFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
};

async function removeRollbackConstraint(): Promise<void> {
  await database.$executeRaw`
    ALTER TABLE "outbox_events"
    DROP CONSTRAINT IF EXISTS
      "outbox_events_phase7_routing_rollback_failure"
  `;
}

async function installRollbackConstraint(): Promise<void> {
  await removeRollbackConstraint();

  await database.$executeRaw`
    ALTER TABLE "outbox_events"
    ADD CONSTRAINT
      "outbox_events_phase7_routing_rollback_failure"
    CHECK (
      (payload ->> 'correlationId')
      IS DISTINCT FROM
      'phase7-routing-rollback-injection'
    )
  `;
}

async function createRoutingRollbackFixture(): Promise<RoutingRollbackFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Routing Rollback Test Org ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Rollback Test Skill ${skillId}`,
    },
  });

  await database.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Routing Rollback Test Operator ${operatorId}`,
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
      level: 5,
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `routing-rollback-${serviceRequestId}`,
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

async function clearRoutingRollbackFixture(
  fixture: RoutingRollbackFixture,
): Promise<void> {
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

beforeAll(async () => {
  await removeRollbackConstraint();
});

afterAll(async () => {
  await removeRollbackConstraint();
  await database.$disconnect();
});

describe("routing transaction rollback", () => {
  it("rolls back every routing write when notification outbox creation fails", async () => {
    const fixture = await createRoutingRollbackFixture();

    try {
      await installRollbackConstraint();

      let routingError: unknown;

      try {
        await executeRouteServiceRequest(database, {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
          correlationId: rollbackCorrelationId,
        });
      } catch (error) {
        routingError = error;
      }

      expect(routingError).toBeDefined();

      const [
        assignmentCountAfterFailure,
        routingDecisionCountAfterFailure,
        outboxEventCountAfterFailure,
        serviceRequestAfterFailure,
        operatorLoadAfterFailure,
      ] = await Promise.all([
        database.assignment.count({
          where: {
            organizationId: fixture.organizationId,
            serviceRequestId: fixture.serviceRequestId,
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
          },
        }),
        database.serviceRequest.findUniqueOrThrow({
          where: {
            id: fixture.serviceRequestId,
          },
        }),
        database.assignment.count({
          where: {
            organizationId: fixture.organizationId,
            operatorId: fixture.operatorId,
            status: "ACTIVE",
          },
        }),
      ]);

      expect(assignmentCountAfterFailure).toBe(0);
      expect(routingDecisionCountAfterFailure).toBe(0);
      expect(outboxEventCountAfterFailure).toBe(0);
      expect(serviceRequestAfterFailure.status).toBe("PENDING");
      expect(operatorLoadAfterFailure).toBe(0);

      await removeRollbackConstraint();

      const recoveryResult = await executeRouteServiceRequest(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `routing-rollback-recovery-${randomUUID()}`,
      });

      expect(recoveryResult.kind).toBe("assigned");

      if (recoveryResult.kind !== "assigned") {
        throw new Error(
          "Expected routing to succeed after rollback constraint removal",
        );
      }

      const [
        assignmentCountAfterRecovery,
        routingDecisionCountAfterRecovery,
        outboxEventCountAfterRecovery,
        serviceRequestAfterRecovery,
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

      expect(assignmentCountAfterRecovery).toBe(1);
      expect(routingDecisionCountAfterRecovery).toBe(1);
      expect(outboxEventCountAfterRecovery).toBe(1);
      expect(serviceRequestAfterRecovery.status).toBe("ASSIGNED");
    } finally {
      await removeRollbackConstraint();
      await clearRoutingRollbackFixture(fixture);
    }
  });
});
