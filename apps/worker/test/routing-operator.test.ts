import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it } from "vitest";

import { loadRoutingCandidates } from "../src/routing-candidates.js";
import { lockAndRecheckSelectedOperator } from "../src/routing-operator.js";
import type { LockedServiceRequest } from "../src/routing-transaction.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for routing operator tests");
}

const database = createDatabaseClient(databaseUrl);

type OperatorFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
};

type OperatorFixtureOptions = {
  maxConcurrentAssignments: number;
  requiredSkillLevel: number;
  activeAssignmentCount: number;
};

async function createOperatorFixture(
  options: OperatorFixtureOptions,
): Promise<OperatorFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Routing Operator Test Org ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Operator Test Skill ${skillId}`,
    },
  });

  await database.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Routing Operator Test Operator ${operatorId}`,
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
      externalId: `routing-operator-target-${serviceRequestId}`,
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
        externalId: `routing-operator-active-${activeRequestId}`,
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

async function clearOperatorFixture(fixture: OperatorFixture): Promise<void> {
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

describe("lockAndRecheckSelectedOperator", () => {
  it("accepts an eligible operator after locking and rechecking", async () => {
    const fixture = await createOperatorFixture({
      maxConcurrentAssignments: 2,
      requiredSkillLevel: 4,
      activeAssignmentCount: 0,
    });

    try {
      const request: LockedServiceRequest = {
        id: fixture.serviceRequestId,
        organizationId: fixture.organizationId,
        status: "PENDING",
        requiredSkillId: fixture.skillId,
        priority: "NORMAL",
        region: "WEST",
      };

      const candidates = await loadRoutingCandidates(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      });

      expect(candidates).toHaveLength(1);

      const result = await lockAndRecheckSelectedOperator(
        database,
        request,
        candidates[0]!,
      );

      expect(result).toEqual({
        kind: "accepted",
        operator: {
          operatorId: fixture.operatorId,
          organizationId: fixture.organizationId,
          status: "AVAILABLE",
          region: "WEST",
          maxConcurrentAssignments: 2,
        },
        activeAssignmentCount: 0,
        requiredSkillLevel: 4,
      });
    } finally {
      await clearOperatorFixture(fixture);
    }
  });

  it("rejects an operator that is at capacity under the lock", async () => {
    const fixture = await createOperatorFixture({
      maxConcurrentAssignments: 1,
      requiredSkillLevel: 5,
      activeAssignmentCount: 1,
    });

    try {
      const request: LockedServiceRequest = {
        id: fixture.serviceRequestId,
        organizationId: fixture.organizationId,
        status: "PENDING",
        requiredSkillId: fixture.skillId,
        priority: "HIGH",
        region: "WEST",
      };

      const candidates = await loadRoutingCandidates(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      });

      expect(candidates).toHaveLength(1);

      const result = await lockAndRecheckSelectedOperator(
        database,
        request,
        candidates[0]!,
      );

      expect(result).toEqual({
        kind: "rejected",
        operator: {
          operatorId: fixture.operatorId,
          organizationId: fixture.organizationId,
          status: "AVAILABLE",
          region: "WEST",
          maxConcurrentAssignments: 1,
        },
        activeAssignmentCount: 1,
        requiredSkillLevel: 5,
        rejectionReasons: ["AT_CAPACITY"],
      });
    } finally {
      await clearOperatorFixture(fixture);
    }
  });
});
