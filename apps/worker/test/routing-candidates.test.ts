import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it } from "vitest";

import { loadRoutingCandidates } from "../src/routing-candidates.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for routing candidate tests");
}

const database = createDatabaseClient(databaseUrl);

type CandidateFixture = {
  organizationId: string;
  skillId: string;
  operatorId: string;
  serviceRequestId: string;
};

type CandidateFixtureOptions = {
  maxConcurrentAssignments: number;
  requiredSkillLevel: number;
  activeAssignmentCount: number;
};

async function createCandidateFixture(
  options: CandidateFixtureOptions,
): Promise<CandidateFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const operatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Routing Candidate Test Org ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Candidate Test Skill ${skillId}`,
    },
  });

  await database.operator.create({
    data: {
      id: operatorId,
      organizationId,
      name: `Routing Candidate Test Operator ${operatorId}`,
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
      externalId: `routing-candidate-target-${serviceRequestId}`,
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
        externalId: `routing-candidate-active-${activeRequestId}`,
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

async function clearCandidateFixture(fixture: CandidateFixture): Promise<void> {
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

describe("loadRoutingCandidates", () => {
  it("loads decision-time facts for a routable request", async () => {
    const fixture = await createCandidateFixture({
      maxConcurrentAssignments: 2,
      requiredSkillLevel: 4,
      activeAssignmentCount: 0,
    });

    try {
      const rows = await loadRoutingCandidates(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      });

      expect(rows).toEqual([
        {
          operatorId: fixture.operatorId,
          organizationId: fixture.organizationId,
          status: "AVAILABLE",
          region: "WEST",
          maxConcurrentAssignments: 2,
          activeAssignmentCount: 0,
          requiredSkillId: fixture.skillId,
          requiredSkillLevel: 4,
          hasRequiredSkill: true,
        },
      ]);
    } finally {
      await clearCandidateFixture(fixture);
    }
  });

  it("loads the at-capacity candidate facts for an unroutable request", async () => {
    const fixture = await createCandidateFixture({
      maxConcurrentAssignments: 1,
      requiredSkillLevel: 5,
      activeAssignmentCount: 1,
    });

    try {
      const rows = await loadRoutingCandidates(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      });

      expect(rows).toEqual([
        {
          operatorId: fixture.operatorId,
          organizationId: fixture.organizationId,
          status: "AVAILABLE",
          region: "WEST",
          maxConcurrentAssignments: 1,
          activeAssignmentCount: 1,
          requiredSkillId: fixture.skillId,
          requiredSkillLevel: 5,
          hasRequiredSkill: true,
        },
      ]);
    } finally {
      await clearCandidateFixture(fixture);
    }
  });

  it("returns equal rows across repeated evaluation", async () => {
    const fixture = await createCandidateFixture({
      maxConcurrentAssignments: 2,
      requiredSkillLevel: 4,
      activeAssignmentCount: 0,
    });

    try {
      const first = await loadRoutingCandidates(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      });

      const second = await loadRoutingCandidates(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      });

      expect(first).toEqual(second);
      expect(first[0]?.operatorId).toBe(fixture.operatorId);
    } finally {
      await clearCandidateFixture(fixture);
    }
  });
});
