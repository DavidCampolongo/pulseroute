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

const HISTORICAL_ASSIGNMENT_AT = new Date("2026-07-20T12:00:00.000Z");
const ACTIVE_ASSIGNMENT_AT = new Date("2026-08-01T12:00:00.000Z");

type CandidateAuditFixture = {
  organizationId: string;
  otherOrganizationId: string;
  skillId: string;
  serviceRequestId: string;
  operatorIds: {
    eligible: string;
    unavailable: string;
    wrongRegion: string;
    missingSkill: string;
    atCapacity: string;
    otherOrganization: string;
  };
};

async function createCandidateAuditFixture(): Promise<CandidateAuditFixture> {
  const organizationId = randomUUID();
  const otherOrganizationId = randomUUID();
  const skillId = randomUUID();
  const otherSkillId = randomUUID();
  const serviceRequestId = randomUUID();

  const operatorIds = {
    eligible: randomUUID(),
    unavailable: randomUUID(),
    wrongRegion: randomUUID(),
    missingSkill: randomUUID(),
    atCapacity: randomUUID(),
    otherOrganization: randomUUID(),
  };

  await database.organization.createMany({
    data: [
      {
        id: organizationId,
        name: `Routing Candidate Audit Org ${organizationId}`,
      },
      {
        id: otherOrganizationId,
        name: `Routing Candidate Other Org ${otherOrganizationId}`,
      },
    ],
  });

  await database.skill.createMany({
    data: [
      {
        id: skillId,
        organizationId,
        name: `Routing Candidate Audit Skill ${skillId}`,
      },
      {
        id: otherSkillId,
        organizationId: otherOrganizationId,
        name: `Routing Candidate Other Skill ${otherSkillId}`,
      },
    ],
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

  await database.operator.createMany({
    data: [
      {
        id: operatorIds.eligible,
        organizationId,
        name: "Eligible Operator",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 4,
      },
      {
        id: operatorIds.unavailable,
        organizationId,
        name: "Unavailable Operator",
        status: "UNAVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 4,
      },
      {
        id: operatorIds.wrongRegion,
        organizationId,
        name: "Wrong Region Operator",
        status: "AVAILABLE",
        region: "EAST",
        maxConcurrentAssignments: 4,
      },
      {
        id: operatorIds.missingSkill,
        organizationId,
        name: "Missing Skill Operator",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 4,
      },
      {
        id: operatorIds.atCapacity,
        organizationId,
        name: "At Capacity Operator",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 1,
      },
      {
        id: operatorIds.otherOrganization,
        organizationId: otherOrganizationId,
        name: "Other Organization Operator",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 4,
      },
    ],
  });

  await database.operatorSkill.createMany({
    data: [
      {
        organizationId,
        operatorId: operatorIds.eligible,
        skillId,
        level: 4,
      },
      {
        organizationId,
        operatorId: operatorIds.unavailable,
        skillId,
        level: 3,
      },
      {
        organizationId,
        operatorId: operatorIds.wrongRegion,
        skillId,
        level: 5,
      },
      {
        organizationId,
        operatorId: operatorIds.atCapacity,
        skillId,
        level: 5,
      },
      {
        organizationId: otherOrganizationId,
        operatorId: operatorIds.otherOrganization,
        skillId: otherSkillId,
        level: 5,
      },
    ],
  });

  const historicalRequestId = randomUUID();

  await database.serviceRequest.create({
    data: {
      id: historicalRequestId,
      organizationId,
      externalId: `routing-candidate-history-${historicalRequestId}`,
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
      serviceRequestId: historicalRequestId,
      operatorId: operatorIds.eligible,
      status: "CANCELLED",
      createdAt: HISTORICAL_ASSIGNMENT_AT,
    },
  });

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
      operatorId: operatorIds.atCapacity,
      status: "ACTIVE",
      createdAt: ACTIVE_ASSIGNMENT_AT,
    },
  });

  return {
    organizationId,
    otherOrganizationId,
    skillId,
    serviceRequestId,
    operatorIds,
  };
}

async function clearCandidateAuditFixture(
  fixture: CandidateAuditFixture,
): Promise<void> {
  const organizationIds = [fixture.organizationId, fixture.otherOrganizationId];

  await database.webhookDelivery.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.outboxEvent.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.routingDecision.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.assignment.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.serviceRequest.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.operatorSkill.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.operator.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId: {
        in: organizationIds,
      },
    },
  });

  await database.organization.deleteMany({
    where: {
      id: {
        in: organizationIds,
      },
    },
  });
}

afterAll(async () => {
  await database.$disconnect();
});

describe("loadRoutingCandidates", () => {
  it("loads all relevant same-organization facts without prefiltering hard rejections", async () => {
    const fixture = await createCandidateAuditFixture();

    try {
      const rows = await loadRoutingCandidates(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
      });

      const expectedOperatorIds = [
        fixture.operatorIds.eligible,
        fixture.operatorIds.unavailable,
        fixture.operatorIds.wrongRegion,
        fixture.operatorIds.missingSkill,
        fixture.operatorIds.atCapacity,
      ].sort();

      expect(rows.map((row) => row.operatorId)).toEqual(expectedOperatorIds);

      const candidatesById = new Map(
        rows.map((row) => [row.operatorId, row] as const),
      );

      expect(candidatesById.get(fixture.operatorIds.eligible)).toEqual({
        operatorId: fixture.operatorIds.eligible,
        organizationId: fixture.organizationId,
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 4,
        activeAssignmentCount: 0,
        requiredSkillId: fixture.skillId,
        requiredSkillLevel: 4,
        hasRequiredSkill: true,
        lastAssignedAt: HISTORICAL_ASSIGNMENT_AT,
        totalAssignmentCount: 1,
      });

      expect(candidatesById.get(fixture.operatorIds.unavailable)).toEqual({
        operatorId: fixture.operatorIds.unavailable,
        organizationId: fixture.organizationId,
        status: "UNAVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 4,
        activeAssignmentCount: 0,
        requiredSkillId: fixture.skillId,
        requiredSkillLevel: 3,
        hasRequiredSkill: true,
        lastAssignedAt: null,
        totalAssignmentCount: 0,
      });

      expect(candidatesById.get(fixture.operatorIds.wrongRegion)).toEqual({
        operatorId: fixture.operatorIds.wrongRegion,
        organizationId: fixture.organizationId,
        status: "AVAILABLE",
        region: "EAST",
        maxConcurrentAssignments: 4,
        activeAssignmentCount: 0,
        requiredSkillId: fixture.skillId,
        requiredSkillLevel: 5,
        hasRequiredSkill: true,
        lastAssignedAt: null,
        totalAssignmentCount: 0,
      });

      expect(candidatesById.get(fixture.operatorIds.missingSkill)).toEqual({
        operatorId: fixture.operatorIds.missingSkill,
        organizationId: fixture.organizationId,
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 4,
        activeAssignmentCount: 0,
        requiredSkillId: fixture.skillId,
        requiredSkillLevel: null,
        hasRequiredSkill: false,
        lastAssignedAt: null,
        totalAssignmentCount: 0,
      });

      expect(candidatesById.get(fixture.operatorIds.atCapacity)).toEqual({
        operatorId: fixture.operatorIds.atCapacity,
        organizationId: fixture.organizationId,
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 1,
        activeAssignmentCount: 1,
        requiredSkillId: fixture.skillId,
        requiredSkillLevel: 5,
        hasRequiredSkill: true,
        lastAssignedAt: ACTIVE_ASSIGNMENT_AT,
        totalAssignmentCount: 1,
      });

      expect(candidatesById.has(fixture.operatorIds.otherOrganization)).toBe(
        false,
      );
    } finally {
      await clearCandidateAuditFixture(fixture);
    }
  });

  it("returns equal facts and stable ordering across repeated loads", async () => {
    const fixture = await createCandidateAuditFixture();

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

      expect(first.map((candidate) => candidate.operatorId)).toEqual(
        first
          .map((candidate) => candidate.operatorId)
          .slice()
          .sort(),
      );
    } finally {
      await clearCandidateAuditFixture(fixture);
    }
  });
});
