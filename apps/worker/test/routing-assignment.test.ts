import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  SCORING_FACTOR_CODES,
  SCORING_REJECTION_REASON_CODES,
  SCORING_VERSION,
} from "@pulseroute/scoring";
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
const EVALUATED_AT = "2026-08-15T12:00:00.000Z";

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

type RejectedSnapshotCandidate = {
  operatorId: string;
  reasons: string[];
  observedFacts: {
    status: string;
    region: string;
    maxConcurrentAssignments: number;
    activeAssignmentCount: number;
    requiredSkillLevel: number | null;
  };
};

type DecisionSnapshot = {
  scoringVersion: string;
  evaluatedAt: string;
  scoring: {
    initialSelectedOperatorId: string | null;
    weightProfile: {
      profileCode: string;
      weights: Record<string, number>;
    };
    rankedEligibleCandidates: Array<{
      operatorId: string;
      rank: number;
      totalScore: number;
      factors: Array<{
        factorCode: string;
        rawValue: string | number | null;
        normalizedValue: number;
        weight: number;
        contribution: number;
      }>;
    }>;
    rejectedCandidates: RejectedSnapshotCandidate[];
  };
  lockTimeOutcomes: Array<{
    operatorId: string;
    scoringRank: number;
    outcome: string;
    reasons?: string[];
  }>;
  result: {
    outcome: string;
    initialSelectedOperatorId: string | null;
    selectedOperatorId: string | null;
    fallbackUsed?: boolean;
    unroutableReason?: string;
    rejectionReasons?: string[];
  };
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
  it("creates one explainable assignment decision and versioned notification atomically", async () => {
    const fixture = await createRoutingFixture({
      maxConcurrentAssignments: 2,
      requiredSkillLevel: 4,
      activeAssignmentCount: 0,
    });

    try {
      const result = await executeRouteServiceRequest(
        database,
        {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
          correlationId: `request-${randomUUID()}`,
        },
        {
          now: () => new Date(EVALUATED_AT),
        },
      );

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
        scoringVersion: SCORING_VERSION,
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
        scoringVersion: SCORING_VERSION,
      });

      const snapshot =
        routingDecision.decisionSnapshot as unknown as DecisionSnapshot;

      expect(snapshot).toMatchObject({
        scoringVersion: SCORING_VERSION,
        evaluatedAt: EVALUATED_AT,
        request: {
          id: fixture.serviceRequestId,
          organizationId: fixture.organizationId,
          requiredSkillId: fixture.skillId,
          priority: "NORMAL",
          region: "WEST",
          status: "PENDING",
        },
        scoring: {
          outcome: "ASSIGNED",
          initialSelectedOperatorId: fixture.operatorId,
          rejectedCandidates: [],
        },
        lockTimeOutcomes: [
          {
            operatorId: fixture.operatorId,
            scoringRank: 1,
            outcome: "ACCEPTED",
          },
        ],
        result: {
          outcome: "ASSIGNED",
          initialSelectedOperatorId: fixture.operatorId,
          selectedOperatorId: fixture.operatorId,
          fallbackUsed: false,
        },
      });

      expect(snapshot.scoring.rankedEligibleCandidates).toHaveLength(1);

      const scoredCandidate = snapshot.scoring.rankedEligibleCandidates[0]!;

      expect(scoredCandidate).toMatchObject({
        operatorId: fixture.operatorId,
        rank: 1,
      });
      expect(scoredCandidate.totalScore).toBeGreaterThan(0);
      expect(
        scoredCandidate.factors.map((factor) => factor.factorCode),
      ).toEqual([
        SCORING_FACTOR_CODES.requiredSkillStrength,
        SCORING_FACTOR_CODES.loadHeadroom,
        SCORING_FACTOR_CODES.assignmentFairness,
        SCORING_FACTOR_CODES.assignmentExperience,
      ]);

      for (const factor of scoredCandidate.factors) {
        expect(factor).toEqual(
          expect.objectContaining({
            factorCode: expect.any(String),
            normalizedValue: expect.any(Number),
            weight: expect.any(Number),
            contribution: expect.any(Number),
          }),
        );
        expect(factor).toHaveProperty("rawValue");
      }
    } finally {
      await clearRoutingFixture(fixture);
    }
  });

  it("creates an explainable unroutable decision without assignment or notification", async () => {
    const fixture = await createRoutingFixture({
      maxConcurrentAssignments: 1,
      requiredSkillLevel: 5,
      activeAssignmentCount: 1,
    });

    try {
      const result = await executeRouteServiceRequest(
        database,
        {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
          correlationId: `request-${randomUUID()}`,
        },
        {
          now: () => new Date(EVALUATED_AT),
        },
      );

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

      expect(result.rejectionReasons).toEqual([
        SCORING_REJECTION_REASON_CODES.atCapacity,
      ]);

      expect(routingDecision).toMatchObject({
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        assignmentId: null,
        scoringVersion: SCORING_VERSION,
        outcome: "UNROUTABLE",
      });

      expect(serviceRequest.status).toBe("UNROUTABLE");
      expect(targetAssignments).toHaveLength(0);
      expect(targetOutboxEvents).toHaveLength(0);

      const snapshot =
        routingDecision.decisionSnapshot as unknown as DecisionSnapshot;

      expect(snapshot).toMatchObject({
        scoringVersion: SCORING_VERSION,
        evaluatedAt: EVALUATED_AT,
        scoring: {
          outcome: "UNROUTABLE",
          initialSelectedOperatorId: null,
          rankedEligibleCandidates: [],
          rejectedCandidates: [
            {
              operatorId: fixture.operatorId,
              reasons: [SCORING_REJECTION_REASON_CODES.atCapacity],
              observedFacts: {
                activeAssignmentCount: 1,
                maxConcurrentAssignments: 1,
              },
            },
          ],
        },
        lockTimeOutcomes: [],
        result: {
          outcome: "UNROUTABLE",
          initialSelectedOperatorId: null,
          selectedOperatorId: null,
          unroutableReason: "ALL_CANDIDATES_REJECTED_AT_SCORING",
          rejectionReasons: [SCORING_REJECTION_REASON_CODES.atCapacity],
        },
      });
    } finally {
      await clearRoutingFixture(fixture);
    }
  });

  it("persists database-backed evidence for every scoring hard filter", async () => {
    const fixture = await createRoutingFixture({
      maxConcurrentAssignments: 3,
      requiredSkillLevel: 4,
      activeAssignmentCount: 0,
    });

    const rejectedOperatorIds = {
      unavailable: randomUUID(),
      wrongRegion: randomUUID(),
      missingSkill: randomUUID(),
      atCapacity: randomUUID(),
    };

    try {
      await database.operator.createMany({
        data: [
          {
            id: rejectedOperatorIds.unavailable,
            organizationId: fixture.organizationId,
            name: "Hard Filter Unavailable",
            status: "UNAVAILABLE",
            region: "WEST",
            maxConcurrentAssignments: 3,
          },
          {
            id: rejectedOperatorIds.wrongRegion,
            organizationId: fixture.organizationId,
            name: "Hard Filter Wrong Region",
            status: "AVAILABLE",
            region: "EAST",
            maxConcurrentAssignments: 3,
          },
          {
            id: rejectedOperatorIds.missingSkill,
            organizationId: fixture.organizationId,
            name: "Hard Filter Missing Skill",
            status: "AVAILABLE",
            region: "WEST",
            maxConcurrentAssignments: 3,
          },
          {
            id: rejectedOperatorIds.atCapacity,
            organizationId: fixture.organizationId,
            name: "Hard Filter At Capacity",
            status: "AVAILABLE",
            region: "WEST",
            maxConcurrentAssignments: 1,
          },
        ],
      });

      await database.operatorSkill.createMany({
        data: [
          {
            organizationId: fixture.organizationId,
            operatorId: rejectedOperatorIds.unavailable,
            skillId: fixture.skillId,
            level: 4,
          },
          {
            organizationId: fixture.organizationId,
            operatorId: rejectedOperatorIds.wrongRegion,
            skillId: fixture.skillId,
            level: 5,
          },
          {
            organizationId: fixture.organizationId,
            operatorId: rejectedOperatorIds.atCapacity,
            skillId: fixture.skillId,
            level: 5,
          },
        ],
      });

      const capacityRequestId = randomUUID();

      await database.serviceRequest.create({
        data: {
          id: capacityRequestId,
          organizationId: fixture.organizationId,
          externalId: `hard-filter-capacity-${capacityRequestId}`,
          requiredSkillId: fixture.skillId,
          status: "ASSIGNED",
          priority: "NORMAL",
          region: "WEST",
        },
      });

      await database.assignment.create({
        data: {
          id: randomUUID(),
          organizationId: fixture.organizationId,
          serviceRequestId: capacityRequestId,
          operatorId: rejectedOperatorIds.atCapacity,
          status: "ACTIVE",
        },
      });

      const result = await executeRouteServiceRequest(
        database,
        {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
          correlationId: `hard-filter-${randomUUID()}`,
        },
        {
          now: () => new Date(EVALUATED_AT),
        },
      );

      expect(result.kind).toBe("assigned");

      if (result.kind !== "assigned") {
        throw new Error("Expected the eligible operator to be assigned");
      }

      const routingDecision = await database.routingDecision.findUniqueOrThrow({
        where: {
          id: result.routingDecisionId,
        },
      });

      const snapshot =
        routingDecision.decisionSnapshot as unknown as DecisionSnapshot;
      const rejectedById = new Map(
        snapshot.scoring.rejectedCandidates.map((candidate) => [
          candidate.operatorId,
          candidate,
        ]),
      );

      expect(snapshot.scoring.rankedEligibleCandidates).toHaveLength(1);
      expect(snapshot.scoring.rejectedCandidates).toHaveLength(4);
      expect(result.operatorId).toBe(fixture.operatorId);

      expect(rejectedById.get(rejectedOperatorIds.unavailable)).toMatchObject({
        reasons: [SCORING_REJECTION_REASON_CODES.operatorNotAvailable],
        observedFacts: {
          status: "UNAVAILABLE",
        },
      });

      expect(rejectedById.get(rejectedOperatorIds.wrongRegion)).toMatchObject({
        reasons: [SCORING_REJECTION_REASON_CODES.regionIncompatible],
        observedFacts: {
          region: "EAST",
        },
      });

      expect(rejectedById.get(rejectedOperatorIds.missingSkill)).toMatchObject({
        reasons: [SCORING_REJECTION_REASON_CODES.missingRequiredSkill],
        observedFacts: {
          requiredSkillLevel: null,
        },
      });

      expect(rejectedById.get(rejectedOperatorIds.atCapacity)).toMatchObject({
        reasons: [SCORING_REJECTION_REASON_CODES.atCapacity],
        observedFacts: {
          activeAssignmentCount: 1,
          maxConcurrentAssignments: 1,
        },
      });
    } finally {
      await clearRoutingFixture(fixture);
    }
  });
});
