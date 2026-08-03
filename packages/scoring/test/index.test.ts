import { describe, expect, it } from "vitest";

import {
  evaluateRoutingPlan,
  REJECTION_REASONS,
  SCORING_VERSION,
  type RoutingCandidateInput,
  type RoutingRequestContext,
} from "../src/index.js";

function createRequest(): RoutingRequestContext {
  return {
    organizationId: "organization-1",
    serviceRequestId: "service-request-1",
    requiredSkillId: "skill-1",
    region: "west",
    priority: "HIGH",
  };
}

function createCandidate(
  overrides: Partial<RoutingCandidateInput> &
    Pick<RoutingCandidateInput, "operatorId">,
): RoutingCandidateInput {
  return {
    organizationId: "organization-1",
    status: "AVAILABLE",
    region: "west",
    maxConcurrentAssignments: 3,
    activeAssignmentCount: 0,
    requiredSkillLevel: 1,
    hasRequiredSkill: true,
    ...overrides,
  };
}

describe("evaluateRoutingPlan", () => {
  it("selects the best eligible operator deterministically", () => {
    const request = createRequest();

    const candidates = [
      createCandidate({
        operatorId: "operator-c",
        activeAssignmentCount: 2,
        requiredSkillLevel: 5,
      }),
      createCandidate({
        operatorId: "operator-a",
        activeAssignmentCount: 0,
        requiredSkillLevel: 2,
      }),
      createCandidate({
        operatorId: "operator-b",
        activeAssignmentCount: 0,
        requiredSkillLevel: 4,
      }),
    ];

    const result = evaluateRoutingPlan(request, candidates);

    expect(result).toEqual({
      scoringVersion: SCORING_VERSION,
      outcome: "ASSIGNED",
      selectedOperatorId: "operator-b",
      rankedEligibleCandidates: [
        {
          operatorId: "operator-b",
          activeAssignmentCount: 0,
          requiredSkillLevel: 4,
          rank: 1,
        },
        {
          operatorId: "operator-a",
          activeAssignmentCount: 0,
          requiredSkillLevel: 2,
          rank: 2,
        },
        {
          operatorId: "operator-c",
          activeAssignmentCount: 2,
          requiredSkillLevel: 5,
          rank: 3,
        },
      ],
      rejectedCandidates: [],
    });
  });

  it("returns rejection reasons for ineligible candidates", () => {
    const request = createRequest();

    const result = evaluateRoutingPlan(request, [
      createCandidate({
        operatorId: "inactive",
        status: "INACTIVE",
        hasRequiredSkill: false,
        region: "east",
        activeAssignmentCount: 3,
        maxConcurrentAssignments: 3,
      }),
    ]);

    expect(result.outcome).toBe("UNROUTABLE");
    expect(result.selectedOperatorId).toBeNull();
    expect(result.rankedEligibleCandidates).toEqual([]);
    expect(result.rejectedCandidates).toEqual([
      {
        operatorId: "inactive",
        rejectionReasons: [
          REJECTION_REASONS.statusNotEligible,
          REJECTION_REASONS.regionMismatch,
          REJECTION_REASONS.missingRequiredSkill,
          REJECTION_REASONS.atCapacity,
        ],
      },
    ]);
  });

  it("keeps equal inputs stable across repeated evaluation", () => {
    const request = createRequest();

    const candidates = [
      createCandidate({
        operatorId: "operator-z",
        activeAssignmentCount: 1,
        requiredSkillLevel: 3,
      }),
      createCandidate({
        operatorId: "operator-y",
        activeAssignmentCount: 1,
        requiredSkillLevel: 3,
      }),
    ] as const;

    const first = evaluateRoutingPlan(request, candidates);
    const second = evaluateRoutingPlan(request, candidates);

    expect(first).toEqual(second);
    expect(first.selectedOperatorId).toBe("operator-y");
  });

  it("does not mutate the input array or objects", () => {
    const request = createRequest();

    const candidate = createCandidate({
      operatorId: "operator-a",
      activeAssignmentCount: 0,
      requiredSkillLevel: 2,
    });

    const candidates = [candidate];

    const snapshot = structuredClone(candidates);

    evaluateRoutingPlan(request, candidates);

    expect(candidates).toEqual(snapshot);
  });
});
