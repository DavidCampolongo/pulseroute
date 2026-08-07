import { describe, expect, it } from "vitest";

import {
  SCORING_FACTOR_CODES,
  SCORING_REJECTION_REASON_CODES,
  SCORING_VERSION,
  SCORING_WEIGHT_PROFILE_CODES,
  scoreRoutingCandidates,
  type ScoreFactorBreakdown,
  type ScoredCandidate,
  type ScoringCandidate,
  type ScoringFactorCode,
  type ScoringInput,
  type ScoringPriority,
} from "../src/index.js";

const EVALUATED_AT = "2026-08-06T16:00:00.000Z";
const EVALUATED_AT_MS = Date.parse(EVALUATED_AT);
const HOUR_MS = 60 * 60 * 1_000;

function hoursBeforeEvaluation(hours: number): string {
  return new Date(EVALUATED_AT_MS - hours * HOUR_MS).toISOString();
}

function createCandidate(
  overrides: Partial<ScoringCandidate> & Pick<ScoringCandidate, "operatorId">,
): ScoringCandidate {
  const { operatorId, ...candidateOverrides } = overrides;

  return {
    operatorId,
    organizationId: "organization-1",
    status: "AVAILABLE",
    region: "WEST",
    maxConcurrentAssignments: 4,
    activeAssignmentCount: 1,
    requiredSkillLevel: 3,
    lastAssignedAt: hoursBeforeEvaluation(84),
    totalAssignmentCount: 10,
    ...candidateOverrides,
  };
}

function createInput(
  candidates: readonly ScoringCandidate[],
  priority: ScoringPriority = "NORMAL",
): ScoringInput {
  return {
    evaluatedAt: EVALUATED_AT,
    request: {
      organizationId: "organization-1",
      serviceRequestId: "service-request-1",
      requiredSkillId: "skill-1",
      region: "WEST",
      priority,
    },
    candidates,
  };
}

function getOnlyScoredCandidate(input: ScoringInput): ScoredCandidate {
  const result = scoreRoutingCandidates(input);

  expect(result.rankedEligibleCandidates).toHaveLength(1);

  return result.rankedEligibleCandidates[0]!;
}

function getFactor(
  candidate: ScoredCandidate,
  factorCode: ScoringFactorCode,
): ScoreFactorBreakdown {
  const factor = candidate.factors.find(
    (candidateFactor) => candidateFactor.factorCode === factorCode,
  );

  if (!factor) {
    throw new Error(`Missing factor ${factorCode}`);
  }

  return factor;
}

describe("scoreRoutingCandidates hard filters", () => {
  const cases: Array<{
    name: string;
    overrides: Partial<ScoringCandidate>;
    expectedReasons: string[];
  }> = [
    {
      name: "keeps an eligible candidate",
      overrides: {},
      expectedReasons: [],
    },
    {
      name: "rejects an unavailable candidate",
      overrides: {
        status: "UNAVAILABLE",
      },
      expectedReasons: [SCORING_REJECTION_REASON_CODES.operatorNotAvailable],
    },
    {
      name: "rejects an inactive candidate",
      overrides: {
        status: "INACTIVE",
      },
      expectedReasons: [SCORING_REJECTION_REASON_CODES.operatorNotAvailable],
    },
    {
      name: "rejects a candidate in the wrong region",
      overrides: {
        region: "EAST",
      },
      expectedReasons: [SCORING_REJECTION_REASON_CODES.regionIncompatible],
    },
    {
      name: "rejects a candidate without the required skill",
      overrides: {
        requiredSkillLevel: null,
      },
      expectedReasons: [SCORING_REJECTION_REASON_CODES.missingRequiredSkill],
    },
    {
      name: "rejects a candidate at capacity",
      overrides: {
        activeAssignmentCount: 4,
        maxConcurrentAssignments: 4,
      },
      expectedReasons: [SCORING_REJECTION_REASON_CODES.atCapacity],
    },
    {
      name: "retains all reasons in deterministic order",
      overrides: {
        status: "INACTIVE",
        region: "EAST",
        requiredSkillLevel: null,
        activeAssignmentCount: 4,
        maxConcurrentAssignments: 4,
      },
      expectedReasons: [
        SCORING_REJECTION_REASON_CODES.operatorNotAvailable,
        SCORING_REJECTION_REASON_CODES.regionIncompatible,
        SCORING_REJECTION_REASON_CODES.missingRequiredSkill,
        SCORING_REJECTION_REASON_CODES.atCapacity,
      ],
    },
  ];

  it.each(cases)("$name", ({ overrides, expectedReasons }) => {
    const candidate = createCandidate({
      operatorId: "operator-1",
      ...overrides,
    });

    const result = scoreRoutingCandidates(createInput([candidate]));

    if (expectedReasons.length === 0) {
      expect(result.outcome).toBe("ASSIGNED");
      expect(result.selectedOperatorId).toBe(candidate.operatorId);
      expect(result.rankedEligibleCandidates).toHaveLength(1);
      expect(result.rejectedCandidates).toEqual([]);
      return;
    }

    expect(result.outcome).toBe("UNROUTABLE");
    expect(result.selectedOperatorId).toBeNull();
    expect(result.rankedEligibleCandidates).toEqual([]);
    expect(result.rejectedCandidates).toEqual([
      {
        operatorId: candidate.operatorId,
        reasons: expectedReasons,
        observedFacts: {
          status: candidate.status,
          region: candidate.region,
          maxConcurrentAssignments: candidate.maxConcurrentAssignments,
          activeAssignmentCount: candidate.activeAssignmentCount,
          requiredSkillLevel: candidate.requiredSkillLevel,
          lastAssignedAt: candidate.lastAssignedAt,
          totalAssignmentCount: candidate.totalAssignmentCount,
        },
      },
    ]);
  });
});

describe("scoreRoutingCandidates factors", () => {
  it.each([
    {
      skillLevel: 1,
      expectedNormalizedValue: 20,
      expectedContribution: 600,
    },
    {
      skillLevel: 3,
      expectedNormalizedValue: 60,
      expectedContribution: 1_800,
    },
    {
      skillLevel: 5,
      expectedNormalizedValue: 100,
      expectedContribution: 3_000,
    },
  ])(
    "normalizes skill level $skillLevel",
    ({ skillLevel, expectedNormalizedValue, expectedContribution }) => {
      const scored = getOnlyScoredCandidate(
        createInput([
          createCandidate({
            operatorId: "operator-1",
            requiredSkillLevel: skillLevel,
          }),
        ]),
      );

      expect(
        getFactor(scored, SCORING_FACTOR_CODES.requiredSkillStrength),
      ).toEqual({
        factorCode: SCORING_FACTOR_CODES.requiredSkillStrength,
        rawValue: skillLevel,
        normalizedValue: expectedNormalizedValue,
        weight: 30,
        contribution: expectedContribution,
      });
    },
  );

  it.each([
    {
      activeAssignmentCount: 0,
      expectedRawValue: 4,
      expectedNormalizedValue: 100,
      expectedContribution: 3_000,
    },
    {
      activeAssignmentCount: 2,
      expectedRawValue: 2,
      expectedNormalizedValue: 50,
      expectedContribution: 1_500,
    },
    {
      activeAssignmentCount: 3,
      expectedRawValue: 1,
      expectedNormalizedValue: 25,
      expectedContribution: 750,
    },
  ])(
    "normalizes load with $activeAssignmentCount active assignments",
    ({
      activeAssignmentCount,
      expectedRawValue,
      expectedNormalizedValue,
      expectedContribution,
    }) => {
      const scored = getOnlyScoredCandidate(
        createInput([
          createCandidate({
            operatorId: "operator-1",
            activeAssignmentCount,
            maxConcurrentAssignments: 4,
          }),
        ]),
      );

      expect(getFactor(scored, SCORING_FACTOR_CODES.loadHeadroom)).toEqual({
        factorCode: SCORING_FACTOR_CODES.loadHeadroom,
        rawValue: expectedRawValue,
        normalizedValue: expectedNormalizedValue,
        weight: 30,
        contribution: expectedContribution,
      });
    },
  );

  it("rejects at-capacity candidates before calculating load factors", () => {
    const result = scoreRoutingCandidates(
      createInput([
        createCandidate({
          operatorId: "operator-1",
          activeAssignmentCount: 4,
          maxConcurrentAssignments: 4,
        }),
      ]),
    );

    expect(result.rankedEligibleCandidates).toEqual([]);
    expect(result.rejectedCandidates[0]?.reasons).toEqual([
      SCORING_REJECTION_REASON_CODES.atCapacity,
    ]);
  });

  it.each([
    {
      name: "never assigned",
      lastAssignedAt: null,
      expectedRawValue: null,
      expectedNormalizedValue: 100,
      expectedContribution: 2_500,
    },
    {
      name: "assigned seven days ago",
      lastAssignedAt: hoursBeforeEvaluation(168),
      expectedRawValue: 168,
      expectedNormalizedValue: 100,
      expectedContribution: 2_500,
    },
    {
      name: "assigned three and a half days ago",
      lastAssignedAt: hoursBeforeEvaluation(84),
      expectedRawValue: 84,
      expectedNormalizedValue: 50,
      expectedContribution: 1_250,
    },
    {
      name: "assigned six hours ago",
      lastAssignedAt: hoursBeforeEvaluation(6),
      expectedRawValue: 6,
      expectedNormalizedValue: 3,
      expectedContribution: 75,
    },
  ])(
    "normalizes assignment fairness for $name",
    ({
      lastAssignedAt,
      expectedRawValue,
      expectedNormalizedValue,
      expectedContribution,
    }) => {
      const scored = getOnlyScoredCandidate(
        createInput([
          createCandidate({
            operatorId: "operator-1",
            lastAssignedAt,
          }),
        ]),
      );

      expect(
        getFactor(scored, SCORING_FACTOR_CODES.assignmentFairness),
      ).toEqual({
        factorCode: SCORING_FACTOR_CODES.assignmentFairness,
        rawValue: expectedRawValue,
        normalizedValue: expectedNormalizedValue,
        weight: 25,
        contribution: expectedContribution,
      });
    },
  );

  it.each([
    {
      totalAssignmentCount: 0,
      expectedNormalizedValue: 0,
      expectedContribution: 0,
    },
    {
      totalAssignmentCount: 10,
      expectedNormalizedValue: 50,
      expectedContribution: 750,
    },
    {
      totalAssignmentCount: 20,
      expectedNormalizedValue: 100,
      expectedContribution: 1_500,
    },
    {
      totalAssignmentCount: 25,
      expectedNormalizedValue: 100,
      expectedContribution: 1_500,
    },
  ])(
    "normalizes $totalAssignmentCount historical assignments",
    ({
      totalAssignmentCount,
      expectedNormalizedValue,
      expectedContribution,
    }) => {
      const scored = getOnlyScoredCandidate(
        createInput([
          createCandidate({
            operatorId: "operator-1",
            totalAssignmentCount,
          }),
        ]),
      );

      expect(
        getFactor(scored, SCORING_FACTOR_CODES.assignmentExperience),
      ).toEqual({
        factorCode: SCORING_FACTOR_CODES.assignmentExperience,
        rawValue: totalAssignmentCount,
        normalizedValue: expectedNormalizedValue,
        weight: 15,
        contribution: expectedContribution,
      });
    },
  );

  it.each([
    {
      priority: "HIGH" as const,
      expectedProfileCode: SCORING_WEIGHT_PROFILE_CODES.highPriority,
      expectedWeights: {
        [SCORING_FACTOR_CODES.requiredSkillStrength]: 40,
        [SCORING_FACTOR_CODES.loadHeadroom]: 30,
        [SCORING_FACTOR_CODES.assignmentFairness]: 10,
        [SCORING_FACTOR_CODES.assignmentExperience]: 20,
      },
      expectedTotal: 6_150,
    },
    {
      priority: "NORMAL" as const,
      expectedProfileCode: SCORING_WEIGHT_PROFILE_CODES.normalPriority,
      expectedWeights: {
        [SCORING_FACTOR_CODES.requiredSkillStrength]: 30,
        [SCORING_FACTOR_CODES.loadHeadroom]: 30,
        [SCORING_FACTOR_CODES.assignmentFairness]: 25,
        [SCORING_FACTOR_CODES.assignmentExperience]: 15,
      },
      expectedTotal: 6_050,
    },
    {
      priority: "LOW" as const,
      expectedProfileCode: SCORING_WEIGHT_PROFILE_CODES.lowPriority,
      expectedWeights: {
        [SCORING_FACTOR_CODES.requiredSkillStrength]: 20,
        [SCORING_FACTOR_CODES.loadHeadroom]: 20,
        [SCORING_FACTOR_CODES.assignmentFairness]: 45,
        [SCORING_FACTOR_CODES.assignmentExperience]: 15,
      },
      expectedTotal: 5_700,
    },
  ])(
    "applies the $priority priority profile",
    ({ priority, expectedProfileCode, expectedWeights, expectedTotal }) => {
      const result = scoreRoutingCandidates(
        createInput(
          [
            createCandidate({
              operatorId: "operator-1",
            }),
          ],
          priority,
        ),
      );

      expect(result.weightProfile).toEqual({
        profileCode: expectedProfileCode,
        requestPriority: priority,
        weights: expectedWeights,
      });

      expect(result.rankedEligibleCandidates[0]?.totalScore).toBe(
        expectedTotal,
      );
    },
  );

  it("calculates an independently derived weighted total", () => {
    const scored = getOnlyScoredCandidate(
      createInput([
        createCandidate({
          operatorId: "operator-1",
          requiredSkillLevel: 3,
          activeAssignmentCount: 1,
          maxConcurrentAssignments: 4,
          lastAssignedAt: hoursBeforeEvaluation(84),
          totalAssignmentCount: 10,
        }),
      ]),
    );

    expect(scored.factors).toEqual([
      {
        factorCode: SCORING_FACTOR_CODES.requiredSkillStrength,
        rawValue: 3,
        normalizedValue: 60,
        weight: 30,
        contribution: 1_800,
      },
      {
        factorCode: SCORING_FACTOR_CODES.loadHeadroom,
        rawValue: 3,
        normalizedValue: 75,
        weight: 30,
        contribution: 2_250,
      },
      {
        factorCode: SCORING_FACTOR_CODES.assignmentFairness,
        rawValue: 84,
        normalizedValue: 50,
        weight: 25,
        contribution: 1_250,
      },
      {
        factorCode: SCORING_FACTOR_CODES.assignmentExperience,
        rawValue: 10,
        normalizedValue: 50,
        weight: 15,
        contribution: 750,
      },
    ]);

    expect(scored.totalScore).toBe(6_050);
  });

  it("allows request priority to change the selected operator", () => {
    const experiencedSpecialist = createCandidate({
      operatorId: "operator-specialist",
      requiredSkillLevel: 5,
      activeAssignmentCount: 3,
      maxConcurrentAssignments: 4,
      lastAssignedAt: EVALUATED_AT,
      totalAssignmentCount: 20,
    });

    const restedGeneralist = createCandidate({
      operatorId: "operator-rested",
      requiredSkillLevel: 2,
      activeAssignmentCount: 0,
      maxConcurrentAssignments: 4,
      lastAssignedAt: null,
      totalAssignmentCount: 0,
    });

    const highPriority = scoreRoutingCandidates(
      createInput([experiencedSpecialist, restedGeneralist], "HIGH"),
    );

    const lowPriority = scoreRoutingCandidates(
      createInput([experiencedSpecialist, restedGeneralist], "LOW"),
    );

    expect(highPriority.selectedOperatorId).toBe(
      experiencedSpecialist.operatorId,
    );

    expect(lowPriority.selectedOperatorId).toBe(restedGeneralist.operatorId);
  });
});

describe("scoreRoutingCandidates ranking", () => {
  it("ranks higher total scores first", () => {
    const result = scoreRoutingCandidates(
      createInput([
        createCandidate({
          operatorId: "operator-low",
          requiredSkillLevel: 1,
        }),
        createCandidate({
          operatorId: "operator-high",
          requiredSkillLevel: 5,
        }),
      ]),
    );

    expect(
      result.rankedEligibleCandidates.map((candidate) => candidate.operatorId),
    ).toEqual(["operator-high", "operator-low"]);
  });

  it("uses lower active load when total scores are equal", () => {
    const lowerLoad = createCandidate({
      operatorId: "operator-lower-load",
      requiredSkillLevel: 3,
      activeAssignmentCount: 0,
      maxConcurrentAssignments: 4,
      lastAssignedAt: EVALUATED_AT,
      totalAssignmentCount: 0,
    });

    const higherLoad = createCandidate({
      operatorId: "operator-higher-load",
      requiredSkillLevel: 5,
      activeAssignmentCount: 2,
      maxConcurrentAssignments: 4,
      lastAssignedAt: hoursBeforeEvaluation(21),
      totalAssignmentCount: 0,
    });

    const result = scoreRoutingCandidates(createInput([higherLoad, lowerLoad]));

    expect(
      result.rankedEligibleCandidates.map((candidate) => ({
        operatorId: candidate.operatorId,
        totalScore: candidate.totalScore,
      })),
    ).toEqual([
      {
        operatorId: lowerLoad.operatorId,
        totalScore: 4_800,
      },
      {
        operatorId: higherLoad.operatorId,
        totalScore: 4_800,
      },
    ]);
  });

  it("uses higher skill when score and active load are equal", () => {
    const higherSkill = createCandidate({
      operatorId: "operator-higher-skill",
      requiredSkillLevel: 5,
      activeAssignmentCount: 1,
      maxConcurrentAssignments: 4,
      lastAssignedAt: EVALUATED_AT,
      totalAssignmentCount: 0,
    });

    const lowerSkill = createCandidate({
      operatorId: "operator-lower-skill",
      requiredSkillLevel: 3,
      activeAssignmentCount: 1,
      maxConcurrentAssignments: 4,
      lastAssignedAt: hoursBeforeEvaluation(81),
      totalAssignmentCount: 0,
    });

    const result = scoreRoutingCandidates(
      createInput([lowerSkill, higherSkill]),
    );

    expect(
      result.rankedEligibleCandidates.map((candidate) => ({
        operatorId: candidate.operatorId,
        totalScore: candidate.totalScore,
      })),
    ).toEqual([
      {
        operatorId: higherSkill.operatorId,
        totalScore: 5_250,
      },
      {
        operatorId: lowerSkill.operatorId,
        totalScore: 5_250,
      },
    ]);
  });

  it("uses operator ID as the complete final tie-break", () => {
    const result = scoreRoutingCandidates(
      createInput([
        createCandidate({
          operatorId: "operator-z",
        }),
        createCandidate({
          operatorId: "operator-a",
        }),
      ]),
    );

    expect(
      result.rankedEligibleCandidates.map((candidate) => ({
        operatorId: candidate.operatorId,
        rank: candidate.rank,
      })),
    ).toEqual([
      {
        operatorId: "operator-a",
        rank: 1,
      },
      {
        operatorId: "operator-z",
        rank: 2,
      },
    ]);

    expect(result.selectedOperatorId).toBe("operator-a");
  });
});

describe("scoreRoutingCandidates determinism", () => {
  it("returns deeply equal output for deeply equal explicit input", () => {
    const input = createInput([
      createCandidate({
        operatorId: "operator-b",
      }),
      createCandidate({
        operatorId: "operator-a",
        lastAssignedAt: null,
      }),
    ]);

    const first = scoreRoutingCandidates(structuredClone(input));
    const second = scoreRoutingCandidates(structuredClone(input));

    expect(first).toEqual(second);
  });

  it("does not depend on candidate input order", () => {
    const candidateA = createCandidate({
      operatorId: "operator-a",
    });

    const candidateB = createCandidate({
      operatorId: "operator-b",
      status: "UNAVAILABLE",
    });

    const first = scoreRoutingCandidates(createInput([candidateA, candidateB]));

    const second = scoreRoutingCandidates(
      createInput([candidateB, candidateA]),
    );

    expect(first).toEqual(second);
  });

  it("does not mutate the input", () => {
    const input = createInput([
      createCandidate({
        operatorId: "operator-b",
      }),
      createCandidate({
        operatorId: "operator-a",
      }),
    ]);

    const snapshot = structuredClone(input);

    scoreRoutingCandidates(input);

    expect(input).toEqual(snapshot);
  });

  it("returns the final stable scoring version", () => {
    const result = scoreRoutingCandidates(
      createInput([
        createCandidate({
          operatorId: "operator-1",
        }),
      ]),
    );

    expect(SCORING_VERSION).toBe("pulseroute-scoring-v1");
    expect(result.scoringVersion).toBe(SCORING_VERSION);
    expect(result.evaluatedAt).toBe(EVALUATED_AT);
  });
});
