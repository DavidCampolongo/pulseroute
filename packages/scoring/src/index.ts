import {
  SCORING_FACTOR_CODES,
  SCORING_REJECTION_REASON_CODES,
  SCORING_WEIGHT_PROFILE_CODES,
  type RejectedCandidate,
  type ScoreFactorBreakdown,
  type ScoredCandidate,
  type ScoringCandidate,
  type ScoringCandidateFacts,
  type ScoringFactorCode,
  type ScoringInput,
  type ScoringPriority,
  type ScoringRejectionReasonCode,
  type ScoringRequest,
  type ScoringResult,
  type ScoringVersion,
  type ScoringWeightProfile,
  type ScoringWeights,
} from "@pulseroute/shared";

export {
  SCORING_FACTOR_CODES,
  SCORING_REJECTION_REASON_CODES,
  SCORING_WEIGHT_PROFILE_CODES,
};

export type {
  RejectedCandidate,
  ScoreFactorBreakdown,
  ScoredCandidate,
  ScoringCandidate,
  ScoringCandidateFacts,
  ScoringFactorCode,
  ScoringInput,
  ScoringOperatorStatus,
  ScoringOutcome,
  ScoringPriority,
  ScoringRejectionReasonCode,
  ScoringRequest,
  ScoringResult,
  ScoringVersion,
  ScoringWeightProfile,
  ScoringWeightProfileCode,
  ScoringWeights,
} from "@pulseroute/shared";

export const SCORING_VERSION: ScoringVersion = "pulseroute-scoring-v1";

export const REJECTION_REASONS = SCORING_REJECTION_REASON_CODES;
export const FACTOR_CODES = SCORING_FACTOR_CODES;

const HOUR_MS = 60 * 60 * 1_000;
const FAIRNESS_WINDOW_HOURS = 168;
const MAX_SUPPORTED_SKILL_LEVEL = 5;
const EXPERIENCE_ASSIGNMENT_CAP = 20;
const ISO_TIMESTAMP_WITH_TIMEZONE_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

const FACTOR_ORDER: readonly ScoringFactorCode[] = [
  SCORING_FACTOR_CODES.requiredSkillStrength,
  SCORING_FACTOR_CODES.loadHeadroom,
  SCORING_FACTOR_CODES.assignmentFairness,
  SCORING_FACTOR_CODES.assignmentExperience,
];

const WEIGHT_PROFILE_TEMPLATES: Record<ScoringPriority, ScoringWeightProfile> =
  {
    HIGH: {
      profileCode: SCORING_WEIGHT_PROFILE_CODES.highPriority,
      requestPriority: "HIGH",
      weights: {
        [SCORING_FACTOR_CODES.requiredSkillStrength]: 40,
        [SCORING_FACTOR_CODES.loadHeadroom]: 30,
        [SCORING_FACTOR_CODES.assignmentFairness]: 10,
        [SCORING_FACTOR_CODES.assignmentExperience]: 20,
      },
    },
    NORMAL: {
      profileCode: SCORING_WEIGHT_PROFILE_CODES.normalPriority,
      requestPriority: "NORMAL",
      weights: {
        [SCORING_FACTOR_CODES.requiredSkillStrength]: 30,
        [SCORING_FACTOR_CODES.loadHeadroom]: 30,
        [SCORING_FACTOR_CODES.assignmentFairness]: 25,
        [SCORING_FACTOR_CODES.assignmentExperience]: 15,
      },
    },
    LOW: {
      profileCode: SCORING_WEIGHT_PROFILE_CODES.lowPriority,
      requestPriority: "LOW",
      weights: {
        [SCORING_FACTOR_CODES.requiredSkillStrength]: 20,
        [SCORING_FACTOR_CODES.loadHeadroom]: 20,
        [SCORING_FACTOR_CODES.assignmentFairness]: 45,
        [SCORING_FACTOR_CODES.assignmentExperience]: 15,
      },
    },
  };

function compareStrings(left: string, right: string): number {
  if (left < right) {
    return -1;
  }

  if (left > right) {
    return 1;
  }

  return 0;
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(maximum, Math.max(minimum, value));
}

function assertNonNegativeInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
}

function parseTimestamp(value: string, name: string): number {
  if (!ISO_TIMESTAMP_WITH_TIMEZONE_PATTERN.test(value)) {
    throw new Error(
      `${name} must be an ISO timestamp with an explicit timezone`,
    );
  }

  const timestamp = Date.parse(value);

  if (!Number.isFinite(timestamp)) {
    throw new Error(
      `${name} must be an ISO timestamp with an explicit timezone`,
    );
  }

  return timestamp;
}

function assertScoringInput(input: ScoringInput): number {
  const evaluatedAtMs = parseTimestamp(input.evaluatedAt, "evaluatedAt");
  const operatorIds = new Set<string>();

  for (const candidate of input.candidates) {
    if (candidate.organizationId !== input.request.organizationId) {
      throw new Error(
        `Candidate ${candidate.operatorId} belongs to a different organization`,
      );
    }

    if (operatorIds.has(candidate.operatorId)) {
      throw new Error(`Duplicate candidate ${candidate.operatorId}`);
    }

    operatorIds.add(candidate.operatorId);

    assertPositiveInteger(
      candidate.maxConcurrentAssignments,
      `Candidate ${candidate.operatorId} maxConcurrentAssignments`,
    );

    assertNonNegativeInteger(
      candidate.activeAssignmentCount,
      `Candidate ${candidate.operatorId} activeAssignmentCount`,
    );

    assertNonNegativeInteger(
      candidate.totalAssignmentCount,
      `Candidate ${candidate.operatorId} totalAssignmentCount`,
    );

    if (
      candidate.requiredSkillLevel !== null &&
      (!Number.isInteger(candidate.requiredSkillLevel) ||
        candidate.requiredSkillLevel < 1 ||
        candidate.requiredSkillLevel > MAX_SUPPORTED_SKILL_LEVEL)
    ) {
      throw new Error(
        `Candidate ${candidate.operatorId} requiredSkillLevel must be null or an integer from 1 to ${MAX_SUPPORTED_SKILL_LEVEL}`,
      );
    }

    if (candidate.lastAssignedAt !== null) {
      parseTimestamp(
        candidate.lastAssignedAt,
        `Candidate ${candidate.operatorId} lastAssignedAt`,
      );
    }
  }

  return evaluatedAtMs;
}

function copyObservedFacts(candidate: ScoringCandidate): ScoringCandidateFacts {
  return {
    status: candidate.status,
    region: candidate.region,
    maxConcurrentAssignments: candidate.maxConcurrentAssignments,
    activeAssignmentCount: candidate.activeAssignmentCount,
    requiredSkillLevel: candidate.requiredSkillLevel,
    lastAssignedAt: candidate.lastAssignedAt,
    totalAssignmentCount: candidate.totalAssignmentCount,
  };
}

function classifyCandidate(
  request: ScoringRequest,
  candidate: ScoringCandidate,
): ScoringRejectionReasonCode[] {
  const reasons: ScoringRejectionReasonCode[] = [];

  if (candidate.status !== "AVAILABLE") {
    reasons.push(SCORING_REJECTION_REASON_CODES.operatorNotAvailable);
  }

  if (candidate.region !== request.region) {
    reasons.push(SCORING_REJECTION_REASON_CODES.regionIncompatible);
  }

  if (candidate.requiredSkillLevel === null) {
    reasons.push(SCORING_REJECTION_REASON_CODES.missingRequiredSkill);
  }

  if (candidate.activeAssignmentCount >= candidate.maxConcurrentAssignments) {
    reasons.push(SCORING_REJECTION_REASON_CODES.atCapacity);
  }

  return reasons;
}

function resolveWeightProfile(priority: ScoringPriority): ScoringWeightProfile {
  const template = WEIGHT_PROFILE_TEMPLATES[priority];

  return {
    profileCode: template.profileCode,
    requestPriority: template.requestPriority,
    weights: {
      ...template.weights,
    },
  };
}

function createFactor(options: {
  factorCode: ScoringFactorCode;
  rawValue: number | string | null;
  normalizedValue: number;
  weights: ScoringWeights;
}): ScoreFactorBreakdown {
  const normalizedValue = clamp(Math.trunc(options.normalizedValue), 0, 100);

  const weight = options.weights[options.factorCode];

  return {
    factorCode: options.factorCode,
    rawValue: options.rawValue,
    normalizedValue,
    weight,
    contribution: normalizedValue * weight,
  };
}

function calculateSkillFactor(
  candidate: ScoringCandidate,
  weights: ScoringWeights,
): ScoreFactorBreakdown {
  const rawValue = candidate.requiredSkillLevel ?? 0;

  const normalizedValue = Math.round(
    (clamp(rawValue, 0, MAX_SUPPORTED_SKILL_LEVEL) /
      MAX_SUPPORTED_SKILL_LEVEL) *
      100,
  );

  return createFactor({
    factorCode: SCORING_FACTOR_CODES.requiredSkillStrength,
    rawValue,
    normalizedValue,
    weights,
  });
}

function calculateLoadFactor(
  candidate: ScoringCandidate,
  weights: ScoringWeights,
): ScoreFactorBreakdown {
  const headroom =
    candidate.maxConcurrentAssignments - candidate.activeAssignmentCount;

  const normalizedValue = Math.floor(
    (clamp(headroom, 0, candidate.maxConcurrentAssignments) /
      candidate.maxConcurrentAssignments) *
      100,
  );

  return createFactor({
    factorCode: SCORING_FACTOR_CODES.loadHeadroom,
    rawValue: headroom,
    normalizedValue,
    weights,
  });
}

function calculateFairnessFactor(
  candidate: ScoringCandidate,
  evaluatedAtMs: number,
  weights: ScoringWeights,
): ScoreFactorBreakdown {
  if (candidate.lastAssignedAt === null) {
    return createFactor({
      factorCode: SCORING_FACTOR_CODES.assignmentFairness,
      rawValue: null,
      normalizedValue: 100,
      weights,
    });
  }

  const lastAssignedAtMs = parseTimestamp(
    candidate.lastAssignedAt,
    `Candidate ${candidate.operatorId} lastAssignedAt`,
  );

  const elapsedHours = Math.floor(
    Math.max(0, evaluatedAtMs - lastAssignedAtMs) / HOUR_MS,
  );

  const normalizedValue = Math.floor(
    (clamp(elapsedHours, 0, FAIRNESS_WINDOW_HOURS) / FAIRNESS_WINDOW_HOURS) *
      100,
  );

  return createFactor({
    factorCode: SCORING_FACTOR_CODES.assignmentFairness,
    rawValue: elapsedHours,
    normalizedValue,
    weights,
  });
}

function calculateExperienceFactor(
  candidate: ScoringCandidate,
  weights: ScoringWeights,
): ScoreFactorBreakdown {
  const normalizedValue = Math.floor(
    (clamp(candidate.totalAssignmentCount, 0, EXPERIENCE_ASSIGNMENT_CAP) /
      EXPERIENCE_ASSIGNMENT_CAP) *
      100,
  );

  return createFactor({
    factorCode: SCORING_FACTOR_CODES.assignmentExperience,
    rawValue: candidate.totalAssignmentCount,
    normalizedValue,
    weights,
  });
}

type UnrankedScoredCandidate = Omit<ScoredCandidate, "rank">;

function scoreEligibleCandidate(
  candidate: ScoringCandidate,
  evaluatedAtMs: number,
  weights: ScoringWeights,
): UnrankedScoredCandidate {
  const factorsByCode = new Map<ScoringFactorCode, ScoreFactorBreakdown>();

  const calculatedFactors = [
    calculateSkillFactor(candidate, weights),
    calculateLoadFactor(candidate, weights),
    calculateFairnessFactor(candidate, evaluatedAtMs, weights),
    calculateExperienceFactor(candidate, weights),
  ];

  for (const factor of calculatedFactors) {
    factorsByCode.set(factor.factorCode, factor);
  }

  const factors = FACTOR_ORDER.map((factorCode) => {
    const factor = factorsByCode.get(factorCode);

    if (!factor) {
      throw new Error(`Missing calculated factor ${factorCode}`);
    }

    return factor;
  });

  const totalScore = factors.reduce(
    (sum, factor) => sum + factor.contribution,
    0,
  );

  return {
    operatorId: candidate.operatorId,
    totalScore,
    observedFacts: copyObservedFacts(candidate),
    factors,
  };
}

function compareScoredCandidates(
  left: UnrankedScoredCandidate,
  right: UnrankedScoredCandidate,
): number {
  if (left.totalScore !== right.totalScore) {
    return right.totalScore - left.totalScore;
  }

  if (
    left.observedFacts.activeAssignmentCount !==
    right.observedFacts.activeAssignmentCount
  ) {
    return (
      left.observedFacts.activeAssignmentCount -
      right.observedFacts.activeAssignmentCount
    );
  }

  const leftSkillLevel = left.observedFacts.requiredSkillLevel ?? 0;
  const rightSkillLevel = right.observedFacts.requiredSkillLevel ?? 0;

  if (leftSkillLevel !== rightSkillLevel) {
    return rightSkillLevel - leftSkillLevel;
  }

  return compareStrings(left.operatorId, right.operatorId);
}

function compareRejectedCandidates(
  left: RejectedCandidate,
  right: RejectedCandidate,
): number {
  return compareStrings(left.operatorId, right.operatorId);
}

export function scoreRoutingCandidates(input: ScoringInput): ScoringResult {
  const evaluatedAtMs = assertScoringInput(input);
  const weightProfile = resolveWeightProfile(input.request.priority);

  const eligibleCandidates: UnrankedScoredCandidate[] = [];
  const rejectedCandidates: RejectedCandidate[] = [];

  for (const candidate of input.candidates) {
    const reasons = classifyCandidate(input.request, candidate);

    if (reasons.length > 0) {
      rejectedCandidates.push({
        operatorId: candidate.operatorId,
        reasons,
        observedFacts: copyObservedFacts(candidate),
      });

      continue;
    }

    eligibleCandidates.push(
      scoreEligibleCandidate(candidate, evaluatedAtMs, weightProfile.weights),
    );
  }

  const rankedEligibleCandidates = eligibleCandidates
    .slice()
    .sort(compareScoredCandidates)
    .map((candidate, index): ScoredCandidate => {
      return {
        ...candidate,
        rank: index + 1,
      };
    });

  rejectedCandidates.sort(compareRejectedCandidates);

  const selectedOperatorId = rankedEligibleCandidates[0]?.operatorId ?? null;

  return {
    scoringVersion: SCORING_VERSION,
    evaluatedAt: input.evaluatedAt,
    outcome: selectedOperatorId === null ? "UNROUTABLE" : "ASSIGNED",
    weightProfile,
    selectedOperatorId,
    rankedEligibleCandidates,
    rejectedCandidates,
  };
}
