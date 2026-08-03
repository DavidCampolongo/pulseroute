export const SCORING_VERSION = "phase-7-stub-v1" as const;

export const REJECTION_REASONS = {
  statusNotEligible: "STATUS_NOT_ELIGIBLE",
  regionMismatch: "REGION_MISMATCH",
  missingRequiredSkill: "MISSING_REQUIRED_SKILL",
  atCapacity: "AT_CAPACITY",
} as const;

export type RejectionReasonCode =
  (typeof REJECTION_REASONS)[keyof typeof REJECTION_REASONS];

export type RoutingOutcome = "ASSIGNED" | "UNROUTABLE";

export type RoutingRequestContext = {
  organizationId: string;
  serviceRequestId: string;
  requiredSkillId: string;
  region: string;
  priority: "LOW" | "NORMAL" | "HIGH";
};

export type RoutingCandidateInput = {
  operatorId: string;
  organizationId: string;
  status: "AVAILABLE" | "UNAVAILABLE" | "INACTIVE";
  region: string;
  maxConcurrentAssignments: number;
  activeAssignmentCount: number;
  requiredSkillLevel: number | null;
  hasRequiredSkill: boolean;
};

export type RejectedRoutingCandidate = {
  operatorId: string;
  rejectionReasons: RejectionReasonCode[];
};

export type RankedRoutingCandidate = {
  operatorId: string;
  activeAssignmentCount: number;
  requiredSkillLevel: number;
  rank: number;
};

export type RoutingDecisionPlan = {
  scoringVersion: typeof SCORING_VERSION;
  outcome: RoutingOutcome;
  selectedOperatorId: string | null;
  rankedEligibleCandidates: RankedRoutingCandidate[];
  rejectedCandidates: RejectedRoutingCandidate[];
};

function uniqueReasons(reasons: RejectionReasonCode[]): RejectionReasonCode[] {
  return [...new Set(reasons)];
}

function compareCandidates(
  left: {
    operatorId: string;
    activeAssignmentCount: number;
    requiredSkillLevel: number;
  },
  right: {
    operatorId: string;
    activeAssignmentCount: number;
    requiredSkillLevel: number;
  },
): number {
  if (left.activeAssignmentCount !== right.activeAssignmentCount) {
    return left.activeAssignmentCount - right.activeAssignmentCount;
  }

  if (left.requiredSkillLevel !== right.requiredSkillLevel) {
    return right.requiredSkillLevel - left.requiredSkillLevel;
  }

  return left.operatorId.localeCompare(right.operatorId);
}

export function evaluateRoutingPlan(
  request: RoutingRequestContext,
  candidates: readonly RoutingCandidateInput[],
): RoutingDecisionPlan {
  const copiedCandidates = candidates.map((candidate) => ({
    ...candidate,
  }));

  const eligible: Array<{
    operatorId: string;
    activeAssignmentCount: number;
    requiredSkillLevel: number;
  }> = [];
  const rejected: RejectedRoutingCandidate[] = [];

  for (const candidate of copiedCandidates) {
    const rejectionReasons: RejectionReasonCode[] = [];

    if (candidate.status !== "AVAILABLE") {
      rejectionReasons.push(REJECTION_REASONS.statusNotEligible);
    }

    if (candidate.region !== request.region) {
      rejectionReasons.push(REJECTION_REASONS.regionMismatch);
    }

    if (!candidate.hasRequiredSkill) {
      rejectionReasons.push(REJECTION_REASONS.missingRequiredSkill);
    }

    if (candidate.activeAssignmentCount >= candidate.maxConcurrentAssignments) {
      rejectionReasons.push(REJECTION_REASONS.atCapacity);
    }

    if (rejectionReasons.length > 0) {
      rejected.push({
        operatorId: candidate.operatorId,
        rejectionReasons: uniqueReasons(rejectionReasons),
      });
      continue;
    }

    eligible.push({
      operatorId: candidate.operatorId,
      activeAssignmentCount: candidate.activeAssignmentCount,
      requiredSkillLevel: candidate.requiredSkillLevel ?? 0,
    });
  }

  const rankedEligibleCandidates = eligible
    .slice()
    .sort(compareCandidates)
    .map((candidate, index) => ({
      operatorId: candidate.operatorId,
      activeAssignmentCount: candidate.activeAssignmentCount,
      requiredSkillLevel: candidate.requiredSkillLevel,
      rank: index + 1,
    }));

  const selectedOperatorId = rankedEligibleCandidates[0]?.operatorId ?? null;

  return {
    scoringVersion: SCORING_VERSION,
    outcome: selectedOperatorId === null ? "UNROUTABLE" : "ASSIGNED",
    selectedOperatorId,
    rankedEligibleCandidates,
    rejectedCandidates: rejected,
  };
}
