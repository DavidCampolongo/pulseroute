import { randomUUID } from "node:crypto";

import { Prisma, type DatabaseClient } from "@pulseroute/db";
import {
  scoreRoutingCandidates,
  type RejectedCandidate,
  type ScoredCandidate,
  type ScoringCandidate,
  type ScoringInput,
  type ScoringRejectionReasonCode,
  type ScoringResult,
  type ScoringVersion,
} from "@pulseroute/scoring";
import type { RouteServiceRequestJobData } from "@pulseroute/shared";

import {
  loadRoutingCandidates,
  type RoutingCandidateRow,
} from "./routing-candidates.js";
import {
  lockAndRecheckSelectedOperator,
  type LockedOperatorOutcome,
} from "./routing-operator.js";
import type { RoutingScorer } from "./scoring.js";
import {
  lockRouteServiceRequest,
  parseRouteServiceRequestJobData,
  type LockedServiceRequest,
} from "./routing-transaction.js";

type TransactionClient = Pick<
  DatabaseClient,
  | "$queryRaw"
  | "assignment"
  | "routingDecision"
  | "serviceRequest"
  | "outboxEvent"
>;

type LockTimeObservedFacts = {
  status: "AVAILABLE" | "UNAVAILABLE" | "INACTIVE";
  region: string;
  maxConcurrentAssignments: number;
  activeAssignmentCount: number;
  requiredSkillLevel: number | null;
};

type LockTimeOutcome =
  | {
      operatorId: string;
      scoringRank: number;
      outcome: "ACCEPTED";
      observedFacts: LockTimeObservedFacts;
    }
  | {
      operatorId: string;
      scoringRank: number;
      outcome: "REJECTED";
      reasons: ScoringRejectionReasonCode[];
      observedFacts: LockTimeObservedFacts | null;
    };

type UnroutableReason =
  | "NO_CANDIDATES"
  | "ALL_CANDIDATES_REJECTED_AT_SCORING"
  | "ALL_RANKED_CANDIDATES_REJECTED_AT_LOCK_TIME";

export type RoutingAssignmentResult =
  | {
      kind: "assigned";
      organizationId: string;
      serviceRequestId: string;
      operatorId: string;
      assignmentId: string;
      routingDecisionId: string;
      outboxEventId: string;
      scoringVersion: ScoringVersion;
    }
  | {
      kind: "unroutable";
      organizationId: string;
      serviceRequestId: string;
      routingDecisionId: string;
      rejectionReasons: ScoringRejectionReasonCode[];
      scoringVersion: ScoringVersion;
    }
  | {
      kind: "already_processed";
      organizationId: string;
      serviceRequestId: string;
      terminalStatus: Exclude<LockedServiceRequest["status"], "PENDING">;
    };

export type RouteServiceRequestOptions = {
  scorer?: RoutingScorer;
  now?: () => Date;
};

function toScoringCandidate(candidate: RoutingCandidateRow): ScoringCandidate {
  return {
    operatorId: candidate.operatorId,
    organizationId: candidate.organizationId,
    status: candidate.status,
    region: candidate.region,
    maxConcurrentAssignments: candidate.maxConcurrentAssignments,
    activeAssignmentCount: candidate.activeAssignmentCount,
    requiredSkillLevel: candidate.requiredSkillLevel,
    lastAssignedAt: candidate.lastAssignedAt?.toISOString() ?? null,
    totalAssignmentCount: candidate.totalAssignmentCount,
  };
}

export function createScoringInput(
  request: LockedServiceRequest,
  candidates: readonly RoutingCandidateRow[],
  evaluatedAt: string,
): ScoringInput {
  return {
    evaluatedAt,
    request: {
      organizationId: request.organizationId,
      serviceRequestId: request.id,
      requiredSkillId: request.requiredSkillId,
      region: request.region,
      priority: request.priority,
    },
    candidates: candidates.map(toScoringCandidate),
  };
}

function createLockTimeOutcome(
  rankedCandidate: ScoredCandidate,
  outcome: LockedOperatorOutcome,
): LockTimeOutcome {
  if (outcome.kind === "accepted") {
    return {
      operatorId: rankedCandidate.operatorId,
      scoringRank: rankedCandidate.rank,
      outcome: "ACCEPTED",
      observedFacts: {
        status: outcome.operator.status,
        region: outcome.operator.region,
        maxConcurrentAssignments: outcome.operator.maxConcurrentAssignments,
        activeAssignmentCount: outcome.activeAssignmentCount,
        requiredSkillLevel: outcome.requiredSkillLevel,
      },
    };
  }

  return {
    operatorId: rankedCandidate.operatorId,
    scoringRank: rankedCandidate.rank,
    outcome: "REJECTED",
    reasons: outcome.rejectionReasons,
    observedFacts:
      outcome.operator === null
        ? null
        : {
            status: outcome.operator.status,
            region: outcome.operator.region,
            maxConcurrentAssignments: outcome.operator.maxConcurrentAssignments,
            activeAssignmentCount: outcome.activeAssignmentCount ?? 0,
            requiredSkillLevel: outcome.requiredSkillLevel,
          },
  };
}

function collectRejectionReasons(
  rejectedCandidates: readonly RejectedCandidate[],
  lockTimeOutcomes: readonly LockTimeOutcome[],
): ScoringRejectionReasonCode[] {
  const reasons = [
    ...rejectedCandidates.flatMap((candidate) => candidate.reasons),
    ...lockTimeOutcomes.flatMap((outcome) =>
      outcome.outcome === "REJECTED" ? outcome.reasons : [],
    ),
  ];

  return [...new Set(reasons)];
}

function resolveUnroutableReason(
  scoringResult: ScoringResult,
): UnroutableReason {
  if (
    scoringResult.rankedEligibleCandidates.length === 0 &&
    scoringResult.rejectedCandidates.length === 0
  ) {
    return "NO_CANDIDATES";
  }

  if (scoringResult.rankedEligibleCandidates.length === 0) {
    return "ALL_CANDIDATES_REJECTED_AT_SCORING";
  }

  return "ALL_RANKED_CANDIDATES_REJECTED_AT_LOCK_TIME";
}

function createDecisionSnapshotBase(options: {
  request: LockedServiceRequest;
  correlationId: string;
  scoringResult: ScoringResult;
  lockTimeOutcomes: readonly LockTimeOutcome[];
}) {
  return {
    scoringVersion: options.scoringResult.scoringVersion,
    evaluatedAt: options.scoringResult.evaluatedAt,
    correlationId: options.correlationId,
    request: {
      id: options.request.id,
      organizationId: options.request.organizationId,
      requiredSkillId: options.request.requiredSkillId,
      priority: options.request.priority,
      region: options.request.region,
      status: options.request.status,
    },
    scoring: {
      outcome: options.scoringResult.outcome,
      weightProfile: options.scoringResult.weightProfile,
      initialSelectedOperatorId: options.scoringResult.selectedOperatorId,
      rankedEligibleCandidates: options.scoringResult.rankedEligibleCandidates,
      rejectedCandidates: options.scoringResult.rejectedCandidates,
    },
    lockTimeOutcomes: options.lockTimeOutcomes,
  };
}

function buildAssignedDecisionSnapshot(options: {
  request: LockedServiceRequest;
  correlationId: string;
  scoringResult: ScoringResult;
  lockTimeOutcomes: readonly LockTimeOutcome[];
  selectedOperatorId: string;
}): Prisma.InputJsonValue {
  return {
    ...createDecisionSnapshotBase(options),
    result: {
      outcome: "ASSIGNED",
      initialSelectedOperatorId: options.scoringResult.selectedOperatorId,
      selectedOperatorId: options.selectedOperatorId,
      fallbackUsed:
        options.selectedOperatorId !== options.scoringResult.selectedOperatorId,
    },
  };
}

function buildUnroutableDecisionSnapshot(options: {
  request: LockedServiceRequest;
  correlationId: string;
  scoringResult: ScoringResult;
  lockTimeOutcomes: readonly LockTimeOutcome[];
  rejectionReasons: readonly ScoringRejectionReasonCode[];
}): Prisma.InputJsonValue {
  return {
    ...createDecisionSnapshotBase(options),
    result: {
      outcome: "UNROUTABLE",
      initialSelectedOperatorId: options.scoringResult.selectedOperatorId,
      selectedOperatorId: null,
      unroutableReason: resolveUnroutableReason(options.scoringResult),
      rejectionReasons: options.rejectionReasons,
    },
  };
}

function defaultNow(): Date {
  return new Date();
}

export async function executeRouteServiceRequest(
  database: DatabaseClient,
  jobData: RouteServiceRequestJobData | unknown,
  options: RouteServiceRequestOptions = {},
): Promise<RoutingAssignmentResult> {
  const parsedJobData = parseRouteServiceRequestJobData(jobData);
  const scorer = options.scorer ?? scoreRoutingCandidates;
  const now = options.now ?? defaultNow;

  return database.$transaction(async (tx: TransactionClient) => {
    const lockedRequest = await lockRouteServiceRequest(tx, parsedJobData);

    if (lockedRequest.kind !== "pending_locked") {
      return {
        kind: "already_processed" as const,
        organizationId: lockedRequest.request.organizationId,
        serviceRequestId: lockedRequest.request.id,
        terminalStatus: lockedRequest.terminalStatus,
      };
    }

    const candidates = await loadRoutingCandidates(tx, {
      organizationId: lockedRequest.request.organizationId,
      serviceRequestId: lockedRequest.request.id,
    });

    const scoringInput = createScoringInput(
      lockedRequest.request,
      candidates,
      now().toISOString(),
    );

    const scoringResult = scorer(scoringInput);

    const candidateById = new Map(
      candidates.map((candidate) => [candidate.operatorId, candidate] as const),
    );

    const lockTimeOutcomes: LockTimeOutcome[] = [];

    for (const rankedCandidate of scoringResult.rankedEligibleCandidates) {
      const candidate = candidateById.get(rankedCandidate.operatorId);

      if (!candidate) {
        continue;
      }

      const operatorOutcome = await lockAndRecheckSelectedOperator(
        tx,
        lockedRequest.request,
        candidate,
      );

      lockTimeOutcomes.push(
        createLockTimeOutcome(rankedCandidate, operatorOutcome),
      );

      if (operatorOutcome.kind !== "accepted") {
        continue;
      }

      const assignmentId = randomUUID();
      const routingDecisionId = randomUUID();
      const outboxEventId = randomUUID();

      const decisionSnapshot = buildAssignedDecisionSnapshot({
        request: lockedRequest.request,
        correlationId: parsedJobData.correlationId,
        scoringResult,
        lockTimeOutcomes,
        selectedOperatorId: candidate.operatorId,
      });

      const assignment = await tx.assignment.create({
        data: {
          id: assignmentId,
          organizationId: lockedRequest.request.organizationId,
          serviceRequestId: lockedRequest.request.id,
          operatorId: candidate.operatorId,
          status: "ACTIVE",
        },
      });

      const routingDecision = await tx.routingDecision.create({
        data: {
          id: routingDecisionId,
          organizationId: lockedRequest.request.organizationId,
          serviceRequestId: lockedRequest.request.id,
          assignmentId: assignment.id,
          scoringVersion: scoringResult.scoringVersion,
          outcome: "ASSIGNED",
          decisionSnapshot,
        },
      });

      await tx.serviceRequest.update({
        where: {
          id: lockedRequest.request.id,
        },
        data: {
          status: "ASSIGNED",
        },
      });

      const outboxEvent = await tx.outboxEvent.create({
        data: {
          id: outboxEventId,
          organizationId: lockedRequest.request.organizationId,
          eventType: "service_request.assigned",
          aggregateType: "service_request",
          aggregateId: lockedRequest.request.id,
          status: "PENDING",
          payload: {
            serviceRequestId: lockedRequest.request.id,
            organizationId: lockedRequest.request.organizationId,
            operatorId: candidate.operatorId,
            assignmentId: assignment.id,
            routingDecisionId: routingDecision.id,
            scoringVersion: scoringResult.scoringVersion,
            correlationId: parsedJobData.correlationId,
          },
        },
      });

      return {
        kind: "assigned" as const,
        organizationId: lockedRequest.request.organizationId,
        serviceRequestId: lockedRequest.request.id,
        operatorId: candidate.operatorId,
        assignmentId: assignment.id,
        routingDecisionId: routingDecision.id,
        outboxEventId: outboxEvent.id,
        scoringVersion: scoringResult.scoringVersion,
      };
    }

    const routingDecisionId = randomUUID();
    const rejectionReasons = collectRejectionReasons(
      scoringResult.rejectedCandidates,
      lockTimeOutcomes,
    );

    const decisionSnapshot = buildUnroutableDecisionSnapshot({
      request: lockedRequest.request,
      correlationId: parsedJobData.correlationId,
      scoringResult,
      lockTimeOutcomes,
      rejectionReasons,
    });

    const routingDecision = await tx.routingDecision.create({
      data: {
        id: routingDecisionId,
        organizationId: lockedRequest.request.organizationId,
        serviceRequestId: lockedRequest.request.id,
        assignmentId: null,
        scoringVersion: scoringResult.scoringVersion,
        outcome: "UNROUTABLE",
        decisionSnapshot,
      },
    });

    await tx.serviceRequest.update({
      where: {
        id: lockedRequest.request.id,
      },
      data: {
        status: "UNROUTABLE",
      },
    });

    return {
      kind: "unroutable" as const,
      organizationId: lockedRequest.request.organizationId,
      serviceRequestId: lockedRequest.request.id,
      routingDecisionId: routingDecision.id,
      rejectionReasons,
      scoringVersion: scoringResult.scoringVersion,
    };
  });
}
