import { randomUUID } from "node:crypto";

import { Prisma, type DatabaseClient } from "@pulseroute/db";
import {
  evaluateRoutingPlan,
  LEGACY_SCORING_VERSION,
  type RoutingCandidateInput,
} from "@pulseroute/scoring";
import type { RouteServiceRequestJobData } from "@pulseroute/shared";

import {
  loadRoutingCandidates,
  type RejectionReasonCode,
  type RoutingCandidateRow,
} from "./routing-candidates.js";
import {
  lockAndRecheckSelectedOperator,
  type LockedOperatorOutcome,
} from "./routing-operator.js";
import {
  lockRouteServiceRequest,
  parseRouteServiceRequestJobData,
  type LockedServiceRequest,
} from "./routing-transaction.js";

const SCORING_VERSION = LEGACY_SCORING_VERSION;

type TransactionClient = Pick<
  DatabaseClient,
  | "$queryRaw"
  | "assignment"
  | "routingDecision"
  | "serviceRequest"
  | "outboxEvent"
>;

type DecisionRejectedCandidate = {
  operatorId: string;
  rejectionReasons: RejectionReasonCode[];
};

type DecisionRankedCandidate = ReturnType<
  typeof evaluateRoutingPlan
>["rankedEligibleCandidates"][number];

export type RoutingAssignmentResult =
  | {
      kind: "assigned";
      organizationId: string;
      serviceRequestId: string;
      operatorId: string;
      assignmentId: string;
      routingDecisionId: string;
      outboxEventId: string;
      scoringVersion: typeof SCORING_VERSION;
    }
  | {
      kind: "unroutable";
      organizationId: string;
      serviceRequestId: string;
      routingDecisionId: string;
      rejectionReasons: RejectionReasonCode[];
      scoringVersion: typeof SCORING_VERSION;
    }
  | {
      kind: "already_processed";
      organizationId: string;
      serviceRequestId: string;
      terminalStatus: Exclude<LockedServiceRequest["status"], "PENDING">;
    };

function toScoringInput(candidate: RoutingCandidateRow): RoutingCandidateInput {
  return {
    operatorId: candidate.operatorId,
    organizationId: candidate.organizationId,
    status: candidate.status,
    region: candidate.region,
    maxConcurrentAssignments: candidate.maxConcurrentAssignments,
    activeAssignmentCount: candidate.activeAssignmentCount,
    requiredSkillLevel: candidate.requiredSkillLevel,
    hasRequiredSkill: candidate.hasRequiredSkill,
  };
}

function collectRejectionReasons(
  rejectedCandidates: DecisionRejectedCandidate[],
): RejectionReasonCode[] {
  return [
    ...new Set(
      rejectedCandidates.flatMap((candidate) => candidate.rejectionReasons),
    ),
  ];
}

function buildAssignedDecisionSnapshot(options: {
  request: LockedServiceRequest;
  correlationId: string;
  rankedEligibleCandidates: DecisionRankedCandidate[];
  rejectedCandidates: DecisionRejectedCandidate[];
  selectedCandidate: RoutingCandidateRow;
  selectedOperator: Extract<LockedOperatorOutcome, { kind: "accepted" }>;
}): Prisma.InputJsonValue {
  return {
    scoringVersion: SCORING_VERSION,
    correlationId: options.correlationId,
    request: {
      id: options.request.id,
      organizationId: options.request.organizationId,
      requiredSkillId: options.request.requiredSkillId,
      priority: options.request.priority,
      region: options.request.region,
      status: options.request.status,
    },
    candidates: {
      rankedEligibleCandidates: options.rankedEligibleCandidates,
      rejectedCandidates: options.rejectedCandidates,
    },
    selectedOperator: {
      operatorId: options.selectedOperator.operator.operatorId,
      organizationId: options.selectedOperator.operator.organizationId,
      status: options.selectedOperator.operator.status,
      region: options.selectedOperator.operator.region,
      maxConcurrentAssignments:
        options.selectedOperator.operator.maxConcurrentAssignments,
      activeAssignmentCount: options.selectedOperator.activeAssignmentCount,
      requiredSkillLevel: options.selectedOperator.requiredSkillLevel,
      rank:
        options.rankedEligibleCandidates.find(
          (candidate) =>
            candidate.operatorId === options.selectedCandidate.operatorId,
        )?.rank ?? 1,
    },
    result: {
      outcome: "ASSIGNED",
      selectedOperatorId: options.selectedCandidate.operatorId,
    },
  };
}

function buildUnroutableDecisionSnapshot(options: {
  request: LockedServiceRequest;
  correlationId: string;
  rankedEligibleCandidates: DecisionRankedCandidate[];
  rejectedCandidates: DecisionRejectedCandidate[];
}): Prisma.InputJsonValue {
  return {
    scoringVersion: SCORING_VERSION,
    correlationId: options.correlationId,
    request: {
      id: options.request.id,
      organizationId: options.request.organizationId,
      requiredSkillId: options.request.requiredSkillId,
      priority: options.request.priority,
      region: options.request.region,
      status: options.request.status,
    },
    candidates: {
      rankedEligibleCandidates: options.rankedEligibleCandidates,
      rejectedCandidates: options.rejectedCandidates,
    },
    result: {
      outcome: "UNROUTABLE",
      selectedOperatorId: null,
      rejectionReasons: collectRejectionReasons(options.rejectedCandidates),
    },
  };
}

export async function executeRouteServiceRequest(
  database: DatabaseClient,
  jobData: RouteServiceRequestJobData | unknown,
): Promise<RoutingAssignmentResult> {
  const parsedJobData = parseRouteServiceRequestJobData(jobData);

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

    const plan = evaluateRoutingPlan(
      {
        organizationId: lockedRequest.request.organizationId,
        serviceRequestId: lockedRequest.request.id,
        requiredSkillId: lockedRequest.request.requiredSkillId,
        region: lockedRequest.request.region,
        priority: lockedRequest.request.priority,
      },
      candidates.map(toScoringInput),
    );

    const candidateById = new Map(
      candidates.map((candidate) => [candidate.operatorId, candidate] as const),
    );

    const rejectedCandidates: DecisionRejectedCandidate[] = [
      ...plan.rejectedCandidates,
    ];

    for (const rankedCandidate of plan.rankedEligibleCandidates) {
      const candidate = candidateById.get(rankedCandidate.operatorId);

      if (!candidate) {
        continue;
      }

      const operatorOutcome = await lockAndRecheckSelectedOperator(
        tx,
        lockedRequest.request,
        candidate,
      );

      if (operatorOutcome.kind !== "accepted") {
        rejectedCandidates.push({
          operatorId: candidate.operatorId,
          rejectionReasons: operatorOutcome.rejectionReasons,
        });

        continue;
      }

      const assignmentId = randomUUID();
      const routingDecisionId = randomUUID();
      const outboxEventId = randomUUID();

      const decisionSnapshot = buildAssignedDecisionSnapshot({
        request: lockedRequest.request,
        correlationId: parsedJobData.correlationId,
        rankedEligibleCandidates: plan.rankedEligibleCandidates,
        rejectedCandidates,
        selectedCandidate: candidate,
        selectedOperator: operatorOutcome,
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
          scoringVersion: SCORING_VERSION,
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
            scoringVersion: SCORING_VERSION,
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
        scoringVersion: SCORING_VERSION,
      };
    }

    const routingDecisionId = randomUUID();
    const rejectionReasons = collectRejectionReasons(rejectedCandidates);

    const decisionSnapshot = buildUnroutableDecisionSnapshot({
      request: lockedRequest.request,
      correlationId: parsedJobData.correlationId,
      rankedEligibleCandidates: plan.rankedEligibleCandidates,
      rejectedCandidates,
    });

    const routingDecision = await tx.routingDecision.create({
      data: {
        id: routingDecisionId,
        organizationId: lockedRequest.request.organizationId,
        serviceRequestId: lockedRequest.request.id,
        assignmentId: null,
        scoringVersion: SCORING_VERSION,
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
      scoringVersion: SCORING_VERSION,
    };
  });
}
