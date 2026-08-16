import { Prisma, type DatabaseClient } from "@pulseroute/db";
import {
  SCORING_REJECTION_REASON_CODES,
  type ScoringRejectionReasonCode,
} from "@pulseroute/scoring";

import type { RoutingCandidateRow } from "./routing-candidates.js";
import type { LockedServiceRequest } from "./routing-transaction.js";

type QueryableDatabase = Pick<DatabaseClient, "$queryRaw">;

type LockedOperatorRow = {
  operatorId: string;
  organizationId: string;
  status: "AVAILABLE" | "UNAVAILABLE" | "INACTIVE";
  region: string;
  maxConcurrentAssignments: number;
};

type OperatorSkillRow = {
  requiredSkillLevel: number | null;
};

export type LockedOperatorOutcome =
  | {
      kind: "accepted";
      operator: LockedOperatorRow;
      activeAssignmentCount: number;
      requiredSkillLevel: number;
    }
  | {
      kind: "rejected";
      operator: LockedOperatorRow | null;
      activeAssignmentCount: number | null;
      requiredSkillLevel: number | null;
      rejectionReasons: ScoringRejectionReasonCode[];
    };

function uniqueReasons(
  reasons: ScoringRejectionReasonCode[],
): ScoringRejectionReasonCode[] {
  return [...new Set(reasons)];
}

export async function lockAndRecheckSelectedOperator(
  database: QueryableDatabase,
  request: LockedServiceRequest,
  candidate: RoutingCandidateRow,
): Promise<LockedOperatorOutcome> {
  const lockedOperators = await database.$queryRaw<LockedOperatorRow[]>(
    Prisma.sql`
      SELECT
        id AS "operatorId",
        organization_id AS "organizationId",
        status,
        region,
        max_concurrent_assignments AS "maxConcurrentAssignments"
      FROM operators
      WHERE id = ${candidate.operatorId}::uuid
        AND organization_id = ${candidate.organizationId}::uuid
      FOR UPDATE
    `,
  );

  const lockedOperator = lockedOperators[0];

  if (!lockedOperator) {
    return {
      kind: "rejected",
      operator: null,
      activeAssignmentCount: null,
      requiredSkillLevel: null,
      rejectionReasons: [SCORING_REJECTION_REASON_CODES.operatorNotAvailable],
    };
  }

  const activeAssignments = await database.$queryRaw<
    { activeAssignmentCount: number }[]
  >(Prisma.sql`
      SELECT COUNT(*)::int AS "activeAssignmentCount"
      FROM assignments
      WHERE organization_id = ${lockedOperator.organizationId}::uuid
        AND operator_id = ${lockedOperator.operatorId}::uuid
        AND status = 'ACTIVE'::"AssignmentStatus"
    `);

  const activeAssignmentCount =
    activeAssignments[0]?.activeAssignmentCount ?? 0;

  const requiredSkillRows = await database.$queryRaw<OperatorSkillRow[]>(
    Prisma.sql`
      SELECT os.level AS "requiredSkillLevel"
      FROM operator_skills os
      WHERE os.organization_id = ${lockedOperator.organizationId}::uuid
        AND os.operator_id = ${lockedOperator.operatorId}::uuid
        AND os.skill_id = ${request.requiredSkillId}::uuid
      LIMIT 1
    `,
  );

  const requiredSkillLevel = requiredSkillRows[0]?.requiredSkillLevel ?? null;

  const rejectionReasons: ScoringRejectionReasonCode[] = [];

  if (lockedOperator.status !== "AVAILABLE") {
    rejectionReasons.push(SCORING_REJECTION_REASON_CODES.operatorNotAvailable);
  }

  if (lockedOperator.region !== request.region) {
    rejectionReasons.push(SCORING_REJECTION_REASON_CODES.regionIncompatible);
  }

  if (requiredSkillLevel === null) {
    rejectionReasons.push(SCORING_REJECTION_REASON_CODES.missingRequiredSkill);
  }

  if (activeAssignmentCount >= lockedOperator.maxConcurrentAssignments) {
    rejectionReasons.push(SCORING_REJECTION_REASON_CODES.atCapacity);
  }

  if (rejectionReasons.length > 0) {
    return {
      kind: "rejected",
      operator: lockedOperator,
      activeAssignmentCount,
      requiredSkillLevel,
      rejectionReasons: uniqueReasons(rejectionReasons),
    };
  }

  return {
    kind: "accepted",
    operator: lockedOperator,
    activeAssignmentCount,
    requiredSkillLevel: requiredSkillLevel ?? 0,
  };
}
