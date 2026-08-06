import { Prisma, type DatabaseClient } from "@pulseroute/db";

export const REJECTION_REASONS = {
  statusNotEligible: "STATUS_NOT_ELIGIBLE",
  regionMismatch: "REGION_MISMATCH",
  missingRequiredSkill: "MISSING_REQUIRED_SKILL",
  atCapacity: "AT_CAPACITY",
} as const;

export type RejectionReasonCode =
  (typeof REJECTION_REASONS)[keyof typeof REJECTION_REASONS];

export type RoutingCandidateQueryInput = {
  organizationId: string;
  serviceRequestId: string;
};

export type RoutingCandidateRow = {
  operatorId: string;
  organizationId: string;
  status: "AVAILABLE" | "UNAVAILABLE" | "INACTIVE";
  region: string;
  maxConcurrentAssignments: number;
  activeAssignmentCount: number;
  requiredSkillId: string;
  requiredSkillLevel: number | null;
  hasRequiredSkill: boolean;
};

type QueryableDatabase = Pick<DatabaseClient, "$queryRaw">;

export async function loadRoutingCandidates(
  database: QueryableDatabase,
  input: RoutingCandidateQueryInput,
): Promise<RoutingCandidateRow[]> {
  const rows = await database.$queryRaw<RoutingCandidateRow[]>(Prisma.sql`
    SELECT
      o.id AS "operatorId",
      o.organization_id AS "organizationId",
      o.status,
      o.region,
      o.max_concurrent_assignments AS "maxConcurrentAssignments",
      (
        SELECT COUNT(*)::int
        FROM assignments a
        WHERE a.organization_id = o.organization_id
          AND a.operator_id = o.id
          AND a.status = 'ACTIVE'::"AssignmentStatus"
      ) AS "activeAssignmentCount",
      sr.required_skill_id AS "requiredSkillId",
      os.level AS "requiredSkillLevel",
      TRUE AS "hasRequiredSkill"
    FROM service_requests sr
    JOIN operators o
      ON o.organization_id = sr.organization_id
    JOIN operator_skills os
      ON os.organization_id = o.organization_id
     AND os.operator_id = o.id
     AND os.skill_id = sr.required_skill_id
    WHERE sr.id = ${input.serviceRequestId}::uuid
      AND sr.organization_id = ${input.organizationId}::uuid
      AND o.status = 'AVAILABLE'::"OperatorStatus"
      AND o.region = sr.region
    ORDER BY
      "activeAssignmentCount" ASC,
      "requiredSkillLevel" DESC,
      o.id ASC
  `);

  return rows;
}
