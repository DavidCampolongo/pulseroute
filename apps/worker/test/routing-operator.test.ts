import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it } from "vitest";

import { loadRoutingCandidates } from "../src/routing-candidates.js";
import { lockAndRecheckSelectedOperator } from "../src/routing-operator.js";
import type { LockedServiceRequest } from "../src/routing-transaction.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for routing operator tests");
}

const database = createDatabaseClient(databaseUrl);

afterAll(async () => {
  await database.$disconnect();
});

describe("lockAndRecheckSelectedOperator", () => {
  it("accepts the west happy-path operator after locking and rechecking", async () => {
    const request: LockedServiceRequest = {
      id: "00000005-0000-4000-8000-000000000012",
      organizationId: "00000001-0000-4000-8000-000000000001",
      status: "PENDING",
      requiredSkillId: "00000003-0000-4000-8000-000000000004",
      priority: "NORMAL",
      region: "WEST",
    };

    const candidates = await loadRoutingCandidates(database, {
      organizationId: request.organizationId,
      serviceRequestId: request.id,
    });

    expect(candidates).toHaveLength(1);

    const result = await lockAndRecheckSelectedOperator(
      database,
      request,
      candidates[0]!,
    );

    expect(result).toEqual({
      kind: "accepted",
      operator: {
        operatorId: "00000004-0000-4000-8000-000000000016",
        organizationId: "00000001-0000-4000-8000-000000000001",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 2,
      },
      activeAssignmentCount: 0,
      requiredSkillLevel: 4,
    });
  });

  it("rejects the hard west operator because it is at capacity under lock", async () => {
    const request: LockedServiceRequest = {
      id: "00000005-0000-4000-8000-000000000010",
      organizationId: "00000001-0000-4000-8000-000000000001",
      status: "PENDING",
      requiredSkillId: "00000003-0000-4000-8000-000000000008",
      priority: "HIGH",
      region: "WEST",
    };

    const candidates = await loadRoutingCandidates(database, {
      organizationId: request.organizationId,
      serviceRequestId: request.id,
    });

    expect(candidates).toHaveLength(1);

    const result = await lockAndRecheckSelectedOperator(
      database,
      request,
      candidates[0]!,
    );

    expect(result).toEqual({
      kind: "rejected",
      operator: {
        operatorId: "00000004-0000-4000-8000-000000000019",
        organizationId: "00000001-0000-4000-8000-000000000001",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 1,
      },
      activeAssignmentCount: 1,
      requiredSkillLevel: 5,
      rejectionReasons: ["AT_CAPACITY"],
    });
  });
});
