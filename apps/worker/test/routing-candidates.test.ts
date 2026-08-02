import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it } from "vitest";

import { loadRoutingCandidates } from "../src/routing-candidates.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for routing candidate tests");
}

const database = createDatabaseClient(databaseUrl);

afterAll(async () => {
  await database.$disconnect();
});

describe("loadRoutingCandidates", () => {
  it("loads decision-time facts for a routable request", async () => {
    const rows = await loadRoutingCandidates(database, {
      organizationId: "00000001-0000-4000-8000-000000000001",
      serviceRequestId: "00000005-0000-4000-8000-000000000012",
    });

    expect(rows).toEqual([
      {
        operatorId: "00000004-0000-4000-8000-000000000016",
        organizationId: "00000001-0000-4000-8000-000000000001",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 2,
        activeAssignmentCount: 0,
        requiredSkillId: "00000003-0000-4000-8000-000000000004",
        requiredSkillLevel: 4,
        hasRequiredSkill: true,
      },
    ]);
  });

  it("loads the at-capacity candidate facts for a hard unroutable request", async () => {
    const rows = await loadRoutingCandidates(database, {
      organizationId: "00000001-0000-4000-8000-000000000001",
      serviceRequestId: "00000005-0000-4000-8000-000000000010",
    });

    expect(rows).toEqual([
      {
        operatorId: "00000004-0000-4000-8000-000000000019",
        organizationId: "00000001-0000-4000-8000-000000000001",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 1,
        activeAssignmentCount: 1,
        requiredSkillId: "00000003-0000-4000-8000-000000000008",
        requiredSkillLevel: 5,
        hasRequiredSkill: true,
      },
    ]);
  });

  it("does not mutate the returned rows across repeated evaluation", async () => {
    const first = await loadRoutingCandidates(database, {
      organizationId: "00000001-0000-4000-8000-000000000001",
      serviceRequestId: "00000005-0000-4000-8000-000000000012",
    });

    const second = await loadRoutingCandidates(database, {
      organizationId: "00000001-0000-4000-8000-000000000001",
      serviceRequestId: "00000005-0000-4000-8000-000000000012",
    });

    expect(first).toEqual(second);
    expect(first[0]?.operatorId).toBe("00000004-0000-4000-8000-000000000016");
  });
});
