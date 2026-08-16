import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  SCORING_REJECTION_REASON_CODES,
  SCORING_VERSION,
} from "@pulseroute/scoring";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it } from "vitest";

import { executeRouteServiceRequest } from "../src/routing-workflow.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error(
    "DATABASE_URL is required for routing lock-time fallback tests",
  );
}

const testRunId = randomUUID();
const workerApplicationName = `pr-lock-fallback-${testRunId}`;

function addApplicationName(
  connectionUrl: string,
  applicationName: string,
): string {
  const url = new URL(connectionUrl);

  url.searchParams.set("application_name", applicationName);

  return url.toString();
}

const fixtureDatabase = createDatabaseClient(databaseUrl);
const blockerDatabase = createDatabaseClient(databaseUrl);
const workerDatabase = createDatabaseClient(
  addApplicationName(databaseUrl, workerApplicationName),
);

type FallbackFixture = {
  organizationId: string;
  skillId: string;
  firstOperatorId: string;
  secondOperatorId: string;
  serviceRequestId: string;
};

type BlockedWorkerRow = {
  applicationName: string;
};

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitForWorkerToBlockOnOperator(): Promise<void> {
  const deadline = Date.now() + 3_000;

  while (Date.now() < deadline) {
    const blockedRows = await fixtureDatabase.$queryRaw<BlockedWorkerRow[]>`
      SELECT application_name AS "applicationName"
      FROM pg_stat_activity
      WHERE wait_event_type = 'Lock'
        AND application_name = ${workerApplicationName}
    `;

    if (blockedRows.length > 0) {
      return;
    }

    await delay(10);
  }

  throw new Error(
    "Routing workflow did not reach the blocked preferred-Operator lock",
  );
}

async function createFallbackFixture(): Promise<FallbackFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const firstOperatorId = randomUUID();
  const secondOperatorId = randomUUID();
  const serviceRequestId = randomUUID();

  await fixtureDatabase.organization.create({
    data: {
      id: organizationId,
      name: `Lock-Time Fallback Org ${organizationId}`,
    },
  });

  await fixtureDatabase.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Lock-Time Fallback Skill ${skillId}`,
    },
  });

  await fixtureDatabase.operator.createMany({
    data: [
      {
        id: firstOperatorId,
        organizationId,
        name: "Initially First Ranked",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 3,
      },
      {
        id: secondOperatorId,
        organizationId,
        name: "Initially Second Ranked",
        status: "AVAILABLE",
        region: "WEST",
        maxConcurrentAssignments: 3,
      },
    ],
  });

  await fixtureDatabase.operatorSkill.createMany({
    data: [
      {
        organizationId,
        operatorId: firstOperatorId,
        skillId,
        level: 5,
      },
      {
        organizationId,
        operatorId: secondOperatorId,
        skillId,
        level: 3,
      },
    ],
  });

  await fixtureDatabase.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `lock-time-fallback-${serviceRequestId}`,
      requiredSkillId: skillId,
      status: "PENDING",
      priority: "HIGH",
      region: "WEST",
    },
  });

  return {
    organizationId,
    skillId,
    firstOperatorId,
    secondOperatorId,
    serviceRequestId,
  };
}

async function clearFallbackFixture(fixture: FallbackFixture): Promise<void> {
  await fixtureDatabase.outboxEvent.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.routingDecision.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.assignment.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.serviceRequest.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.operatorSkill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.operator.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.skill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await fixtureDatabase.organization.deleteMany({
    where: {
      id: fixture.organizationId,
    },
  });
}

afterAll(async () => {
  await fixtureDatabase.$disconnect();
  await blockerDatabase.$disconnect();
  await workerDatabase.$disconnect();
});

describe("routing lock-time fallback", () => {
  it("preserves scoring-time rank while safely falling back after lock-time invalidation", async () => {
    const fixture = await createFallbackFixture();

    let releaseBlocker = (): void => undefined;
    let reportBlockerReady = (): void => undefined;

    const blockerReleased = new Promise<void>((resolve) => {
      releaseBlocker = resolve;
    });

    const blockerReady = new Promise<void>((resolve) => {
      reportBlockerReady = resolve;
    });

    const blockerTransaction = blockerDatabase.$transaction(
      async (transaction) => {
        await transaction.$queryRaw`
          SELECT id
          FROM operators
          WHERE id = ${fixture.firstOperatorId}::uuid
          FOR UPDATE
        `;

        await transaction.operator.update({
          where: {
            id: fixture.firstOperatorId,
          },
          data: {
            status: "UNAVAILABLE",
          },
        });

        reportBlockerReady();

        await blockerReleased;
      },
      {
        timeout: 10_000,
      },
    );

    await blockerReady;

    try {
      const routingPromise = executeRouteServiceRequest(
        workerDatabase,
        {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
          correlationId: `lock-time-fallback-${randomUUID()}`,
        },
        {
          now: () => new Date("2026-08-15T12:00:00.000Z"),
        },
      );

      await waitForWorkerToBlockOnOperator();

      releaseBlocker();

      const [result] = await Promise.all([routingPromise, blockerTransaction]);

      expect(result.kind).toBe("assigned");

      if (result.kind !== "assigned") {
        throw new Error("Expected routing to use the fallback Operator");
      }

      expect(result.operatorId).toBe(fixture.secondOperatorId);
      expect(result.scoringVersion).toBe(SCORING_VERSION);

      const [assignments, routingDecisions] = await Promise.all([
        fixtureDatabase.assignment.findMany({
          where: {
            organizationId: fixture.organizationId,
            serviceRequestId: fixture.serviceRequestId,
          },
        }),
        fixtureDatabase.routingDecision.findMany({
          where: {
            organizationId: fixture.organizationId,
            serviceRequestId: fixture.serviceRequestId,
          },
        }),
      ]);

      expect(assignments).toHaveLength(1);
      expect(assignments[0]?.operatorId).toBe(fixture.secondOperatorId);
      expect(routingDecisions).toHaveLength(1);

      const snapshot = routingDecisions[0]?.decisionSnapshot as unknown as {
        scoring: {
          initialSelectedOperatorId: string;
          rankedEligibleCandidates: Array<{
            operatorId: string;
            rank: number;
          }>;
          rejectedCandidates: unknown[];
        };
        lockTimeOutcomes: Array<{
          operatorId: string;
          scoringRank: number;
          outcome: string;
          reasons?: string[];
          observedFacts: {
            status: string;
          };
        }>;
        result: {
          outcome: string;
          initialSelectedOperatorId: string;
          selectedOperatorId: string;
          fallbackUsed: boolean;
        };
      };

      expect(snapshot.scoring).toMatchObject({
        initialSelectedOperatorId: fixture.firstOperatorId,
        rankedEligibleCandidates: [
          {
            operatorId: fixture.firstOperatorId,
            rank: 1,
          },
          {
            operatorId: fixture.secondOperatorId,
            rank: 2,
          },
        ],
        rejectedCandidates: [],
      });

      expect(snapshot.lockTimeOutcomes).toEqual([
        expect.objectContaining({
          operatorId: fixture.firstOperatorId,
          scoringRank: 1,
          outcome: "REJECTED",
          reasons: [SCORING_REJECTION_REASON_CODES.operatorNotAvailable],
          observedFacts: expect.objectContaining({
            status: "UNAVAILABLE",
          }),
        }),
        expect.objectContaining({
          operatorId: fixture.secondOperatorId,
          scoringRank: 2,
          outcome: "ACCEPTED",
          observedFacts: expect.objectContaining({
            status: "AVAILABLE",
          }),
        }),
      ]);

      expect(snapshot.result).toEqual({
        outcome: "ASSIGNED",
        initialSelectedOperatorId: fixture.firstOperatorId,
        selectedOperatorId: fixture.secondOperatorId,
        fallbackUsed: true,
      });
    } finally {
      releaseBlocker();
      await blockerTransaction.catch(() => undefined);
      await clearFallbackFixture(fixture);
    }
  }, 10_000);
});
