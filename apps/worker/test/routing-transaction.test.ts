import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it } from "vitest";

import {
  prepareRouteServiceRequest,
  RouteServiceRequestMissingError,
  RouteServiceRequestTenantMismatchError,
} from "../src/routing-transaction.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for routing transaction tests");
}

const database = createDatabaseClient(databaseUrl);

type RequestFixture = {
  organizationId: string;
  skillId: string;
  serviceRequestId: string;
};

type RequestFixtureOptions = {
  status: "PENDING" | "ASSIGNED" | "CANCELLED";
  priority: "LOW" | "NORMAL" | "HIGH";
  region: "NORTH" | "SOUTH" | "EAST" | "WEST";
};

async function createRequestFixture(
  options: RequestFixtureOptions,
): Promise<RequestFixture> {
  const organizationId = randomUUID();
  const skillId = randomUUID();
  const serviceRequestId = randomUUID();

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Routing Transaction Test Org ${organizationId}`,
    },
  });

  await database.skill.create({
    data: {
      id: skillId,
      organizationId,
      name: `Routing Transaction Test Skill ${skillId}`,
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `routing-transaction-${serviceRequestId}`,
      requiredSkillId: skillId,
      status: options.status,
      priority: options.priority,
      region: options.region,
    },
  });

  return {
    organizationId,
    skillId,
    serviceRequestId,
  };
}

async function clearRequestFixture(fixture: RequestFixture): Promise<void> {
  await database.assignment.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.routingDecision.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.outboxEvent.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.serviceRequest.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId: fixture.organizationId,
    },
  });

  await database.organization.deleteMany({
    where: {
      id: fixture.organizationId,
    },
  });
}

afterAll(async () => {
  await database.$disconnect();
});

describe("prepareRouteServiceRequest", () => {
  it("locks a pending request and returns the pending outcome", async () => {
    const fixture = await createRequestFixture({
      status: "PENDING",
      priority: "NORMAL",
      region: "WEST",
    });

    try {
      const result = await prepareRouteServiceRequest(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `request-${randomUUID()}`,
      });

      expect(result).toEqual({
        kind: "pending_locked",
        request: {
          id: fixture.serviceRequestId,
          organizationId: fixture.organizationId,
          status: "PENDING",
          requiredSkillId: fixture.skillId,
          priority: "NORMAL",
          region: "WEST",
        },
      });
    } finally {
      await clearRequestFixture(fixture);
    }
  });

  it("returns a clean no-op for an already assigned request", async () => {
    const fixture = await createRequestFixture({
      status: "ASSIGNED",
      priority: "HIGH",
      region: "WEST",
    });

    try {
      const result = await prepareRouteServiceRequest(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `request-${randomUUID()}`,
      });

      expect(result).toEqual({
        kind: "terminal_no_op",
        terminalStatus: "ASSIGNED",
        request: {
          id: fixture.serviceRequestId,
          organizationId: fixture.organizationId,
          status: "ASSIGNED",
          requiredSkillId: fixture.skillId,
          priority: "HIGH",
          region: "WEST",
        },
      });
    } finally {
      await clearRequestFixture(fixture);
    }
  });

  it("returns a clean no-op for a cancelled request", async () => {
    const fixture = await createRequestFixture({
      status: "CANCELLED",
      priority: "NORMAL",
      region: "SOUTH",
    });

    try {
      const result = await prepareRouteServiceRequest(database, {
        organizationId: fixture.organizationId,
        serviceRequestId: fixture.serviceRequestId,
        correlationId: `request-${randomUUID()}`,
      });

      expect(result).toEqual({
        kind: "terminal_no_op",
        terminalStatus: "CANCELLED",
        request: {
          id: fixture.serviceRequestId,
          organizationId: fixture.organizationId,
          status: "CANCELLED",
          requiredSkillId: fixture.skillId,
          priority: "NORMAL",
          region: "SOUTH",
        },
      });
    } finally {
      await clearRequestFixture(fixture);
    }
  });

  it("rejects a tenant mismatch", async () => {
    const fixture = await createRequestFixture({
      status: "PENDING",
      priority: "NORMAL",
      region: "WEST",
    });

    try {
      await expect(
        prepareRouteServiceRequest(database, {
          organizationId: randomUUID(),
          serviceRequestId: fixture.serviceRequestId,
          correlationId: `request-${randomUUID()}`,
        }),
      ).rejects.toBeInstanceOf(RouteServiceRequestTenantMismatchError);
    } finally {
      await clearRequestFixture(fixture);
    }
  });

  it("rejects a missing request", async () => {
    await expect(
      prepareRouteServiceRequest(database, {
        organizationId: randomUUID(),
        serviceRequestId: randomUUID(),
        correlationId: `request-${randomUUID()}`,
      }),
    ).rejects.toBeInstanceOf(RouteServiceRequestMissingError);
  });
});
