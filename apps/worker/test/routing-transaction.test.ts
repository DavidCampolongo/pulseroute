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

async function createTenantMismatchFixture(): Promise<{
  organizationId: string;
  serviceRequestId: string;
}> {
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
      status: "PENDING",
      priority: "NORMAL",
      region: "WEST",
    },
  });

  return {
    organizationId,
    serviceRequestId,
  };
}

async function clearTenantMismatchFixture(
  organizationId: string,
  serviceRequestId: string,
): Promise<void> {
  await database.serviceRequest.deleteMany({
    where: {
      id: serviceRequestId,
      organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });
}

afterAll(async () => {
  await database.$disconnect();
});

describe("prepareRouteServiceRequest", () => {
  it("locks a pending request and returns the pending outcome", async () => {
    const result = await prepareRouteServiceRequest(database, {
      organizationId: "00000001-0000-4000-8000-000000000001",
      serviceRequestId: "00000005-0000-4000-8000-000000000012",
      correlationId: `req-${randomUUID()}`,
    });

    expect(result).toEqual({
      kind: "pending_locked",
      request: {
        id: "00000005-0000-4000-8000-000000000012",
        organizationId: "00000001-0000-4000-8000-000000000001",
        status: "PENDING",
        requiredSkillId: "00000003-0000-4000-8000-000000000004",
        priority: "NORMAL",
        region: "WEST",
      },
    });
  });

  it("returns a clean no-op for an already assigned request", async () => {
    const result = await prepareRouteServiceRequest(database, {
      organizationId: "00000001-0000-4000-8000-000000000001",
      serviceRequestId: "00000005-0000-4000-8000-000000000001",
      correlationId: `req-${randomUUID()}`,
    });

    expect(result).toEqual({
      kind: "terminal_no_op",
      terminalStatus: "ASSIGNED",
      request: {
        id: "00000005-0000-4000-8000-000000000001",
        organizationId: "00000001-0000-4000-8000-000000000001",
        status: "ASSIGNED",
        requiredSkillId: "00000003-0000-4000-8000-000000000008",
        priority: "HIGH",
        region: "WEST",
      },
    });
  });

  it("returns a clean no-op for a cancelled request", async () => {
    const result = await prepareRouteServiceRequest(database, {
      organizationId: "00000001-0000-4000-8000-000000000001",
      serviceRequestId: "00000005-0000-4000-8000-000000000008",
      correlationId: `req-${randomUUID()}`,
    });

    expect(result).toEqual({
      kind: "terminal_no_op",
      terminalStatus: "CANCELLED",
      request: {
        id: "00000005-0000-4000-8000-000000000008",
        organizationId: "00000001-0000-4000-8000-000000000001",
        status: "CANCELLED",
        requiredSkillId: "00000003-0000-4000-8000-000000000003",
        priority: "NORMAL",
        region: "SOUTH",
      },
    });
  });

  it("rejects a tenant mismatch", async () => {
    const fixture = await createTenantMismatchFixture();

    try {
      await expect(
        prepareRouteServiceRequest(database, {
          organizationId: "00000001-0000-4000-8000-000000000001",
          serviceRequestId: fixture.serviceRequestId,
          correlationId: `req-${randomUUID()}`,
        }),
      ).rejects.toBeInstanceOf(RouteServiceRequestTenantMismatchError);
    } finally {
      await clearTenantMismatchFixture(
        fixture.organizationId,
        fixture.serviceRequestId,
      );
    }
  });

  it("rejects a missing request", async () => {
    await expect(
      prepareRouteServiceRequest(database, {
        organizationId: "00000001-0000-4000-8000-000000000001",
        serviceRequestId: randomUUID(),
        correlationId: `req-${randomUUID()}`,
      }),
    ).rejects.toBeInstanceOf(RouteServiceRequestMissingError);
  });
});
