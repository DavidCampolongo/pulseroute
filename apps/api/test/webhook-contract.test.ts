import { describe, expect, it } from "vitest";

import {
  SERVICE_REQUEST_EVENT_TYPE,
  serviceRequestWebhookSchema,
  toServiceRequestCreateValues,
  webhookEvidenceIdentitySchema,
} from "../src/webhooks/contract.js";

const organizationId = "00000001-0000-4000-8000-000000000001";

const requiredSkillId = "00000003-0000-4000-8000-000000000004";

function validWebhook() {
  return {
    organizationId,
    eventId: "evt-phase-5-001",
    type: SERVICE_REQUEST_EVENT_TYPE,
    data: {
      externalId: "request-phase-5-001",
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  };
}

describe("service-request webhook contract", () => {
  it("accepts a valid service-request event", () => {
    const result = serviceRequestWebhookSchema.safeParse(validWebhook());

    expect(result.success).toBe(true);
  });

  it("maps a valid webhook to ServiceRequest creation values", () => {
    const webhook = serviceRequestWebhookSchema.parse(validWebhook());

    expect(toServiceRequestCreateValues(webhook)).toEqual({
      organizationId,
      externalId: "request-phase-5-001",
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    });
  });

  it("does not allow the sender to provide ServiceRequest status", () => {
    const webhook = validWebhook();

    const result = serviceRequestWebhookSchema.safeParse({
      ...webhook,
      data: {
        ...webhook.data,
        status: "ASSIGNED",
      },
    });

    expect(result.success).toBe(false);
  });

  it("rejects an unsupported priority", () => {
    const webhook = validWebhook();

    const result = serviceRequestWebhookSchema.safeParse({
      ...webhook,
      data: {
        ...webhook.data,
        priority: "URGENT",
      },
    });

    expect(result.success).toBe(false);
  });

  it("rejects an invalid organization ID", () => {
    const result = serviceRequestWebhookSchema.safeParse({
      ...validWebhook(),
      organizationId: "not-a-uuid",
    });

    expect(result.success).toBe(false);
  });

  it("rejects an invalid required skill ID", () => {
    const webhook = validWebhook();

    const result = serviceRequestWebhookSchema.safeParse({
      ...webhook,
      data: {
        ...webhook.data,
        requiredSkillId: "not-a-uuid",
      },
    });

    expect(result.success).toBe(false);
  });

  it("rejects an unsupported event type", () => {
    const result = serviceRequestWebhookSchema.safeParse({
      ...validWebhook(),
      type: "service_request.cancelled",
    });

    expect(result.success).toBe(false);
  });

  it("rejects unexpected top-level fields", () => {
    const result = serviceRequestWebhookSchema.safeParse({
      ...validWebhook(),
      unexpected: true,
    });

    expect(result.success).toBe(false);
  });
});

describe("webhook malformed-evidence identity", () => {
  it("extracts organization and event identity even when business data is invalid", () => {
    const malformedWebhook = {
      organizationId,
      eventId: "evt-malformed-001",
      type: SERVICE_REQUEST_EVENT_TYPE,
      data: {
        priority: "VERY_HIGH",
      },
    };

    expect(
      serviceRequestWebhookSchema.safeParse(malformedWebhook).success,
    ).toBe(false);

    expect(webhookEvidenceIdentitySchema.parse(malformedWebhook)).toMatchObject(
      {
        organizationId,
        eventId: "evt-malformed-001",
      },
    );
  });

  it("can identify malformed evidence even when eventId is absent", () => {
    const result = webhookEvidenceIdentitySchema.safeParse({
      organizationId,
      type: SERVICE_REQUEST_EVENT_TYPE,
      data: {},
    });

    expect(result.success).toBe(true);

    if (result.success) {
      expect(result.data.organizationId).toBe(organizationId);
      expect(result.data.eventId).toBeUndefined();
    }
  });

  it("refuses to attribute evidence when organization identity is missing", () => {
    const result = webhookEvidenceIdentitySchema.safeParse({
      eventId: "evt-malformed-002",
      data: {},
    });

    expect(result.success).toBe(false);
  });

  it("refuses to attribute evidence when organization identity is malformed", () => {
    const result = webhookEvidenceIdentitySchema.safeParse({
      organizationId: "not-a-uuid",
      eventId: "evt-malformed-003",
    });

    expect(result.success).toBe(false);
  });
});
