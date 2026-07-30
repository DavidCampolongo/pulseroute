import { z } from "zod";

export const WEBHOOK_TIMESTAMP_HEADER = "x-pulseroute-timestamp";
export const WEBHOOK_SIGNATURE_HEADER = "x-pulseroute-signature";

export const WEBHOOK_PROVIDER = "pulseroute";

export const SERVICE_REQUEST_EVENT_TYPE = "service_request.created";

const identifierSchema = z.string().trim().min(1).max(200);

const serviceRequestDataSchema = z.strictObject({
  externalId: identifierSchema,

  requiredSkillId: z.uuid(),

  priority: z.enum(["LOW", "NORMAL", "HIGH"]),

  region: z.string().trim().min(1).max(100),
});

export const serviceRequestWebhookSchema = z.strictObject({
  organizationId: z.uuid(),

  eventId: identifierSchema,

  type: z.literal(SERVICE_REQUEST_EVENT_TYPE),

  data: serviceRequestDataSchema,
});

export const serviceRequestWebhookOpenApiSchema = z.toJSONSchema(
  serviceRequestWebhookSchema,
  {
    target: "openapi-3.0",
  },
);

export const webhookEvidenceIdentitySchema = z.looseObject({
  organizationId: z.uuid(),

  eventId: identifierSchema.optional(),
});

export type ServiceRequestWebhook = z.infer<typeof serviceRequestWebhookSchema>;

export type WebhookEvidenceIdentity = z.infer<
  typeof webhookEvidenceIdentitySchema
>;

export type ServiceRequestCreateValues = {
  organizationId: string;
  externalId: string;
  requiredSkillId: string;
  priority: "LOW" | "NORMAL" | "HIGH";
  region: string;
};

export function toServiceRequestCreateValues(
  webhook: ServiceRequestWebhook,
): ServiceRequestCreateValues {
  return {
    organizationId: webhook.organizationId,
    externalId: webhook.data.externalId,
    requiredSkillId: webhook.data.requiredSkillId,
    priority: webhook.data.priority,
    region: webhook.data.region,
  };
}
