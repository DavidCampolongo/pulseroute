import { Buffer } from "node:buffer";
import console from "node:console";
import { createHmac } from "node:crypto";
import process from "node:process";
import { fileURLToPath, URL } from "node:url";

import { config as loadEnvironmentFile } from "dotenv";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const webhookSecret = process.env.WEBHOOK_SECRET;

if (!webhookSecret) {
  throw new Error("WEBHOOK_SECRET is required in the local root .env file");
}

const [organizationId, requiredSkillId, externalIdArgument, eventIdArgument] =
  process.argv.slice(2);

if (!organizationId || !requiredSkillId) {
  console.error(
    [
      "Usage:",
      "node apps/api/scripts/send-webhook.mjs \\",
      "  <organization-id> \\",
      "  <required-skill-id> \\",
      "  [external-request-id] \\",
      "  [external-event-id]",
    ].join("\n"),
  );

  process.exitCode = 1;
} else {
  const uniqueSuffix = Date.now();

  const externalId = externalIdArgument ?? `manual-request-${uniqueSuffix}`;

  const eventId = eventIdArgument ?? `manual-event-${uniqueSuffix}`;

  const rawBody = JSON.stringify({
    organizationId,
    eventId,
    type: "service_request.created",
    data: {
      externalId,
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  });

  const timestamp = String(Math.floor(Date.now() / 1000));

  const signature = createHmac("sha256", webhookSecret)
    .update(timestamp, "utf8")
    .update(".", "utf8")
    .update(Buffer.from(rawBody, "utf8"))
    .digest("hex");

  const port = process.env.API_PORT ?? "3000";

  const endpoint = `http://127.0.0.1:${port}` + "/webhooks/service-requests";

  try {
    const response = await globalThis.fetch(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-pulseroute-timestamp": timestamp,
        "x-pulseroute-signature": signature,
      },
      body: rawBody,
    });

    const responseText = await response.text();

    let responseBody;

    try {
      responseBody = JSON.parse(responseText);
    } catch {
      responseBody = responseText;
    }

    console.log(
      JSON.stringify(
        {
          endpoint,
          externalId,
          eventId,
          statusCode: response.status,
          response: responseBody,
        },
        null,
        2,
      ),
    );
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));

    process.exitCode = 1;
  }
}
