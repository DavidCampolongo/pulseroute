import assert from "node:assert/strict";
import console from "node:console";
import { createHmac, randomUUID } from "node:crypto";
import process from "node:process";

const { fetch, AbortSignal } = globalThis;

const secret = process.env.WEBHOOK_SECRET;
if (!secret || secret.length < 32) {
  throw new Error("Set WEBHOOK_SECRET in .env to at least 32 characters.");
}

// Keep the demo local; never send a development secret to an arbitrary URL.
const url = `http://127.0.0.1:${process.env.API_PORT ?? "3000"}/webhooks/service-requests`;
const externalId = `demo-${randomUUID()}`;
const body = JSON.stringify({
  organizationId: "00000001-0000-4000-8000-000000000001",
  eventId: `event-${randomUUID()}`,
  type: "service_request.created",
  data: {
    externalId,
    requiredSkillId: "00000003-0000-4000-8000-000000000004",
    priority: "NORMAL",
    region: "NORTH",
  },
});

async function send(validSignature = true) {
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = createHmac("sha256", secret)
    .update(`${timestamp}.${body}`)
    .digest("hex");
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-pulseroute-timestamp": timestamp,
      "x-pulseroute-signature": validSignature ? signature : "0".repeat(64),
    },
    body,
    signal: AbortSignal.timeout(5000),
  });
  return { code: response.status, body: await response.json() };
}

const first = await send();
assert.equal(first.code, 202, JSON.stringify(first));
assert.equal(first.body.status, "accepted", JSON.stringify(first));
assert.equal(typeof first.body.serviceRequestId, "string");
console.log("First delivery: HTTP 202, accepted");

const duplicate = await send();
assert.equal(duplicate.code, 200, JSON.stringify(duplicate));
assert.equal(duplicate.body.status, "duplicate", JSON.stringify(duplicate));
assert.equal(duplicate.body.serviceRequestId, first.body.serviceRequestId);
console.log("Repeated delivery: HTTP 200, duplicate");

const invalid = await send(false);
assert.equal(invalid.code, 401, JSON.stringify(invalid));
console.log("Invalid signature: HTTP 401, rejected");
console.log(`Request external ID: ${externalId}`);
console.log(`Service request ID: ${first.body.serviceRequestId}`);
console.log(
  "Check the worker and fake-receiver logs for the assignment delivery.",
);
