import { describe, expect, it } from "vitest";

import { runSimulator, type SimulatorFetch } from "../src/simulator.js";

const secret = "response-limit-test-secret-at-least-32-characters";

describe("simulator response limits", () => {
  it("cancels a response stream as soon as it exceeds 64 KiB", async () => {
    let cancelled = false;
    const oversizedBody = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(32 * 1024));
        controller.enqueue(new Uint8Array(32 * 1024));
        controller.enqueue(new Uint8Array(1));
      },
      cancel() {
        cancelled = true;
      },
    });
    const fetch: SimulatorFetch = async () =>
      new Response(oversizedBody, {
        status: 202,
        headers: {
          "content-type": "application/json",
        },
      });

    const summary = await runSimulator({
      endpoint: "https://pulseroute.example/webhooks/service-requests",
      count: 1,
      duplicates: 0,
      webhookSecret: secret,
      timeoutMs: 1_000,
      fetch,
      createRunId: () => "11111111-2222-4333-8444-555555555555",
    });

    expect(cancelled).toBe(true);
    expect(summary).toMatchObject({
      attempted: 1,
      accepted: 0,
      duplicate: 0,
      errors: 1,
    });
    expect(summary.results[0]).toMatchObject({
      outcome: "error",
      errorCode: "UNEXPECTED_RESPONSE",
    });
  });
});
