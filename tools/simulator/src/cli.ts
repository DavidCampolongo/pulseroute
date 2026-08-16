import { existsSync } from "node:fs";
import process, { loadEnvFile } from "node:process";
import { fileURLToPath, URL } from "node:url";

import { parseSimulatorConfig } from "./config.js";
import { runSimulator, type SimulatorRequestResult } from "./simulator.js";

const rootEnvironmentPath = fileURLToPath(
  new URL("../../../.env", import.meta.url),
);

if (existsSync(rootEnvironmentPath)) {
  loadEnvFile(rootEnvironmentPath);
}

function writeRequestResult(result: SimulatorRequestResult): void {
  console.log(
    JSON.stringify({
      component: "pulseroute-simulator",
      requestNumber: result.requestNumber,
      kind: result.kind,
      sourceSequence: result.sourceSequence,
      eventId: result.eventId,
      externalId: result.externalId,
      statusCode: result.statusCode,
      outcome: result.outcome,
      requestId: result.requestId,
      serviceRequestId: result.serviceRequestId,
      errorCode: result.errorCode,
    }),
  );
}

async function run(): Promise<void> {
  const config = parseSimulatorConfig(process.argv.slice(2), process.env);
  const summary = await runSimulator({
    ...config,
    onResult: writeRequestResult,
  });

  console.log(
    JSON.stringify({
      component: "pulseroute-simulator",
      outcome: summary.errors === 0 ? "completed" : "failed",
      runId: summary.runId,
      endpoint: summary.endpoint,
      requestedCount: summary.requestedCount,
      requestedDuplicates: summary.requestedDuplicates,
      attempted: summary.attempted,
      accepted: summary.accepted,
      duplicate: summary.duplicate,
      errors: summary.errors,
    }),
  );

  if (summary.errors > 0) {
    process.exitCode = 1;
  }
}

try {
  await run();
} catch (error) {
  console.error(error instanceof Error ? error.message : "Simulator failed");
  console.error(
    "Usage: pulseroute-simulator --base-url <http(s)://host> --count <1-100> --duplicates <0-100>",
  );
  process.exitCode = 1;
}
