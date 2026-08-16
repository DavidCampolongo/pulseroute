import { existsSync } from "node:fs";
import process, { loadEnvFile } from "node:process";
import { fileURLToPath, URL } from "node:url";

import { parseFakeReceiverConfig } from "./config.js";
import { FAKE_RECEIVER_HOST, startFakeReceiver } from "./server.js";

const rootEnvironmentPath = fileURLToPath(
  new URL("../../../.env", import.meta.url),
);

if (existsSync(rootEnvironmentPath)) {
  loadEnvFile(rootEnvironmentPath);
}

async function run(): Promise<void> {
  const config = parseFakeReceiverConfig(process.env);
  const receiver = await startFakeReceiver({
    secret: config.outboundWebhookSecret,
    host: FAKE_RECEIVER_HOST,
    port: config.port,
    mode: config.mode,
    delayMs: config.delayMs,
    log: (entry) => {
      console.log(JSON.stringify(entry));
    },
  });

  console.log(
    JSON.stringify({
      component: "fake-receiver",
      outcome: "started",
      url: receiver.url,
      mode: config.mode,
      delayMs: config.delayMs,
    }),
  );

  let shutdownPromise: Promise<void> | undefined;

  function shutdown(signal: NodeJS.Signals): void {
    if (!shutdownPromise) {
      shutdownPromise = receiver.close().then(() => {
        console.log(
          JSON.stringify({
            component: "fake-receiver",
            outcome: "stopped",
            signal,
          }),
        );
      });
    }

    void shutdownPromise.catch((error: unknown) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
  }

  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

try {
  await run();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
