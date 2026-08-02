import { config as loadEnvironmentFile } from "dotenv";

import { parseWorkerConfig } from "./config.js";
import { createWorkerLogger } from "./logger.js";
import { createWorkerRuntime, runWorkerProcess } from "./runtime.js";

loadEnvironmentFile({
  path: "../../.env",
  quiet: true,
});

const config = parseWorkerConfig(process.env);
const logger = createWorkerLogger(config);

try {
  const runtime = await createWorkerRuntime(config, logger);

  await runWorkerProcess(runtime);
} catch (error) {
  logger.fatal(
    {
      err: error,
    },
    "Worker process terminated unsuccessfully",
  );

  process.exitCode = 1;
}
