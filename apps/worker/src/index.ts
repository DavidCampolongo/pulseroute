import { config as loadEnvironmentFile } from "dotenv";

import { parseWorkerConfig } from "./config.js";
import { createWorkerLogger } from "./logger.js";

loadEnvironmentFile({
  path: "../../.env",
  quiet: true,
});

const config = parseWorkerConfig(process.env);
const logger = createWorkerLogger(config);

logger.info("Worker configuration loaded");
