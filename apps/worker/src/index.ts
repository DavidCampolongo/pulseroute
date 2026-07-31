import { config as loadEnvironmentFile } from "dotenv";

import { parseWorkerConfig } from "./config.js";

loadEnvironmentFile({
  path: "../../.env",
  quiet: true,
});

const config = parseWorkerConfig(process.env);

console.log("PulseRoute worker configuration loaded", {
  nodeEnv: config.nodeEnv,
  logLevel: config.logLevel,
});
