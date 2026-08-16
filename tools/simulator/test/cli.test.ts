import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const repositoryRoot = fileURLToPath(new URL("../../../", import.meta.url));
const cliPath = fileURLToPath(new URL("../src/cli.ts", import.meta.url));
const environmentSecret =
  "cli-subprocess-environment-secret-at-least-32-characters";

type CliResult = {
  exitCode: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
};

function runCli(arguments_: readonly string[]): Promise<CliResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ["--import", "tsx", cliPath, ...arguments_],
      {
        cwd: repositoryRoot,
        env: {
          ...process.env,
          WEBHOOK_SECRET: environmentSecret,
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    let stdout = "";
    let stderr = "";

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk: string) => {
      stderr += chunk;
    });

    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("Simulator CLI subprocess timed out"));
    }, 10_000);

    child.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });

    child.once("close", (exitCode, signal) => {
      clearTimeout(timeout);
      resolve({
        exitCode,
        signal,
        stdout,
        stderr,
      });
    });
  });
}

describe("simulator CLI", () => {
  it("redacts raw unknown arguments and exits unsuccessfully", async () => {
    const sentinelArgument = "never-print-this-cli-argument-sentinel-secret";
    const result = await runCli([
      "--base-url",
      "https://pulseroute.example",
      "--count",
      "1",
      "--duplicates",
      "0",
      "--webhook-secret",
      sentinelArgument,
    ]);

    expect(result).toMatchObject({
      exitCode: 1,
      signal: null,
      stdout: "",
    });
    expect(result.stderr).toContain("Unknown simulator option");
    expect(result.stderr).toContain("Unexpected positional simulator argument");
    expect(result.stderr).toContain("Usage: pulseroute-simulator");
    expect(result.stderr).not.toContain(sentinelArgument);
    expect(result.stderr).not.toContain(environmentSecret);
  });

  it("prints a safe failing summary and exits one on a network error", async () => {
    const result = await runCli([
      "--base-url",
      "http://127.0.0.1:1",
      "--count",
      "1",
      "--duplicates",
      "0",
    ]);

    expect(result.exitCode).toBe(1);
    expect(result.signal).toBeNull();

    const outputEntries = result.stdout
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as Record<string, unknown>);

    expect(outputEntries).toHaveLength(2);
    expect(outputEntries[0]).toMatchObject({
      component: "pulseroute-simulator",
      requestNumber: 1,
      outcome: "error",
      errorCode: "NETWORK_ERROR",
    });
    expect(outputEntries[1]).toMatchObject({
      component: "pulseroute-simulator",
      outcome: "failed",
      attempted: 1,
      accepted: 0,
      duplicate: 0,
      errors: 1,
    });
    expect(result.stdout).not.toContain(environmentSecret);
    expect(result.stderr).not.toContain(environmentSecret);
    expect(result.stdout).not.toContain("x-pulseroute-signature");
  });
});
