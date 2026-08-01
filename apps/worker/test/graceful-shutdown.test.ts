import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { type Job, type Queue } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createRoutingJobId } from "../src/incoming-worker.js";
import {
  closePulseRouteQueues,
  createPulseRouteQueues,
  type PulseRouteQueues,
  waitForPulseRouteQueues,
} from "../src/queues.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const baseRedisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for graceful-shutdown tests");
}

if (!baseRedisUrl) {
  throw new Error("REDIS_URL is required for graceful-shutdown tests");
}

const testRedisUrl = new URL(baseRedisUrl);

testRedisUrl.pathname = "/11";

const redisUrl = testRedisUrl.toString();

const database = createDatabaseClient(databaseUrl);

const lockDatabase = createDatabaseClient(databaseUrl);

const queues = createPulseRouteQueues(redisUrl);

const organizationId = randomUUID();
const requiredSkillId = randomUUID();

const activeServiceRequestId = randomUUID();

const waitingServiceRequestId = randomUUID();

const childScriptPath = fileURLToPath(
  new URL("./fixtures/graceful-worker-child.mjs", import.meta.url),
);

const repositoryRoot = fileURLToPath(new URL("../../../", import.meta.url));

type HeldDatabaseLock = {
  ready: Promise<void>;
  release: () => void;
  done: Promise<void>;
};

let childProcess: ChildProcess | undefined;

let childOutput = "";
let childErrorOutput = "";

let heldDatabaseLock: HeldDatabaseLock | undefined;

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function obliterateQueues(activeQueues: PulseRouteQueues): Promise<void> {
  await Promise.all([
    activeQueues.incomingEvents
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    activeQueues.routing
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    activeQueues.notifications
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    activeQueues.webhookDelivery
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    activeQueues.deadLetter
      .obliterate({
        force: true,
      })
      .catch(() => undefined),
  ]);
}

async function clearDatabaseFixture(): Promise<void> {
  await database.serviceRequest.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });
}

function startWorkerChild(): ChildProcess {
  childOutput = "";
  childErrorOutput = "";

  const child = spawn(process.execPath, [childScriptPath], {
    cwd: repositoryRoot,

    env: {
      ...process.env,
      NODE_ENV: "test",
      LOG_LEVEL: "info",
      DATABASE_URL: databaseUrl,
      REDIS_URL: redisUrl,
    },

    stdio: ["ignore", "pipe", "pipe"],
  });

  if (!child.stdout || !child.stderr) {
    throw new Error("Expected child stdout and stderr pipes");
  }

  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");

  child.stdout.on("data", (chunk: string) => {
    childOutput += chunk;
  });

  child.stderr.on("data", (chunk: string) => {
    childErrorOutput += chunk;
  });

  return child;
}

async function waitForChildOutput(
  child: ChildProcess,
  expectedText: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (childOutput.includes(expectedText)) {
      return;
    }

    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(
        [
          `Child exited before logging: ${expectedText}`,
          `stdout: ${childOutput}`,
          `stderr: ${childErrorOutput}`,
        ].join("\n"),
      );
    }

    await sleep(25);
  }

  throw new Error(
    [
      `Timed out waiting for child output: ${expectedText}`,
      `stdout: ${childOutput}`,
      `stderr: ${childErrorOutput}`,
    ].join("\n"),
  );
}

async function waitForChildExit(
  child: ChildProcess,
  timeoutMs = 5_000,
): Promise<{
  code: number | null;
  signal: NodeJS.Signals | null;
}> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return {
      code: child.exitCode,
      signal: child.signalCode,
    };
  }

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();

      reject(
        new Error(
          [
            "Timed out waiting for worker child to exit",
            `stdout: ${childOutput}`,
            `stderr: ${childErrorOutput}`,
          ].join("\n"),
        ),
      );
    }, timeoutMs);

    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();

      resolve({
        code,
        signal,
      });
    };

    const onError = (error: Error) => {
      cleanup();

      reject(error);
    };

    const cleanup = () => {
      clearTimeout(timeout);

      child.removeListener("exit", onExit);

      child.removeListener("error", onError);
    };

    child.once("exit", onExit);
    child.once("error", onError);
  });
}

async function waitForJobState<DataType>(
  job: Job<DataType>,
  expectedState: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const state = await job.getState();

    if (state === expectedState) {
      return;
    }

    await sleep(25);
  }

  const finalState = await job.getState();

  throw new Error(
    `Timed out waiting for job ${job.id} to reach ${expectedState}; final state was ${finalState}`,
  );
}

async function waitForJob<DataType>(
  queue: Queue<DataType>,
  jobId: string,
  timeoutMs = 5_000,
): Promise<Job<DataType>> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const job = await queue.getJob(jobId);

    if (job) {
      return job;
    }

    await sleep(25);
  }

  throw new Error(`Timed out waiting for job ${jobId}`);
}

function holdServiceRequestTableLock(): HeldDatabaseLock {
  let resolveReady: (() => void) | undefined;

  let rejectReady: ((error: unknown) => void) | undefined;

  let resolveRelease: (() => void) | undefined;

  const ready = new Promise<void>((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });

  const releasePromise = new Promise<void>((resolve) => {
    resolveRelease = resolve;
  });

  const transactionPromise = lockDatabase
    .$transaction(
      async (transaction) => {
        await transaction.$executeRawUnsafe(
          'LOCK TABLE "service_requests" IN ACCESS EXCLUSIVE MODE',
        );

        resolveReady?.();

        await releasePromise;
      },
      {
        maxWait: 5_000,
        timeout: 20_000,
      },
    )
    .catch((error) => {
      rejectReady?.(error);

      throw error;
    });

  return {
    ready,

    release: () => {
      resolveRelease?.();
    },

    done: transactionPromise,
  };
}

beforeAll(async () => {
  await waitForPulseRouteQueues(queues);

  await obliterateQueues(queues);

  await clearDatabaseFixture();

  await database.organization.create({
    data: {
      id: organizationId,
      name: "Phase 6 Graceful Shutdown Test Organization",
    },
  });

  await database.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Phase 6 Graceful Shutdown Test Skill",
    },
  });

  await database.serviceRequest.createMany({
    data: [
      {
        id: activeServiceRequestId,
        organizationId,
        externalId: `graceful-active-${randomUUID()}`,
        requiredSkillId,
        priority: "HIGH",
        region: "WEST",
      },

      {
        id: waitingServiceRequestId,
        organizationId,
        externalId: `graceful-waiting-${randomUUID()}`,
        requiredSkillId,
        priority: "NORMAL",
        region: "WEST",
      },
    ],
  });
});

afterAll(async () => {
  if (heldDatabaseLock) {
    heldDatabaseLock.release();

    await heldDatabaseLock.done.catch(() => undefined);

    heldDatabaseLock = undefined;
  }

  if (
    childProcess &&
    childProcess.exitCode === null &&
    childProcess.signalCode === null
  ) {
    childProcess.kill("SIGKILL");

    await waitForChildExit(childProcess).catch(() => undefined);
  }

  await obliterateQueues(queues);

  await closePulseRouteQueues(queues);

  await clearDatabaseFixture();

  await database.$disconnect();
  await lockDatabase.$disconnect();
});

describe("worker process graceful shutdown", () => {
  it("stops intake, drains the active job, closes resources, and exits zero", async () => {
    childProcess = startWorkerChild();

    await waitForChildOutput(childProcess, "Worker runtime started");

    heldDatabaseLock = holdServiceRequestTableLock();

    await heldDatabaseLock.ready;

    const activeJobData: ServiceRequestIngestedJobData = {
      outboxEventId: randomUUID(),
      organizationId,
      serviceRequestId: activeServiceRequestId,
      correlationId: `active-${randomUUID()}`,
    };

    const activeJob = await queues.incomingEvents.add(
      JOB_NAMES.serviceRequestIngested,
      activeJobData,
      {
        jobId: `graceful-active-${randomUUID()}`,

        attempts: 1,
        removeOnComplete: false,
        removeOnFail: false,
      },
    );

    await waitForJobState(activeJob, "active");

    expect(childProcess.kill("SIGTERM")).toBe(true);

    await waitForChildOutput(
      childProcess,
      "Worker intake stopped; draining active jobs",
    );

    /*
     * This job is added after worker.close() has begun. It must remain
     * waiting rather than being accepted by the closing child.
     */
    const waitingJob = await queues.incomingEvents.add(
      JOB_NAMES.serviceRequestIngested,
      {
        outboxEventId: randomUUID(),
        organizationId,
        serviceRequestId: waitingServiceRequestId,
        correlationId: `waiting-${randomUUID()}`,
      },
      {
        jobId: `graceful-waiting-${randomUUID()}`,

        attempts: 1,
        removeOnComplete: false,
        removeOnFail: false,
      },
    );

    await sleep(150);

    expect(childProcess.exitCode).toBeNull();
    expect(childProcess.signalCode).toBeNull();

    expect(await activeJob.getState()).toBe("active");

    expect(await waitingJob.getState()).toBe("waiting");

    heldDatabaseLock.release();

    await heldDatabaseLock.done;

    heldDatabaseLock = undefined;

    const exitResult = await waitForChildExit(childProcess, 7_500);

    expect(exitResult).toEqual({
      code: 0,
      signal: null,
    });

    await waitForJobState(activeJob, "completed");

    expect(await waitingJob.getState()).toBe("waiting");

    const activeRoutingJobId = createRoutingJobId(activeServiceRequestId);

    const activeRoutingJob = await waitForJob<RouteServiceRequestJobData>(
      queues.routing,
      activeRoutingJobId,
    );

    expect(activeRoutingJob.data).toEqual({
      organizationId,
      serviceRequestId: activeServiceRequestId,
      correlationId: activeJobData.correlationId,
    });

    expect(await activeRoutingJob.getState()).toBe("waiting");

    const waitingRoutingJob = await queues.routing.getJob(
      createRoutingJobId(waitingServiceRequestId),
    );

    expect(waitingRoutingJob).toBeUndefined();

    const shutdownStartedIndex = childOutput.indexOf("Worker shutdown started");

    const intakeStoppedIndex = childOutput.indexOf(
      "Worker intake stopped; draining active jobs",
    );

    const jobsDrainedIndex = childOutput.indexOf("Worker active jobs drained");

    const queuesClosedIndex = childOutput.indexOf("Worker queues closed");

    const databaseClosedIndex = childOutput.indexOf(
      "Worker database disconnected",
    );

    const shutdownCompletedIndex = childOutput.indexOf(
      "Worker shutdown completed",
    );

    expect(shutdownStartedIndex).toBeGreaterThanOrEqual(0);

    expect(intakeStoppedIndex).toBeGreaterThan(shutdownStartedIndex);

    expect(jobsDrainedIndex).toBeGreaterThan(intakeStoppedIndex);

    expect(queuesClosedIndex).toBeGreaterThan(jobsDrainedIndex);

    expect(databaseClosedIndex).toBeGreaterThan(queuesClosedIndex);

    expect(shutdownCompletedIndex).toBeGreaterThan(databaseClosedIndex);

    expect(childErrorOutput).not.toContain(
      "Worker process terminated unsuccessfully",
    );
  }, 15_000);
});
