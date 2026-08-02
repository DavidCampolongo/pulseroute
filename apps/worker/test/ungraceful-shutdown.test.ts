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

import {
  createIncomingWorker,
  createRoutingJobId,
} from "../src/incoming-worker.js";
import { createWorkerLogger } from "../src/logger.js";
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
  throw new Error("DATABASE_URL is required for ungraceful-shutdown tests");
}

if (!baseRedisUrl) {
  throw new Error("REDIS_URL is required for ungraceful-shutdown tests");
}

const LOCK_DURATION_MS = 500;
const STALLED_INTERVAL_MS = 100;

const testRedisUrl = new URL(baseRedisUrl);

testRedisUrl.pathname = "/10";

const redisUrl = testRedisUrl.toString();

const database = createDatabaseClient(databaseUrl);

const lockDatabase = createDatabaseClient(databaseUrl);

const queues = createPulseRouteQueues(redisUrl);

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

const organizationId = randomUUID();
const requiredSkillId = randomUUID();
const serviceRequestId = randomUUID();

const childScriptPath = fileURLToPath(
  new URL("./fixtures/ungraceful-worker-child.mjs", import.meta.url),
);

const repositoryRoot = fileURLToPath(new URL("../../../", import.meta.url));

type HeldDatabaseLock = {
  ready: Promise<void>;
  release: () => void;
  done: Promise<void>;
};

let childProcess: ChildProcess | undefined;

let recoveryWorker: ReturnType<typeof createIncomingWorker> | undefined;

let heldDatabaseLock: HeldDatabaseLock | undefined;

let childOutput = "";
let childErrorOutput = "";

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

      TEST_WORKER_LOCK_DURATION_MS: String(LOCK_DURATION_MS),

      TEST_WORKER_STALLED_INTERVAL_MS: String(STALLED_INTERVAL_MS),
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
  timeoutMs = 10_000,
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

  throw new Error(`Timed out waiting for BullMQ job ${jobId}`);
}

async function countQueueJobs<DataType>(
  queue: Queue<DataType>,
): Promise<number> {
  const counts = await queue.getJobCounts(
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
  );

  return Object.values(counts).reduce((total, count) => total + count, 0);
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
      name: "Phase 6 Ungraceful Shutdown Test Organization",
    },
  });

  await database.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Phase 6 Ungraceful Shutdown Test Skill",
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: `ungraceful-${randomUUID()}`,
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
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

  if (recoveryWorker) {
    await recoveryWorker.close();
  }

  await obliterateQueues(queues);

  await closePulseRouteQueues(queues);

  await clearDatabaseFixture();

  await database.$disconnect();
  await lockDatabase.$disconnect();
});

describe("worker process ungraceful termination", () => {
  it("recovers the abandoned active job through BullMQ stalled-job handling", async () => {
    childProcess = startWorkerChild();

    await waitForChildOutput(childProcess, "Worker runtime started");

    heldDatabaseLock = holdServiceRequestTableLock();

    await heldDatabaseLock.ready;

    const sourceJobId = `ungraceful-${randomUUID()}`;

    const correlationId = `request-${randomUUID()}`;

    const sourceJobData: ServiceRequestIngestedJobData = {
      outboxEventId: randomUUID(),
      organizationId,
      serviceRequestId,
      correlationId,
    };

    const sourceJob = await queues.incomingEvents.add(
      JOB_NAMES.serviceRequestIngested,
      sourceJobData,
      {
        jobId: sourceJobId,

        attempts: 1,

        removeOnComplete: false,
        removeOnFail: false,
      },
    );

    await waitForJobState(sourceJob, "active");

    expect(childProcess.kill("SIGKILL")).toBe(true);

    const killedProcess = await waitForChildExit(childProcess);

    expect(killedProcess).toEqual({
      code: null,
      signal: "SIGKILL",
    });

    expect(childOutput).toContain("Worker runtime started");

    expect(childOutput).not.toContain("Worker shutdown started");

    expect(childOutput).not.toContain("Worker shutdown completed");

    /*
     * The killed process cannot finish its blocked query. Release the table
     * lock so the replacement worker can complete the recovered execution.
     */
    heldDatabaseLock.release();

    await heldDatabaseLock.done;

    heldDatabaseLock = undefined;

    recoveryWorker = createIncomingWorker({
      database,

      routingQueue: queues.routing,

      deadLetterQueue: queues.deadLetter,

      logger,

      redisUrl,

      concurrency: 1,

      lockDuration: LOCK_DURATION_MS,

      stalledInterval: STALLED_INTERVAL_MS,

      maxStalledCount: 1,
    });

    recoveryWorker.on("error", (error) => {
      logger.error(
        {
          err: error,
        },
        "Ungraceful recovery worker error",
      );
    });

    await recoveryWorker.waitUntilReady();

    /*
     * The original Redis job is not replaced. After its abandoned lock
     * expires, the replacement worker moves it through stalled recovery and
     * processes that same job again.
     */
    await waitForJobState(sourceJob, "completed", 10_000);

    const savedSourceJob = await queues.incomingEvents.getJob(sourceJobId);

    expect(savedSourceJob).not.toBeUndefined();

    if (!savedSourceJob) {
      throw new Error("Expected the recovered source job to remain available");
    }

    expect(savedSourceJob.id).toBe(sourceJobId);

    expect(savedSourceJob.data).toEqual(sourceJobData);

    expect(await savedSourceJob.getState()).toBe("completed");

    expect(savedSourceJob.stalledCounter).toBe(1);

    expect(savedSourceJob.attemptsStarted).toBe(2);

    const routingJobId = createRoutingJobId(serviceRequestId);

    const routingJob = await waitForJob<RouteServiceRequestJobData>(
      queues.routing,
      routingJobId,
    );

    expect(routingJob.data).toEqual({
      organizationId,
      serviceRequestId,
      correlationId,
    });

    expect(await routingJob.getState()).toBe("waiting");

    expect(await countQueueJobs(queues.routing)).toBe(1);

    expect(await countQueueJobs(queues.deadLetter)).toBe(0);

    const unchangedRequest = await database.serviceRequest.findUniqueOrThrow({
      where: {
        id: serviceRequestId,
      },
    });

    expect(unchangedRequest.status).toBe("PENDING");

    expect(childErrorOutput).not.toContain("Ungraceful-shutdown child failed");
  }, 15_000);
});
