import type { RedisOptions } from "bullmq";

const REDIS_CONNECT_TIMEOUT_MS = 5_000;
const PRODUCER_MAX_RETRIES_PER_REQUEST = 1;

export function createProducerRedisOptions(redisUrl: string): RedisOptions {
  return {
    url: redisUrl,
    connectTimeout: REDIS_CONNECT_TIMEOUT_MS,

    enableOfflineQueue: true,
    maxRetriesPerRequest: PRODUCER_MAX_RETRIES_PER_REQUEST,
  };
}

export function createWorkerRedisOptions(redisUrl: string): RedisOptions {
  return {
    url: redisUrl,
    connectTimeout: REDIS_CONNECT_TIMEOUT_MS,

    enableOfflineQueue: true,
    maxRetriesPerRequest: null,
  };
}
