import { Redis, type RedisOptions } from "ioredis";
import { logger } from "./logger.js";

export interface RedisSettings {
  url: string;
  options?: RedisOptions;
}

/**
 * Builds an ioredis client. Only entrypoints call this -- everything else
 * receives the client (or a narrow structural slice of it) as a dependency.
 *
 * `maxRetriesPerRequest` is deliberately finite rather than ioredis's `null`
 * ("retry forever"): the API answers HTTP requests, so a Redis outage must
 * surface as a fast 503 instead of a socket that hangs until the client times
 * out. The worker inherits the same behaviour, where a thrown command turns
 * into a normal retryable job failure.
 */
export function createRedisClient({ url, options }: RedisSettings): Redis {
  const client = new Redis(url, {
    maxRetriesPerRequest: 2,
    enableReadyCheck: true,
    ...options,
  });

  // Mandatory, not decorative: ioredis emits 'error' on the client for every
  // failed reconnect attempt, and an EventEmitter 'error' with no listener is an
  // uncaught exception. Without this a Redis restart kills the API process --
  // and each SSE stream owns a connection, so every viewer is another such
  // trigger. Reconnection is ioredis's job; ours is only to not crash.
  client.on("error", (error: unknown) => logger.warn({ err: error }, "Redis client error"));

  return client;
}

/**
 * A connection dedicated to `SUBSCRIBE`. Redis puts a subscribed connection into
 * subscriber mode, where ordinary commands are rejected -- so SSE streams must
 * never share the client that `jobs-repository` issues commands on.
 */
export function createRedisSubscriber(settings: RedisSettings): Redis {
  return createRedisClient(settings);
}
