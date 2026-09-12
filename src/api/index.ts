// Must be imported before anything the OpenTelemetry instrumentations patch.
import "../lib/tracing.js";

import { pathToFileURL } from "node:url";
import { config } from "../config/index.js";
import { connectAmqp, createConfirmChannelProvider, createJobPublisher } from "../lib/amqp.js";
import { createJobsRepository } from "../lib/jobs-repository.js";
import { logger } from "../lib/logger.js";
import { createObjectRepository } from "../lib/object-repository.js";
import { createRedisClient, createRedisSubscriber } from "../lib/redis.js";
import { createS3Client } from "../lib/s3.js";
import { stopTracing } from "../lib/tracing.js";
import { createApp } from "./app.js";
import { createUploadService } from "./upload-service.js";
import { createSseRegistry } from "./sse.js";

const HEALTH_CHECK_TIMEOUT_MS = 2_000;

/**
 * A health probe must answer, not hang. The AMQP recovery layer in particular
 * parks callers until it reconnects, which would otherwise pin the request open
 * for the whole outage.
 */
function withTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_resolve, reject) =>
      setTimeout(
        () => reject(new Error(`${label} check timed out`)),
        HEALTH_CHECK_TIMEOUT_MS,
      ).unref(),
    ),
  ]);
}

/**
 * The API's composition root: the only place that builds real clients.
 */
export async function main(): Promise<void> {
  const redis = createRedisClient({ url: config.REDIS_URL });
  const jobsRepository = createJobsRepository({ redis, jobTtlSeconds: config.JOB_TTL_SECONDS });

  const objectRepository = createObjectRepository(
    createS3Client({
      endpoint: config.S3_ENDPOINT,
      region: config.S3_REGION,
      accessKeyId: config.S3_ACCESS_KEY,
      secretAccessKey: config.S3_SECRET_KEY,
    }),
  );

  // Asserts the topology on connect, so the API can publish into a broker that
  // has never seen this app before.
  const amqpConnection = await connectAmqp({ url: config.AMQP_URL });
  const channelProvider = createConfirmChannelProvider(amqpConnection);
  const jobPublisher = createJobPublisher({ channelProvider });

  const uploadService = createUploadService({
    objectRepository,
    jobsRepository,
    jobPublisher,
    bucket: config.BUCKET_UPLOADS,
  });

  // SSE streams are hijacked responses that never go idle, so they must be
  // ended explicitly before `app.close()` can resolve.
  const sseRegistry = createSseRegistry();

  const healthChecks = {
    broker: () =>
      withTimeout(
        amqpConnection.createChannel().then((channel) => channel.close()),
        "broker",
      ),
    storage: () =>
      withTimeout(
        objectRepository.bucketExists(config.BUCKET_UPLOADS).then((exists: boolean) => {
          if (!exists) throw new Error(`Bucket ${config.BUCKET_UPLOADS} is missing`);
        }),
        "storage",
      ),
    redis: () => withTimeout(redis.ping(), "redis"),
  };

  const app = await createApp({
    uploadService,
    jobsRepository,
    // A dedicated connection per SSE stream: subscriber-mode clients cannot run
    // ordinary commands, so this must never be the jobs repository's client.
    createSubscriber: () => createRedisSubscriber({ url: config.REDIS_URL }),
    sseRegistry,
    healthChecks,
    maxUploadBytes: config.MAX_UPLOAD_BYTES,
  });

  let shuttingDown = false;
  const shutdown = async (signal: NodeJS.Signals) => {
    if (shuttingDown) {
      logger.warn({ signal }, "Second signal received; exiting immediately");
      process.exit(1);
    }
    shuttingDown = true;
    logger.info({ signal }, "Shutting down API");

    try {
      // Order matters. Long-lived SSE streams go first -- Fastify only reaps
      // *idle* keep-alive sockets, so `app.close()` would otherwise wait on
      // them until the orchestrator SIGKILLs us and the trace flush is lost.
      // Ordinary in-flight requests (including uploads) still drain normally.
      sseRegistry.closeAll();
      await app.close();
      await channelProvider.close();
      await amqpConnection.close();
      await redis.quit();
      await stopTracing();
      process.exit(0);
    } catch (error) {
      logger.error({ err: error }, "Graceful shutdown failed");
      process.exit(1);
    }
  };

  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));

  await app.listen({ port: config.API_PORT, host: "0.0.0.0" });
}

// Import-time side effects are confined to entrypoints, and only when this file
// is the one that was executed -- importing it from a test must stay inert.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error: unknown) => {
    logger.error({ err: error }, "API failed to start");
    process.exit(1);
  });
}
