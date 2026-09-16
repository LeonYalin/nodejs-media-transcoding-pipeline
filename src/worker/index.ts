// Must be imported before anything the OpenTelemetry instrumentations patch.
import "../lib/tracing.js";

import { pathToFileURL } from "node:url";
import { config } from "../config/index.js";
import { connectAmqp, createConfirmChannelProvider, createJobPublisher } from "../lib/amqp.js";
import { createJobsRepository } from "../lib/jobs-repository.js";
import { logger } from "../lib/logger.js";
import { startMetricsServer } from "../lib/metrics-server.js";
import { createObjectRepository } from "../lib/object-repository.js";
import { createRedisClient } from "../lib/redis.js";
import { createS3Client } from "../lib/s3.js";
import { stopTracing } from "../lib/tracing.js";
import { createJobConsumer, type JobHandlers } from "./consumer.js";
import { createImageHandler } from "./handlers/image.js";
import { createVideoPlanHandler } from "./handlers/video-plan.js";
import { createVideoRenditionHandler } from "./handlers/video-rendition.js";

/**
 * The worker's composition root: the only place that builds real clients.
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

  const metricsServer = startMetricsServer({ port: config.WORKER_METRICS_PORT });

  const amqpConnection = await connectAmqp({ url: config.AMQP_URL });
  // The plan stage's fan-out publishes through the same provider the API uses,
  // which re-opens its channel after a reconnect.
  const channelProvider = createConfirmChannelProvider(amqpConnection);
  const jobPublisher = createJobPublisher({ channelProvider });

  const buckets = { uploadsBucket: config.BUCKET_UPLOADS, outputsBucket: config.BUCKET_OUTPUTS };
  // Aborted only by a second signal, to kill a running ffmpeg child.
  const encodeAbort = new AbortController();
  const jobHandlers: JobHandlers = {
    image: createImageHandler({ objectRepository, jobsRepository, ...buckets }),
    videoPlan: createVideoPlanHandler({
      objectRepository,
      jobsRepository,
      jobPublisher,
      abortSignal: encodeAbort.signal,
      ...buckets,
    }),
    videoRendition: createVideoRenditionHandler({
      objectRepository,
      jobsRepository,
      segmentSeconds: config.HLS_SEGMENT_SECONDS,
      abortSignal: encodeAbort.signal,
      ...buckets,
    }),
  };

  // A confirm channel, because parking publishes to media.parked and must see
  // the broker's confirm before it acks the original.
  const consumerChannel = await amqpConnection.createConfirmChannel();
  const jobConsumer = createJobConsumer({
    channel: consumerChannel,
    jobHandlers,
    jobsRepository,
    maxAttempts: config.MAX_ATTEMPTS,
    prefetch: config.AMQP_PREFETCH,
  });

  let shuttingDown = false;

  // Consumers die with their channel and are not re-registered in-process (see
  // consumer.ts). Exiting hands recovery to Docker's restart policy, and the
  // broker redelivers whatever this worker had not acked.
  consumerChannel.on("error", (error) => logger.error({ err: error }, "Consumer channel error"));
  consumerChannel.on("close", () => {
    if (shuttingDown) return;
    logger.fatal("Consumer channel closed; exiting so the container restarts");
    process.exit(1);
  });

  const shutdown = async (signal: NodeJS.Signals) => {
    if (shuttingDown) {
      logger.warn({ signal }, "Second signal received; killing ffmpeg and exiting");
      encodeAbort.abort();
      process.exit(1);
    }
    shuttingDown = true;
    logger.info({ signal }, "Shutting down worker");

    try {
      // Stop new deliveries first, then let the in-flight job finish and ack --
      // closing the channel any earlier would hand that job back to the broker.
      await jobConsumer.stop();
      await consumerChannel.close();
      await channelProvider.close();
      await amqpConnection.close();
      await redis.quit();
      await new Promise<void>((resolve) => metricsServer.close(() => resolve()));
      await stopTracing();
      process.exit(0);
    } catch (error) {
      logger.error({ err: error }, "Graceful shutdown failed");
      process.exit(1);
    }
  };

  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));

  await jobConsumer.start();
}

// Import-time side effects are confined to entrypoints, and only when this file
// is the one that was executed -- importing it from a test must stay inert.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error: unknown) => {
    logger.error({ err: error }, "Worker failed to start");
    process.exit(1);
  });
}
