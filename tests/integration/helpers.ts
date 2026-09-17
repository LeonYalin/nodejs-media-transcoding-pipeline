import type { AddressInfo } from "node:net";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import { inject } from "vitest";
import { createApp } from "../../src/api/app.js";
import { createUploadService } from "../../src/api/upload-service.js";
import {
  connectAmqp,
  createConfirmChannelProvider,
  createJobPublisher,
} from "../../src/lib/amqp.js";
import { createJobsRepository } from "../../src/lib/jobs-repository.js";
import { createObjectRepository } from "../../src/lib/object-repository.js";
import { createRedisClient, createRedisSubscriber } from "../../src/lib/redis.js";
import { createS3Client } from "../../src/lib/s3.js";
import { WORKER_IMAGE } from "./globalSetup.js";

// Short, so the retry path finishes in seconds. Workers and the test process
// must agree: queue arguments are immutable once q.retry exists.
export const RETRY_TTL_MS = 1000;
export const MAX_ATTEMPTS = 3;
export const BUCKET_UPLOADS = "media-uploads";
export const BUCKET_OUTPUTS = "media-outputs";

/** Polls `check` until it returns a truthy value, or the timeout elapses. */
export async function waitFor<T>(
  description: string,
  check: () => Promise<T | undefined | null | false>,
  { timeoutMs = 60_000, intervalMs = 250 } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await check();
    if (result) return result;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for: ${description}`);
}

/** Starts one real worker container on the suite's network. */
export async function startWorker(): Promise<StartedTestContainer> {
  return new GenericContainer(WORKER_IMAGE)
    .withNetworkMode(inject("networkName"))
    .withEnvironment({
      // production = JSON logs on stdout; `test` would silence the log the wait matches.
      NODE_ENV: "production",
      AMQP_URL: "amqp://media:media@rabbitmq:5672",
      S3_ENDPOINT: "http://minio:9000",
      REDIS_URL: "redis://redis:6379",
      RETRY_TTL_MS: String(RETRY_TTL_MS),
      MAX_ATTEMPTS: String(MAX_ATTEMPTS),
    })
    .withWaitStrategy(Wait.forLogMessage(/Worker consuming/))
    .start();
}

/**
 * The API as the entrypoint wires it, listening on a free port. Built here
 * rather than by spawning `src/api/index.ts`, so the test can also read Redis
 * and MinIO through the same repositories.
 */
export async function startApi() {
  const redis = createRedisClient({ url: inject("redisUrl") });
  const jobsRepository = createJobsRepository({ redis, jobTtlSeconds: 3600 });
  const objectRepository = createObjectRepository(
    createS3Client({
      endpoint: inject("s3Endpoint"),
      region: "us-east-1",
      accessKeyId: "minioadmin",
      secretAccessKey: "minioadmin",
    }),
  );
  const amqpConnection = await connectAmqp({ url: inject("amqpUrl") });
  const channelProvider = createConfirmChannelProvider(amqpConnection);
  const jobPublisher = createJobPublisher({ channelProvider });

  const app = await createApp({
    uploadService: createUploadService({
      objectRepository,
      jobsRepository,
      jobPublisher,
      bucket: BUCKET_UPLOADS,
    }),
    jobsRepository,
    createSubscriber: () => createRedisSubscriber({ url: inject("redisUrl") }),
    healthChecks: {},
    maxUploadBytes: 100 * 1024 * 1024,
  });
  await app.listen({ port: 0, host: "127.0.0.1" });
  const { port } = app.server.address() as AddressInfo;

  /** POSTs a file the way the browser form does; returns the new job id. */
  async function upload(
    bytes: Buffer<ArrayBuffer>,
    mime: string,
    fileName: string,
  ): Promise<string> {
    const form = new FormData();
    form.append("file", new Blob([bytes], { type: mime }), fileName);
    const response = await fetch(`http://127.0.0.1:${port}/uploads`, {
      method: "POST",
      body: form,
    });
    if (response.status !== 202) {
      throw new Error(`Upload returned ${response.status}: ${await response.text()}`);
    }
    const body = (await response.json()) as { jobId: string };
    return body.jobId;
  }

  async function readOutput(key: string): Promise<Buffer> {
    const stream = await objectRepository.getStream({ bucket: BUCKET_OUTPUTS, key });
    const chunks: Buffer[] = [];
    for await (const chunk of stream) chunks.push(chunk as Buffer);
    return Buffer.concat(chunks);
  }

  async function close(): Promise<void> {
    await app.close();
    await channelProvider.close();
    await amqpConnection.close();
    await redis.quit();
  }

  return { upload, readOutput, jobsRepository, close };
}
