import { randomUUID } from "node:crypto";
import type { ConfirmChannel, RecoveringChannelModel } from "amqplib";
import { afterAll, beforeAll, expect, inject, it } from "vitest";
import type { Redis } from "ioredis";
import {
  connectAmqp,
  createConfirmChannelProvider,
  createJobPublisher,
} from "../../src/lib/amqp.js";
import { createJobsRepository, type JobsRepository } from "../../src/lib/jobs-repository.js";
import { createRedisClient } from "../../src/lib/redis.js";
import { QUEUES, ROUTING_KEYS } from "../../src/lib/topology.js";
import { createJobConsumer, type JobConsumer } from "../../src/worker/consumer.js";
import { countRejections } from "../../src/worker/retry.js";
import { MAX_ATTEMPTS, RETRY_TTL_MS, waitFor } from "./helpers.js";

// No worker containers here: the real consumer runs in-process with a handler
// that always fails, against the real broker topology.
let amqpConnection: RecoveringChannelModel;
let channel: ConfirmChannel;
let redis: Redis;
let jobsRepository: JobsRepository;
let jobConsumer: JobConsumer;
let handlerCalls = 0;

beforeAll(async () => {
  amqpConnection = await connectAmqp({ url: inject("amqpUrl") });
  channel = await amqpConnection.createConfirmChannel();
  redis = createRedisClient({ url: inject("redisUrl") });
  jobsRepository = createJobsRepository({ redis, jobTtlSeconds: 3600 });

  async function alwaysFail(): Promise<void> {
    handlerCalls++;
    throw new Error("forced failure");
  }
  jobConsumer = createJobConsumer({
    channel,
    jobHandlers: { image: alwaysFail, videoPlan: alwaysFail, videoRendition: alwaysFail },
    jobsRepository,
    maxAttempts: MAX_ATTEMPTS,
  });
  await jobConsumer.start();
});

afterAll(async () => {
  await jobConsumer?.stop();
  await channel?.close();
  await amqpConnection?.close();
  await redis?.quit();
});

it("retries a failing job through the delay queue, then parks it", async () => {
  const jobId = randomUUID();
  const now = new Date().toISOString();
  const message = {
    type: "image" as const,
    jobId,
    sourceKey: `${jobId}/source.jpg`,
    mime: "image/jpeg" as const,
  };
  await jobsRepository.createJob({
    ...message,
    status: "queued",
    bytes: 1,
    createdAt: now,
    updatedAt: now,
    progress: 0,
  });

  const channelProvider = createConfirmChannelProvider(amqpConnection);
  const started = Date.now();
  await createJobPublisher({ channelProvider }).publish(ROUTING_KEYS.IMAGE_TRANSFORM, message);
  await channelProvider.close();

  const record = await waitFor("the job to be marked failed", async () => {
    const current = await jobsRepository.getJob(jobId);
    return current?.status === "failed" ? current : null;
  });

  // Every attempt ran, and each retry waited out the delay queue's TTL.
  expect(handlerCalls).toBe(MAX_ATTEMPTS);
  expect(Date.now() - started).toBeGreaterThanOrEqual((MAX_ATTEMPTS - 1) * RETRY_TTL_MS);
  expect(record.error).toBe("max-attempts: forced failure");

  const parked = await waitFor("the parked copy", async () => channel.get(QUEUES.PARKED));
  channel.ack(parked);
  expect(parked.properties.messageId).toBe(jobId);
  expect(parked.properties.headers?.["park-error"]).toBe("max-attempts: forced failure");
  // Two rejections by q.work sent it round the delay queue; the third attempt parked.
  expect(countRejections(parked.properties.headers, QUEUES.WORK)).toBe(MAX_ATTEMPTS - 1);
});
