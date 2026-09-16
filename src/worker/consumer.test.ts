import { randomUUID } from "node:crypto";
import type { ConfirmChannel, ConsumeMessage, MessagePropertyHeaders, Options } from "amqplib";
import { describe, expect, it } from "vitest";
import { CorruptMediaError } from "../domain/media.js";
import { mediaMetrics } from "../lib/metrics.js";
import { EXCHANGES, QUEUES, ROUTING_KEYS } from "../lib/topology.js";
import { createJobConsumer, type ConsumerChannel, type JobHandlers } from "./consumer.js";

// Compile-time proof that a real amqplib ConfirmChannel satisfies the fake's contract.
const _realChannelFits = (channel: ConfirmChannel): ConsumerChannel => channel;

type OnMessage = (message: ConsumeMessage | null) => unknown;

function createFakeChannel() {
  const consumers = new Map<string, OnMessage>();
  const acked: ConsumeMessage[] = [];
  const nacked: { message: ConsumeMessage; requeue: boolean | undefined }[] = [];
  const published: { exchange: string; routingKey: string; options: Options.Publish }[] = [];
  const cancelled: string[] = [];
  const prefetchCalls: { count: number; global: boolean | undefined }[] = [];
  let confirmError: Error | null = null;

  const channel: ConsumerChannel = {
    async prefetch(count, global) {
      prefetchCalls.push({ count, global });
    },
    async consume(queue, onMessage, options) {
      expect(options).toEqual({ noAck: false });
      consumers.set(queue, onMessage);
      return { consumerTag: `tag:${queue}` };
    },
    async cancel(consumerTag) {
      cancelled.push(consumerTag);
    },
    ack(message) {
      acked.push(message);
    },
    nack(message, _allUpTo, requeue) {
      nacked.push({ message, requeue });
    },
    publish(exchange, routingKey, _content, options, callback) {
      published.push({ exchange, routingKey, options });
      queueMicrotask(() => callback(confirmError));
      return true;
    },
  };

  return {
    channel,
    consumers,
    acked,
    nacked,
    published,
    cancelled,
    prefetchCalls,
    failConfirmsWith: (error: Error) => (confirmError = error),
    /** Resolves once the consumer has settled the delivery. */
    deliver: async (message: ConsumeMessage) => {
      await consumers.get(QUEUES.WORK)!(message);
    },
  };
}

function createFakeHandlers() {
  const calls: { handler: keyof JobHandlers; message: unknown }[] = [];
  let failure: unknown = null;
  let gate: Promise<void> | null = null;

  async function record(handler: keyof JobHandlers, message: unknown) {
    calls.push({ handler, message });
    if (gate) await gate;
    if (failure) throw failure;
  }

  const handlers: JobHandlers = {
    image: (message) => record("image", message),
    videoPlan: (message) => record("videoPlan", message),
    videoRendition: (message) => record("videoRendition", message),
  };

  return {
    calls,
    handlers,
    failWith: (error: unknown) => (failure = error),
    holdUntil: (promise: Promise<void>) => (gate = promise),
  };
}

let nextDeliveryTag = 1;

function buildMessage(
  body: unknown,
  {
    routingKey = ROUTING_KEYS.IMAGE_TRANSFORM,
    headers,
    messageId,
  }: {
    routingKey?: string;
    headers?: MessagePropertyHeaders;
    messageId?: string;
  } = {},
): ConsumeMessage {
  return {
    content: Buffer.from(typeof body === "string" ? body : JSON.stringify(body)),
    fields: {
      deliveryTag: nextDeliveryTag++,
      redelivered: false,
      exchange: EXCHANGES.JOBS,
      routingKey,
      consumerTag: "tag",
    },
    properties: {
      contentType: "application/json",
      contentEncoding: undefined,
      headers,
      deliveryMode: 2,
      priority: undefined,
      correlationId: undefined,
      replyTo: undefined,
      expiration: undefined,
      messageId,
      timestamp: undefined,
      type: undefined,
      userId: undefined,
      appId: undefined,
      clusterId: undefined,
    },
  };
}

function imageJob() {
  const jobId = randomUUID();
  return {
    jobId,
    body: { type: "image", jobId, sourceKey: `${jobId}/source.png`, mime: "image/png" },
  };
}

async function build({ maxAttempts = 3 } = {}) {
  const fake = createFakeChannel();
  const handlers = createFakeHandlers();
  const failed: { jobId: string; error: string }[] = [];
  let markFailedError: Error | null = null;

  const consumer = createJobConsumer({
    channel: fake.channel,
    jobHandlers: handlers.handlers,
    jobsRepository: {
      async markFailed(jobId, error) {
        if (markFailedError) throw markFailedError;
        failed.push({ jobId, error });
        return null;
      },
    },
    maxAttempts,
  });
  await consumer.start();

  return {
    consumer,
    fake,
    handlers,
    failed,
    failMarkFailedWith: (error: Error) => (markFailedError = error),
  };
}

/**
 * The registry is a process-wide singleton, so every assertion is a delta
 * against the value this test started from -- never an absolute count.
 */
interface ReadableMetric {
  get(): Promise<{ values: { labels: Partial<Record<string, string | number>>; value: number }[] }>;
}

async function metricValue(
  metric: ReadableMetric,
  labels: Record<string, string> = {},
): Promise<number> {
  const { values } = await metric.get();
  const match = values.find((sample) =>
    Object.entries(labels).every(([key, value]) => sample.labels[key] === value),
  );
  return match?.value ?? 0;
}

describe("createJobConsumer", () => {
  it("runs one consumer on the work queue with a plain, non-global prefetch", async () => {
    const { fake } = await build();

    // Global QoS is denied on RabbitMQ 4.x; one consumer is what makes 1 mean 1.
    expect(fake.prefetchCalls).toEqual([{ count: 1, global: undefined }]);
    expect([...fake.consumers.keys()]).toEqual([QUEUES.WORK]);
  });

  it("parses with the routing key's schema, runs its handler, then acks", async () => {
    const { fake, handlers } = await build();
    const { body } = imageJob();
    const message = buildMessage(body);

    await fake.deliver(message);

    expect(handlers.calls).toEqual([{ handler: "image", message: body }]);
    expect(fake.acked).toEqual([message]);
    expect(fake.nacked).toHaveLength(0);
  });

  it("routes a rendition message to the rendition handler", async () => {
    const { fake, handlers } = await build();
    const { body } = imageJob();
    const rendition = { name: "720p", width: 1280, height: 720, bandwidth: 2_800_000 };

    await fake.deliver(
      buildMessage(
        { ...body, type: "video", mime: "video/mp4", rendition },
        { routingKey: ROUTING_KEYS.VIDEO_RENDITION },
      ),
    );

    expect(handlers.calls.map((call) => call.handler)).toEqual(["videoRendition"]);
    expect(fake.acked).toHaveLength(1);
  });

  it("never acks before the handler has finished", async () => {
    const { fake, handlers } = await build();
    let release!: () => void;
    handlers.holdUntil(new Promise((resolve) => (release = resolve)));

    const delivery = fake.deliver(buildMessage(imageJob().body));
    await Promise.resolve();
    expect(fake.acked).toHaveLength(0);

    release();
    await delivery;
    expect(fake.acked).toHaveLength(1);
  });

  it("nacks a transient failure into the retry path without touching Redis", async () => {
    const { fake, handlers, failed } = await build();
    handlers.failWith(new Error("minio timeout"));
    const message = buildMessage(imageJob().body);

    await fake.deliver(message);

    expect(fake.nacked).toEqual([{ message, requeue: false }]);
    expect(fake.acked).toHaveLength(0);
    expect(fake.published).toHaveLength(0);
    expect(failed).toHaveLength(0);
  });

  it("parks a job on its last attempt: confirmed publish, ack, Redis failed", async () => {
    const { fake, handlers, failed } = await build({ maxAttempts: 3 });
    handlers.failWith(new Error("minio timeout"));
    const { jobId, body } = imageJob();
    const message = buildMessage(body, {
      messageId: jobId,
      headers: {
        "x-death": [
          { queue: QUEUES.RETRY, reason: "expired", count: 2 },
          { queue: QUEUES.WORK, reason: "rejected", count: 2 },
        ] as MessagePropertyHeaders["x-death"],
      },
    });

    await fake.deliver(message);

    expect(fake.published).toEqual([
      {
        exchange: EXCHANGES.PARKED,
        // Preserved, so a parked message can be shovelled back to where it came from.
        routingKey: ROUTING_KEYS.IMAGE_TRANSFORM,
        options: expect.objectContaining({ persistent: true, messageId: jobId }),
      },
    ]);
    expect(fake.acked).toEqual([message]);
    expect(fake.nacked).toHaveLength(0);
    expect(failed).toEqual([{ jobId, error: "max-attempts: minio timeout" }]);
  });

  it("parks a non-retryable error on the first attempt", async () => {
    const { fake, handlers } = await build();
    handlers.failWith(new CorruptMediaError("bad moov atom"));

    await fake.deliver(buildMessage(imageJob().body));

    expect(fake.published[0].exchange).toBe(EXCHANGES.PARKED);
    expect(fake.acked).toHaveLength(1);
  });

  it.each([
    { name: "a body that is not JSON", body: "{not json" },
    { name: "a body that breaks the schema", body: { type: "image", jobId: "nope" } },
    {
      name: "a video body under the image routing key",
      body: { ...imageJob().body, type: "video" },
    },
  ])("parks $name without calling a handler", async ({ body }) => {
    const { fake, handlers, failed } = await build();
    const jobId = randomUUID();

    await fake.deliver(buildMessage(body, { messageId: jobId }));

    expect(handlers.calls).toHaveLength(0);
    expect(fake.published[0].exchange).toBe(EXCHANGES.PARKED);
    expect(fake.acked).toHaveLength(1);
    // messageId is the fallback when the body cannot say which job it was.
    expect(failed[0].jobId).toBe(jobId);
  });

  it("parks a job that crashed its worker on every attempt, without running it again", async () => {
    const { fake, handlers, failed } = await build({ maxAttempts: 3 });
    const { jobId, body } = imageJob();

    await fake.deliver(
      buildMessage(body, { messageId: jobId, headers: { "x-delivery-count": 3 } }),
    );

    expect(handlers.calls).toHaveLength(0);
    expect(fake.published[0].exchange).toBe(EXCHANGES.PARKED);
    expect(fake.acked).toHaveLength(1);
    expect(failed[0].error).toMatch(/^max-attempts: Worker crashed/);
    // A replay must start fresh, not arrive already at the crash limit.
    expect(fake.published[0].options.headers).not.toHaveProperty("x-delivery-count");
    expect(fake.published[0].options.headers).toHaveProperty("park-error");
  });

  it("counts thrown failures and crashes together before running", async () => {
    const { fake, handlers } = await build({ maxAttempts: 3 });
    const headers: MessagePropertyHeaders = {
      "x-death": [
        { queue: QUEUES.WORK, reason: "rejected", count: 1 },
      ] as MessagePropertyHeaders["x-death"],
      "x-delivery-count": 2,
    };

    await fake.deliver(buildMessage(imageJob().body, { headers }));

    expect(handlers.calls).toHaveLength(0);
    expect(fake.published[0].exchange).toBe(EXCHANGES.PARKED);
  });

  it("parks an unknown routing key without retrying", async () => {
    const { fake, handlers } = await build();

    await fake.deliver(buildMessage(imageJob().body, { routingKey: "job.audio.transcode" }));

    expect(handlers.calls).toHaveLength(0);
    expect(fake.published[0].exchange).toBe(EXCHANGES.PARKED);
    expect(fake.nacked).toHaveLength(0);
  });

  it("still runs a job whose worker crashed fewer times than the limit", async () => {
    const { fake, handlers } = await build({ maxAttempts: 3 });

    await fake.deliver(buildMessage(imageJob().body, { headers: { "x-delivery-count": 2 } }));

    expect(handlers.calls).toHaveLength(1);
    expect(fake.acked).toHaveLength(1);
  });

  it("still parks when the Redis write fails", async () => {
    const { fake, handlers, failMarkFailedWith } = await build();
    handlers.failWith(new CorruptMediaError("bad"));
    failMarkFailedWith(new Error("redis down"));

    await fake.deliver(buildMessage(imageJob().body, { messageId: randomUUID() }));

    expect(fake.published).toHaveLength(1);
    expect(fake.acked).toHaveLength(1);
  });

  it("does not ack when the broker refuses the parked copy", async () => {
    const { fake, handlers } = await build();
    handlers.failWith(new CorruptMediaError("bad"));
    fake.failConfirmsWith(new Error("nack from broker"));

    // Must resolve, not reject: amqplib would surface a rejection as unhandled.
    await fake.deliver(buildMessage(imageJob().body));

    expect(fake.acked).toHaveLength(0);
  });

  it("counts a completed stage, times it, and leaves the busy gauge at rest", async () => {
    const { fake } = await build();
    const before = await metricValue(mediaMetrics.jobsTotal, {
      type: "image",
      status: "completed",
    });
    const timedBefore = await metricValue(mediaMetrics.transcodeDuration, {
      type: "image",
      rendition: "image",
    });

    await fake.deliver(buildMessage(imageJob().body));

    expect(await metricValue(mediaMetrics.jobsTotal, { type: "image", status: "completed" })).toBe(
      before + 1,
    );
    // The `rendition` label comes back from the handler, so a timed sample here
    // proves the parse-then-label path stayed connected.
    expect(
      await metricValue(mediaMetrics.transcodeDuration, { type: "image", rendition: "image" }),
    ).toBeGreaterThan(timedBefore);
    expect(await metricValue(mediaMetrics.workerBusy)).toBe(0);
  });

  it("counts a retry and a park under the stage's routing key", async () => {
    const { fake, handlers } = await build();
    const stage = { routing_key: ROUTING_KEYS.IMAGE_TRANSFORM };
    const retriesBefore = await metricValue(mediaMetrics.retriesTotal, stage);
    const parksBefore = await metricValue(mediaMetrics.parkedTotal, {
      ...stage,
      reason: "non-retryable",
    });

    handlers.failWith(new Error("minio timeout"));
    await fake.deliver(buildMessage(imageJob().body));
    handlers.failWith(new CorruptMediaError("bad moov atom"));
    await fake.deliver(buildMessage(imageJob().body));

    expect(await metricValue(mediaMetrics.retriesTotal, stage)).toBe(retriesBefore + 1);
    expect(
      await metricValue(mediaMetrics.parkedTotal, {
        ...stage,
        reason: "non-retryable",
      }),
    ).toBe(parksBefore + 1);
    expect(await metricValue(mediaMetrics.workerBusy)).toBe(0);
  });

  it("stop cancels the consumer, then waits for the in-flight job to ack", async () => {
    const { consumer, fake, handlers } = await build();
    let release!: () => void;
    handlers.holdUntil(new Promise((resolve) => (release = resolve)));
    const delivery = fake.deliver(buildMessage(imageJob().body));

    let stopped = false;
    const stopping = consumer.stop().then(() => (stopped = true));
    await new Promise((resolve) => setImmediate(resolve));

    expect(fake.cancelled).toEqual([`tag:${QUEUES.WORK}`]);
    expect(stopped).toBe(false);

    release();
    await Promise.all([delivery, stopping]);
    expect(fake.acked).toHaveLength(1);
  });
});
