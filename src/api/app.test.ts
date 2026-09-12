import { Readable } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import type { JobMessage, JobRecord } from "../domain/job.js";
import type { JobPublisher } from "../lib/amqp.js";
import { createJobsRepository, type JobsRepositoryRedis } from "../lib/jobs-repository.js";
import type { ObjectRepository } from "../lib/object-repository.js";
import { ROUTING_KEYS } from "../lib/topology.js";
import { createApp, type AppDeps } from "./app.js";
import type { SseSubscriber } from "./sse.js";
import { createUploadService } from "./upload-service.js";

const BOUNDARY = "----mediapipelinetest";

/** Hand-rolls a multipart body so `app.inject()` exercises the real parser. */
function multipartBody(content: Buffer, filename: string, contentType: string) {
  return {
    headers: { "content-type": `multipart/form-data; boundary=${BOUNDARY}` },
    payload: Buffer.concat([
      Buffer.from(
        `--${BOUNDARY}\r\n` +
          `Content-Disposition: form-data; name="file"; filename="${filename}"\r\n` +
          `Content-Type: ${contentType}\r\n\r\n`,
      ),
      content,
      Buffer.from(`\r\n--${BOUNDARY}--\r\n`),
    ]),
  };
}

/**
 * An object repository that really drains the stream (so `truncated` and the
 * byte count behave as they do against MinIO) but keeps the bytes in a Map.
 */
function createFakeObjectRepository() {
  const objects = new Map<string, Buffer>();
  const deleted: string[] = [];
  let failPut: Error | null = null;

  const repository: Pick<ObjectRepository, "putStream" | "deleteObject"> = {
    async putStream({ bucket, key, body }) {
      const chunks: Buffer[] = [];
      for await (const chunk of body as Readable) chunks.push(chunk as Buffer);
      if (failPut) throw failPut;
      const buffer = Buffer.concat(chunks);
      objects.set(`${bucket}/${key}`, buffer);
      return { bytes: buffer.length };
    },
    async deleteObject({ bucket, key }) {
      deleted.push(`${bucket}/${key}`);
      objects.delete(`${bucket}/${key}`);
    },
  };

  return {
    repository,
    objects,
    deleted,
    failPutWith(error: Error) {
      failPut = error;
    },
  };
}

function createFakeRedis(): JobsRepositoryRedis {
  const hashes = new Map<string, Record<string, string>>();
  const zset = new Map<string, number>();

  return {
    async hset(key, value) {
      hashes.set(key, { ...(hashes.get(key) ?? {}), ...value });
      return 1;
    },
    async hgetall(key) {
      return { ...(hashes.get(key) ?? {}) };
    },
    async expire() {
      return 1;
    },
    async zadd(key, score, member) {
      zset.set(`${key}:${member}`, score);
      return 1;
    },
    async zrevrange(key, start, stop) {
      return [...zset.entries()]
        .filter(([composite]) => composite.startsWith(`${key}:`))
        .sort(([, a], [, b]) => b - a)
        .map(([composite]) => composite.slice(key.length + 1))
        .slice(start, stop + 1);
    },
    async zremrangebyrank() {
      return 0;
    },
    async publish() {
      return 1;
    },
  };
}

function createFakeJobPublisher() {
  const published: { routingKey: string; message: JobMessage }[] = [];
  let failure: Error | null = null;

  const publisher: JobPublisher = {
    async publish(routingKey, message) {
      if (failure) throw failure;
      published.push({ routingKey, message });
    },
  };

  return {
    publisher,
    published,
    failWith(error: Error) {
      failure = error;
    },
  };
}

function createFakeSubscriber(): SseSubscriber {
  return {
    async subscribe() {
      return 1;
    },
    async unsubscribe() {
      return 1;
    },
    on() {
      return this;
    },
    async quit() {
      return "OK";
    },
  };
}

interface Harness {
  app: Awaited<ReturnType<typeof createApp>>;
  objects: ReturnType<typeof createFakeObjectRepository>;
  publisher: ReturnType<typeof createFakeJobPublisher>;
  jobsRepository: ReturnType<typeof createJobsRepository>;
}

const harnesses: Harness[] = [];

async function buildHarness(overrides: Partial<AppDeps> = {}): Promise<Harness> {
  const objects = createFakeObjectRepository();
  const publisher = createFakeJobPublisher();
  const jobsRepository = createJobsRepository({ redis: createFakeRedis(), jobTtlSeconds: 60 });

  const app = await createApp({
    uploadService: createUploadService({
      objectRepository: objects.repository,
      jobsRepository,
      jobPublisher: publisher.publisher,
      bucket: "media-uploads",
    }),
    jobsRepository,
    createSubscriber: createFakeSubscriber,
    healthChecks: { broker: async () => "ok", storage: async () => "ok", redis: async () => "ok" },
    maxUploadBytes: 1_000_000,
    ...overrides,
  });

  const harness = { app, objects, publisher, jobsRepository };
  harnesses.push(harness);
  return harness;
}

afterEach(async () => {
  await Promise.all(harnesses.splice(0).map((harness) => harness.app.close()));
});

describe("POST /uploads", () => {
  it("streams the file to the bucket, records the job and publishes a confirmed message", async () => {
    const { app, objects, publisher, jobsRepository } = await buildHarness();

    const response = await app.inject({
      method: "POST",
      url: "/uploads",
      ...multipartBody(Buffer.from("fake-jpeg-bytes"), "holiday.jpg", "image/jpeg"),
    });

    expect(response.statusCode).toBe(202);
    const body = response.json<{ jobId: string; statusUrl: string; eventsUrl: string }>();
    expect(body.statusUrl).toBe(`/jobs/${body.jobId}`);
    expect(body.eventsUrl).toBe(`/jobs/${body.jobId}/events`);

    // The key is deterministic from the jobId and carries no bucket prefix.
    const key = `media-uploads/${body.jobId}/source.jpg`;
    expect(objects.objects.get(key)?.toString()).toBe("fake-jpeg-bytes");

    expect(publisher.published).toEqual([
      {
        routingKey: ROUTING_KEYS.IMAGE_TRANSFORM,
        message: {
          jobId: body.jobId,
          type: "image",
          sourceKey: `${body.jobId}/source.jpg`,
          mime: "image/jpeg",
        },
      },
    ]);

    const record = await jobsRepository.getJob(body.jobId);
    expect(record).toMatchObject({ status: "queued", type: "image", bytes: 15, progress: 0 });
  });

  it("routes video to the plan queue, not the image queue", async () => {
    const { app, publisher } = await buildHarness();

    await app.inject({
      method: "POST",
      url: "/uploads",
      ...multipartBody(Buffer.from("fake-mp4"), "clip.mov", "video/quicktime"),
    });

    expect(publisher.published[0].routingKey).toBe(ROUTING_KEYS.VIDEO_PLAN);
    // The extension comes from the MIME type, not the client-supplied filename.
    expect(publisher.published[0].message.sourceKey).toMatch(/\/source\.mov$/);
  });

  it("rejects a type outside the allowlist with 415 and never touches the bucket", async () => {
    const { app, objects, publisher } = await buildHarness();

    const response = await app.inject({
      method: "POST",
      url: "/uploads",
      ...multipartBody(Buffer.from("MZ"), "payload.exe", "application/octet-stream"),
    });

    expect(response.statusCode).toBe(415);
    expect(objects.objects.size).toBe(0);
    expect(publisher.published).toHaveLength(0);
  });

  it("returns 400 when the request carries no file part", async () => {
    const { app } = await buildHarness();

    const response = await app.inject({
      method: "POST",
      url: "/uploads",
      headers: { "content-type": `multipart/form-data; boundary=${BOUNDARY}` },
      payload: Buffer.from(`--${BOUNDARY}--\r\n`),
    });

    expect(response.statusCode).toBe(400);
  });

  it("deletes the partial object and returns 413 when the size limit is hit", async () => {
    const { app, objects, publisher } = await buildHarness({ maxUploadBytes: 16 });

    const response = await app.inject({
      method: "POST",
      url: "/uploads",
      ...multipartBody(Buffer.alloc(512, 0x41), "big.png", "image/png"),
    });

    expect(response.statusCode).toBe(413);
    // The truncated bytes were already in the bucket, so they must be removed.
    expect(objects.deleted).toHaveLength(1);
    expect(objects.objects.size).toBe(0);
    // And nothing may be queued for a job whose source is incomplete.
    expect(publisher.published).toHaveLength(0);
  });

  it("returns 503 without publishing when the object store fails", async () => {
    const { app, objects, publisher } = await buildHarness();
    objects.failPutWith(new Error("minio down"));

    const response = await app.inject({
      method: "POST",
      url: "/uploads",
      ...multipartBody(Buffer.from("bytes"), "a.png", "image/png"),
    });

    expect(response.statusCode).toBe(503);
    expect(publisher.published).toHaveLength(0);
  });

  it("marks the job failed and returns 503 when the broker does not confirm", async () => {
    const { app, publisher, jobsRepository } = await buildHarness();
    publisher.failWith(new Error("no confirm"));

    const response = await app.inject({
      method: "POST",
      url: "/uploads",
      ...multipartBody(Buffer.from("bytes"), "a.webp", "image/webp"),
    });

    expect(response.statusCode).toBe(503);

    // A 202 was never sent, so the record must not be left claiming `queued`.
    const [record] = await jobsRepository.listRecentJobs(10);
    expect(record).toMatchObject({ status: "failed" });
  });
});

describe("GET /jobs", () => {
  const seed = (jobId: string): JobRecord => ({
    jobId,
    status: "queued",
    type: "image",
    sourceKey: `${jobId}/source.png`,
    mime: "image/png",
    bytes: 10,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    progress: 0,
  });

  it("returns a job record", async () => {
    const { app, jobsRepository } = await buildHarness();
    const record = seed("55555555-5555-4555-8555-555555555555");
    await jobsRepository.createJob(record);

    const response = await app.inject({ method: "GET", url: `/jobs/${record.jobId}` });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual(record);
  });

  it("404s an unknown job", async () => {
    const { app } = await buildHarness();

    const response = await app.inject({
      method: "GET",
      url: "/jobs/66666666-6666-4666-8666-666666666666",
    });

    expect(response.statusCode).toBe(404);
  });

  it("400s a job id that is not a UUID", async () => {
    const { app } = await buildHarness();

    expect((await app.inject({ method: "GET", url: "/jobs/not-a-uuid" })).statusCode).toBe(400);
  });

  it("400s an events subscription for an id that is not a UUID", async () => {
    const { app } = await buildHarness();

    const response = await app.inject({ method: "GET", url: "/jobs/not-a-uuid/events" });

    // An arbitrary id must never reach Redis SUBSCRIBE.
    expect(response.statusCode).toBe(400);
  });

  it("404s an events subscription for an unknown job", async () => {
    const { app } = await buildHarness();

    const response = await app.inject({
      method: "GET",
      url: "/jobs/88888888-8888-4888-8888-888888888888/events",
    });

    expect(response.statusCode).toBe(404);
  });

  it("lists recent jobs for the UI table", async () => {
    const { app, jobsRepository } = await buildHarness();
    await jobsRepository.createJob(seed("77777777-7777-4777-8777-777777777777"));

    const response = await app.inject({ method: "GET", url: "/jobs?limit=5" });

    expect(response.statusCode).toBe(200);
    expect(response.json<{ jobs: JobRecord[] }>().jobs).toHaveLength(1);
  });
});

describe("observability endpoints", () => {
  it("reports healthy when every dependency answers", async () => {
    const { app } = await buildHarness();

    const response = await app.inject({ method: "GET", url: "/health" });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      status: "ok",
      checks: { broker: "ok", storage: "ok", redis: "ok" },
    });
  });

  it("reports 503 and names the failing dependency", async () => {
    const { app } = await buildHarness({
      healthChecks: {
        broker: async () => "ok",
        storage: () => Promise.reject(new Error("bucket missing")),
        redis: async () => "ok",
      },
    });

    const response = await app.inject({ method: "GET", url: "/health" });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({
      status: "degraded",
      checks: { storage: "unavailable", broker: "ok" },
    });
  });

  it("exposes the Prometheus registry through Fastify", async () => {
    const { app } = await buildHarness();

    const response = await app.inject({ method: "GET", url: "/metrics" });

    expect(response.statusCode).toBe(200);
    expect(response.body).toContain("media_uploads_received_total");
  });
});
