import { Readable } from "node:stream";
import { describe, expect, it } from "vitest";
import type { JobMessage, JobRecord } from "../domain/job.js";
import type { JobPublisher } from "../lib/amqp.js";
import { ROUTING_KEYS } from "../lib/topology.js";
import { createUploadService, type UploadPart } from "./upload-service.js";

/**
 * A silent logger. The service takes `request.log` in production; here we only
 * need the four methods it calls.
 */
const log = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
} as unknown as Parameters<ReturnType<typeof createUploadService>["acceptUpload"]>[1];

/** A multipart part backed by a plain Readable -- what `UploadPart` exists to allow. */
function fakePart(content: string, mimetype: string, truncated = false): UploadPart {
  const stream = Readable.from([Buffer.from(content)]) as UploadPart["file"];
  stream.truncated = truncated;
  return { file: stream, mimetype };
}

function createFakeObjectRepository() {
  const stored = new Map<string, number>();
  const deleted: string[] = [];
  let putError: Error | null = null;
  let deleteError: Error | null = null;

  return {
    stored,
    deleted,
    failPutWith: (error: Error) => (putError = error),
    failDeleteWith: (error: Error) => (deleteError = error),
    repository: {
      async putStream({ key, body }: { key: string; body: Readable }) {
        let bytes = 0;
        for await (const chunk of body) bytes += (chunk as Buffer).length;
        if (putError) throw putError;
        stored.set(key, bytes);
        return { bytes };
      },
      async deleteObject({ key }: { key: string }) {
        if (deleteError) throw deleteError;
        deleted.push(key);
        stored.delete(key);
      },
    },
  };
}

function createFakeJobsRepository() {
  const created: JobRecord[] = [];
  const failed: { jobId: string; error: string }[] = [];
  let createError: Error | null = null;

  return {
    created,
    failed,
    failCreateWith: (error: Error) => (createError = error),
    repository: {
      async createJob(record: JobRecord) {
        if (createError) throw createError;
        created.push(record);
        return record;
      },
      async markFailed(jobId: string, error: string) {
        failed.push({ jobId, error });
        return null;
      },
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

  return { published, publisher, failWith: (error: Error) => (failure = error) };
}

function build() {
  const objects = createFakeObjectRepository();
  const jobs = createFakeJobsRepository();
  const publisher = createFakeJobPublisher();

  const service = createUploadService({
    objectRepository: objects.repository,
    jobsRepository: jobs.repository,
    jobPublisher: publisher.publisher,
    bucket: "media-uploads",
  });

  return { service, objects, jobs, publisher };
}

describe("createUploadService", () => {
  it("stores the bytes, records the job and publishes a confirmed message", async () => {
    const { service, objects, jobs, publisher } = build();

    const outcome = await service.acceptUpload(fakePart("jpeg-bytes", "image/jpeg"), log);

    expect(outcome).toEqual({ accepted: true, jobId: expect.any(String) });
    expect(jobs.created).toHaveLength(1);
    expect(jobs.created[0]).toMatchObject({ status: "queued", type: "image", bytes: 10 });
    expect(publisher.published[0].routingKey).toBe(ROUTING_KEYS.IMAGE_TRANSFORM);
    // Key is deterministic from the jobId, with the extension from the MIME type.
    expect(objects.stored.has(`${jobs.created[0].jobId}/source.jpg`)).toBe(true);
  });

  it("routes video to the plan queue", async () => {
    const { service, publisher } = build();

    await service.acceptUpload(fakePart("mp4-bytes", "video/quicktime"), log);

    expect(publisher.published[0].routingKey).toBe(ROUTING_KEYS.VIDEO_PLAN);
    expect(publisher.published[0].message.sourceKey).toMatch(/\/source\.mov$/);
  });

  it("rejects a missing part as no_file", async () => {
    const { service } = build();

    expect(await service.acceptUpload(undefined, log)).toMatchObject({
      accepted: false,
      reason: "no_file",
    });
  });

  it("rejects a type outside the allowlist without touching the bucket", async () => {
    const { service, objects, publisher } = build();

    const outcome = await service.acceptUpload(fakePart("MZ", "application/octet-stream"), log);

    expect(outcome).toMatchObject({ accepted: false, reason: "unsupported_mime" });
    expect(objects.stored.size).toBe(0);
    expect(publisher.published).toHaveLength(0);
  });

  it("deletes the partial object when the part was truncated", async () => {
    const { service, objects, jobs, publisher } = build();

    const outcome = await service.acceptUpload(fakePart("too-big", "image/png", true), log);

    expect(outcome).toMatchObject({ accepted: false, reason: "too_large" });
    expect(objects.deleted).toHaveLength(1);
    expect(objects.stored.size).toBe(0);
    // Nothing may be queued or recorded for a job whose source is incomplete.
    expect(jobs.created).toHaveLength(0);
    expect(publisher.published).toHaveLength(0);
  });

  it("rejects with storage_error and publishes nothing when the bucket write fails", async () => {
    const { service, objects, jobs, publisher } = build();
    objects.failPutWith(new Error("minio down"));

    const outcome = await service.acceptUpload(fakePart("bytes", "image/png"), log);

    expect(outcome).toMatchObject({ accepted: false, reason: "storage_error" });
    expect(jobs.created).toHaveLength(0);
    expect(publisher.published).toHaveLength(0);
  });

  it("removes the orphaned object when the job record cannot be written", async () => {
    const { service, objects, jobs, publisher } = build();
    jobs.failCreateWith(new Error("redis down"));

    const outcome = await service.acceptUpload(fakePart("bytes", "image/png"), log);

    expect(outcome).toMatchObject({ accepted: false, reason: "job_record_error" });
    // Bytes nothing references and no message will ever point at must not linger.
    expect(objects.deleted).toHaveLength(1);
    expect(objects.stored.size).toBe(0);
    expect(publisher.published).toHaveLength(0);
  });

  it("still rejects when the orphan cleanup itself fails", async () => {
    const { service, objects, jobs } = build();
    jobs.failCreateWith(new Error("redis down"));
    objects.failDeleteWith(new Error("minio down too"));

    // A failed compensation is logged, never rethrown -- the client still gets
    // the 503 that describes what actually went wrong.
    expect(await service.acceptUpload(fakePart("bytes", "image/png"), log)).toMatchObject({
      accepted: false,
      reason: "job_record_error",
    });
  });

  it("marks the job failed when the broker does not confirm", async () => {
    const { service, jobs, publisher } = build();
    publisher.failWith(new Error("no confirm"));

    const outcome = await service.acceptUpload(fakePart("bytes", "image/webp"), log);

    expect(outcome).toMatchObject({ accepted: false, reason: "publish_failed" });
    // A 202 was never sent, so the record must not be left claiming `queued`.
    expect(jobs.failed).toEqual([
      { jobId: jobs.created[0].jobId, error: "Broker did not confirm the job" },
    ]);
  });
});
