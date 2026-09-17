import { describe, expect, it } from "vitest";
import type { JobRecord } from "../domain/job.js";
import {
  RECENT_JOBS_KEY,
  createJobsRepository,
  jobEventsChannel,
  jobKey,
  type JobsRepositoryRedis,
} from "./jobs-repository.js";

/**
 * A small in-memory Redis. Only the commands jobs-repository issues -- injected rather
 * than mocked, per the project's no-module-mocking rule.
 */
function createFakeRedis() {
  const hashes = new Map<string, Record<string, string>>();
  const zset = new Map<string, number>();
  const expiries = new Map<string, number>();
  const published: { channel: string; message: string }[] = [];

  const redis: JobsRepositoryRedis = {
    async hset(key, value) {
      hashes.set(key, { ...(hashes.get(key) ?? {}), ...value });
      return Object.keys(value).length;
    },
    async hgetall(key) {
      return { ...(hashes.get(key) ?? {}) };
    },
    async expire(key, seconds) {
      expiries.set(key, seconds);
      return 1;
    },
    async zadd(key, score, member) {
      zset.set(`${key}:${member}`, score);
      return 1;
    },
    async zrevrange(key, start, stop) {
      const members = [...zset.entries()]
        .filter(([composite]) => composite.startsWith(`${key}:`))
        .sort(([, a], [, b]) => b - a)
        .map(([composite]) => composite.slice(key.length + 1));
      return members.slice(start, stop + 1);
    },
    async zremrangebyrank() {
      return 0;
    },
    async publish(channel, message) {
      published.push({ channel, message });
      return 1;
    },
  };

  return { redis, hashes, expiries, published };
}

const baseRecord: JobRecord = {
  jobId: "11111111-1111-4111-8111-111111111111",
  status: "queued",
  type: "video",
  sourceKey: "11111111-1111-4111-8111-111111111111/source.mp4",
  mime: "video/mp4",
  bytes: 2048,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  progress: 0,
};

describe("createJobsRepository", () => {
  it("round-trips a record through Redis's string-only hash fields", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });

    await repository.createJob(baseRecord);

    expect(await repository.getJob(baseRecord.jobId)).toEqual(baseRecord);
  });

  it("restores numbers and arrays that Redis flattened to strings", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });

    await repository.createJob(baseRecord);
    await repository.updateJob(baseRecord.jobId, {
      outputs: ["a/full.webp", "a/thumb.webp"],
      renditionsExpected: 3,
      renditionsDone: 1,
      progress: 50,
      durationSeconds: 12.5,
    });

    const record = await repository.getJob(baseRecord.jobId);
    expect(record?.outputs).toEqual(["a/full.webp", "a/thumb.webp"]);
    expect(record?.renditionsExpected).toBe(3);
    expect(record?.renditionsDone).toBe(1);
    expect(record?.progress).toBe(50);
    expect(record?.durationSeconds).toBe(12.5);
  });

  it("applies the job TTL to every write", async () => {
    const { redis, expiries } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 900 });

    await repository.createJob(baseRecord);

    expect(expiries.get(jobKey(baseRecord.jobId))).toBe(900);
  });

  it("publishes every mutation to the job's SSE channel", async () => {
    const { redis, published } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });

    await repository.createJob(baseRecord);
    await repository.updateJob(baseRecord.jobId, { status: "processing", progress: 10 });

    expect(published).toHaveLength(2);
    expect(published.every((e) => e.channel === jobEventsChannel(baseRecord.jobId))).toBe(true);
    expect(JSON.parse(published[1].message)).toMatchObject({ status: "processing", progress: 10 });
  });

  it("returns null for an unknown or expired job rather than an empty record", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });

    expect(await repository.getJob("22222222-2222-4222-8222-222222222222")).toBeNull();
  });

  it("lists recent jobs newest first and skips ids whose hash has expired", async () => {
    const { redis, hashes } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });

    const older = { ...baseRecord, jobId: "33333333-3333-4333-8333-333333333333" };
    const newer = {
      ...baseRecord,
      jobId: "44444444-4444-4444-8444-444444444444",
      createdAt: "2026-01-02T00:00:00.000Z",
      updatedAt: "2026-01-02T00:00:00.000Z",
    };

    await repository.createJob(older);
    await repository.createJob(newer);
    await repository.createJob(baseRecord);

    // Simulate the hash expiring while its id lingers in the sorted set.
    hashes.delete(jobKey(older.jobId));

    const listed = await repository.listRecentJobs(10);
    expect(listed.map((job) => job.jobId)).toEqual([newer.jobId, baseRecord.jobId]);
  });

  it("markFailed records the reason and leaves the job terminal", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });

    await repository.createJob(baseRecord);
    const failed = await repository.markFailed(baseRecord.jobId, "broker refused");

    expect(failed?.status).toBe("failed");
    expect(failed?.error).toBe("broker refused");
  });

  it("updateJob on a missing job returns null instead of resurrecting it", async () => {
    const { redis, hashes } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });

    expect(await repository.updateJob(baseRecord.jobId, { progress: 10 })).toBeNull();
    expect(hashes.has(jobKey(baseRecord.jobId))).toBe(false);
  });

  it("indexes new jobs under the recent-jobs key", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });

    await repository.createJob(baseRecord);

    expect(await redis.zrevrange(RECENT_JOBS_KEY, 0, 9)).toEqual([baseRecord.jobId]);
  });

  it("writes only the fields an update changed, so a stale read cannot undo another worker", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });
    await repository.createJob({ ...baseRecord, status: "processing" });

    // Worker A reads the job, then worker B completes it before A writes.
    const staleRead = await redis.hgetall(jobKey(baseRecord.jobId));
    await repository.updateJob(baseRecord.jobId, { status: "completed" });
    const hgetall = redis.hgetall;
    redis.hgetall = async () => {
      redis.hgetall = hgetall;
      return staleRead;
    };
    await repository.updateJob(baseRecord.jobId, { progress: 40 });

    expect(await repository.getJob(baseRecord.jobId)).toMatchObject({
      status: "completed",
      progress: 40,
    });
  });

  it("round-trips the rendition ladder the plan stage chose", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });
    const renditions = [
      { name: "720p", width: 1280, height: 720, bandwidth: 2_800_000 },
      { name: "360p", width: 640, height: 360, bandwidth: 800_000 },
    ];

    await repository.createJob(baseRecord);
    await repository.updateJob(baseRecord.jobId, { renditions, renditionsExpected: 2 });

    expect((await repository.getJob(baseRecord.jobId))?.renditions).toEqual(renditions);
  });

  it("counts a redelivered rendition once at the fan-out barrier", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });
    await repository.createJob({ ...baseRecord, status: "processing", renditionsExpected: 3 });

    await repository.completeRendition(baseRecord.jobId, "720p");
    // Uploaded and tallied, then redelivered before the ack reached the broker.
    expect(await repository.completeRendition(baseRecord.jobId, "720p")).toMatchObject({
      done: 1,
      expected: 3,
    });

    await repository.completeRendition(baseRecord.jobId, "360p");
    const last = await repository.completeRendition(baseRecord.jobId, "1080p");

    expect(last).toMatchObject({ done: 3, expected: 3 });
    expect(last?.record.renditionsDone).toBe(3);
  });

  it("reports progress over the whole ladder, keeping 100 for uploaded renditions", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });
    await repository.createJob({ ...baseRecord, status: "processing", renditionsExpected: 2 });

    const record = await repository.reportRenditionProgress(baseRecord.jobId, "720p", 100);

    // An encode tops out at 99, averaged with a rung that has not started.
    expect(record).toMatchObject({ progress: 49, renditionsDone: 0 });
  });

  it("does not move a finished job's progress when a redelivered rendition restarts", async () => {
    const { redis } = createFakeRedis();
    const repository = createJobsRepository({ redis, jobTtlSeconds: 60 });
    await repository.createJob({
      ...baseRecord,
      status: "completed",
      progress: 100,
      renditionsExpected: 1,
    });

    await repository.reportRenditionProgress(baseRecord.jobId, "720p", 3);

    expect((await repository.getJob(baseRecord.jobId))?.progress).toBe(100);
  });
});
