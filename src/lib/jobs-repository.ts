import { JobRecordSchema, type JobRecord, type JobStatus } from "../domain/job.js";

/** Key layout, kept in one place so the SSE route and RedisInsight agree with it. */
export const jobKey = (jobId: string) => `job:${jobId}`;
export const jobEventsChannel = (jobId: string) => `job:${jobId}:events`;
export const RECENT_JOBS_KEY = "jobs:recent";

/**
 * The slice of ioredis this module uses, declared structurally so tests can pass
 * a small in-memory fake instead of mocking the module (see topology.ts for the
 * same pattern).
 */
export interface JobsRepositoryRedis {
  hset(key: string, value: Record<string, string>): Promise<number>;
  hgetall(key: string): Promise<Record<string, string>>;
  expire(key: string, seconds: number): Promise<number>;
  zadd(key: string, score: number, member: string): Promise<string | number>;
  zrevrange(key: string, start: number, stop: number): Promise<string[]>;
  zremrangebyrank(key: string, start: number, stop: number): Promise<number>;
  publish(channel: string, message: string): Promise<number>;
}

/** The fields a later stage is allowed to move. `jobId`/`createdAt` are immutable. */
export interface JobPatch {
  status?: JobStatus;
  progress?: number;
  error?: string;
  outputs?: string[];
  renditionsExpected?: number;
  renditionsDone?: number;
}

export interface JobsRepositoryDeps {
  redis: JobsRepositoryRedis;
  jobTtlSeconds: number;
  /** Cap on the `jobs:recent` index the UI table reads. */
  recentLimit?: number;
}

/**
 * Redis stores strings, so numbers and arrays need an explicit encoding on the
 * way in and a matching decode on the way out. A hash (rather than one blob) is
 * deliberate: it keeps the record browsable field-by-field in RedisInsight and
 * lets the worker's HINCRBY barrier operate on `renditionsDone` atomically.
 */
function encode(record: Partial<JobRecord>): Record<string, string> {
  const fields: Record<string, string> = {};
  for (const [key, value] of Object.entries(record)) {
    if (value === undefined) continue;
    fields[key] = Array.isArray(value) ? JSON.stringify(value) : String(value);
  }
  return fields;
}

function decode(fields: Record<string, string>): JobRecord {
  const { bytes, progress, outputs, renditionsExpected, renditionsDone, ...rest } = fields;
  return JobRecordSchema.parse({
    ...rest,
    bytes: Number(bytes),
    progress: Number(progress),
    ...(outputs !== undefined ? { outputs: JSON.parse(outputs) as string[] } : {}),
    ...(renditionsExpected !== undefined ? { renditionsExpected: Number(renditionsExpected) } : {}),
    ...(renditionsDone !== undefined ? { renditionsDone: Number(renditionsDone) } : {}),
  });
}

/**
 * The only module that writes Redis.
 *
 * Every mutation refreshes `updatedAt` and publishes the resulting record to
 * `job:{id}:events`, which is what the API's SSE route relays to the browser --
 * so progress can never be written without the UI being told.
 */
export function createJobsRepository({
  redis,
  jobTtlSeconds,
  recentLimit = 100,
}: JobsRepositoryDeps) {
  // Explicit return types on these are load-bearing: `listRecentJobs` and
  // `markFailed` call their siblings, and TypeScript cannot infer through a
  // mutually-recursive group without them.
  async function persist(record: JobRecord): Promise<JobRecord> {
    await redis.hset(jobKey(record.jobId), encode(record));
    await redis.expire(jobKey(record.jobId), jobTtlSeconds);
    await redis.publish(jobEventsChannel(record.jobId), JSON.stringify(record));
    return record;
  }

  async function getJob(jobId: string): Promise<JobRecord | null> {
    const fields = await redis.hgetall(jobKey(jobId));
    // ioredis returns {} for a missing key, never null.
    if (Object.keys(fields).length === 0) return null;
    return decode(fields);
  }

  async function createJob(record: JobRecord): Promise<JobRecord> {
    const validated = JobRecordSchema.parse(record);
    await persist(validated);

    // Sorted by creation time so the UI table can read the newest N. Trimmed
    // on write because the index has no TTL of its own -- the job hashes
    // expire underneath it, and an untrimmed set would grow forever.
    await redis.zadd(RECENT_JOBS_KEY, Date.parse(validated.createdAt), validated.jobId);
    await redis.zremrangebyrank(RECENT_JOBS_KEY, 0, -(recentLimit + 1));

    return validated;
  }

  async function listRecentJobs(limit = 20): Promise<JobRecord[]> {
    const ids = await redis.zrevrange(RECENT_JOBS_KEY, 0, limit - 1);
    const records = await Promise.all(ids.map(getJob));
    // Expired hashes leave their id behind in the index; drop those rather
    // than surfacing holes to the UI.
    return records.filter((record): record is JobRecord => record !== null);
  }

  async function updateJob(jobId: string, patch: JobPatch): Promise<JobRecord | null> {
    const existing = await getJob(jobId);
    if (!existing) return null;
    return persist(
      JobRecordSchema.parse({ ...existing, ...patch, updatedAt: new Date().toISOString() }),
    );
  }

  async function markFailed(jobId: string, error: string): Promise<JobRecord | null> {
    return updateJob(jobId, { status: "failed", error });
  }

  return { createJob, getJob, listRecentJobs, updateJob, markFailed };
}

export type JobsRepository = ReturnType<typeof createJobsRepository>;
