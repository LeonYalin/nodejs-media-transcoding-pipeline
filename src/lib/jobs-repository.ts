import { JobRecordSchema, type JobRecord, type JobStatus } from "../domain/job.js";
import type { Rendition } from "../domain/media.js";

/** Key layout, kept in one place so the SSE route and RedisInsight agree with it. */
export const jobKey = (jobId: string) => `job:${jobId}`;
export const jobEventsChannel = (jobId: string) => `job:${jobId}:events`;
/** `renditionName -> percent`. A separate hash, so parallel renditions never write the same field. */
export const jobRenditionsKey = (jobId: string) => `job:${jobId}:renditions`;
export const RECENT_JOBS_KEY = "jobs:recent";

/** Encoding tops out at 99: 100 is written only once a rendition's files are uploaded. */
const RENDITION_DONE = 100;

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
  renditions?: Rendition[];
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
 * lets an update write only the fields it changed.
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
  const { bytes, progress, outputs, renditions, renditionsExpected, renditionsDone, ...rest } =
    fields;
  return JobRecordSchema.parse({
    ...rest,
    bytes: Number(bytes),
    progress: Number(progress),
    ...(outputs !== undefined ? { outputs: JSON.parse(outputs) as unknown } : {}),
    ...(renditions !== undefined ? { renditions: JSON.parse(renditions) as unknown } : {}),
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
  // Explicit return types on these are load-bearing: several call their
  // siblings, and TypeScript cannot infer through a mutually-recursive group
  // without them.
  async function persist(record: JobRecord, fields: Partial<JobRecord>): Promise<JobRecord> {
    await redis.hset(jobKey(record.jobId), encode(fields));
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
    await persist(validated, validated);

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

    // Only the changed fields reach the hash. One job's renditions run on
    // different workers at once, and writing back a whole merged record would
    // let one worker's stale read silently undo another's update.
    const changes = { ...patch, updatedAt: new Date().toISOString() };
    return persist(JobRecordSchema.parse({ ...existing, ...changes }), changes);
  }

  async function markFailed(jobId: string, error: string): Promise<JobRecord | null> {
    return updateJob(jobId, { status: "failed", error });
  }

  /**
   * Stores one rendition's percent and tallies the whole ladder.
   *
   * Keyed by rendition name, so a redelivered rendition overwrites its own
   * entry instead of counting twice. A bare HINCRBY counter would count it
   * twice, and could declare the job complete while another rung is still
   * encoding.
   */
  async function setRenditionPercent(
    jobId: string,
    renditionName: string,
    percent: number,
  ): Promise<{ existing: JobRecord; done: number; progress: number } | null> {
    const existing = await getJob(jobId);
    if (!existing) return null;

    const key = jobRenditionsKey(jobId);
    await redis.hset(key, { [renditionName]: String(percent) });
    await redis.expire(key, jobTtlSeconds);
    const percents = Object.values(await redis.hgetall(key)).map(Number);

    // Averaged over the whole ladder rather than the renditions that have
    // reported so far, so progress cannot reach 100 while rungs are queued.
    const expected = Math.max(existing.renditionsExpected ?? percents.length, 1);
    const progress = Math.floor(percents.reduce((sum, value) => sum + value, 0) / expected);

    return {
      existing,
      done: percents.filter((value) => value === RENDITION_DONE).length,
      progress: Math.min(progress, 100),
    };
  }

  async function reportRenditionProgress(
    jobId: string,
    renditionName: string,
    percent: number,
  ): Promise<JobRecord | null> {
    const encoding = Number.isFinite(percent)
      ? Math.max(0, Math.min(RENDITION_DONE - 1, Math.floor(percent)))
      : 0;
    const tally = await setRenditionPercent(jobId, renditionName, encoding);
    if (!tally) return null;

    // A redelivered rendition restarts at 0, and must not drag a finished (or
    // parked) job's progress backwards.
    if (tally.existing.status !== "processing") return tally.existing;
    return updateJob(jobId, { progress: tally.progress, renditionsDone: tally.done });
  }

  /** The fan-out barrier: the caller finishes the job once `done` reaches `expected`. */
  async function completeRendition(
    jobId: string,
    renditionName: string,
  ): Promise<{ record: JobRecord; done: number; expected: number | undefined } | null> {
    const tally = await setRenditionPercent(jobId, renditionName, RENDITION_DONE);
    if (!tally) return null;

    const record = await updateJob(jobId, { progress: tally.progress, renditionsDone: tally.done });
    if (!record) return null;
    return { record, done: tally.done, expected: record.renditionsExpected };
  }

  return {
    createJob,
    getJob,
    listRecentJobs,
    updateJob,
    markFailed,
    reportRenditionProgress,
    completeRendition,
  };
}

export type JobsRepository = ReturnType<typeof createJobsRepository>;
