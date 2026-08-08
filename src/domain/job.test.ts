import { describe, expect, it } from "vitest";
import { JobMessageSchema, JobRecordSchema, VideoRenditionJobMessageSchema } from "./job.js";

const jobId = "a3fa7df2-2c67-4aef-b328-3d14214643b2";

describe("JobMessageSchema", () => {
  const video = { type: "video", jobId, sourceKey: `${jobId}/source.mp4`, mime: "video/mp4" };

  it("round-trips a valid message", () => {
    expect(JobMessageSchema.parse(video)).toEqual(video);
  });

  it("rejects a mime outside the allowlist", () => {
    expect(JobMessageSchema.safeParse({ ...video, mime: "video/avi" }).success).toBe(false);
  });

  it("rejects a jobId that is not a uuid", () => {
    expect(JobMessageSchema.safeParse({ ...video, jobId: "not-a-uuid" }).success).toBe(false);
  });

  it("rejects an unknown type", () => {
    expect(JobMessageSchema.safeParse({ ...video, type: "audio" }).success).toBe(false);
  });
});

describe("VideoRenditionJobMessageSchema", () => {
  const rendition = { name: "720p", width: 1280, height: 720, bandwidth: 2_800_000 };
  const message = {
    type: "video",
    jobId,
    sourceKey: `${jobId}/source.mp4`,
    mime: "video/mp4",
    rendition,
  };

  it("carries the rung it is responsible for", () => {
    expect(VideoRenditionJobMessageSchema.parse(message).rendition).toEqual(rendition);
  });

  it("cannot be built without a rendition", () => {
    const { rendition: _omitted, ...withoutRendition } = message;

    expect(VideoRenditionJobMessageSchema.safeParse(withoutRendition).success).toBe(false);
  });
});

describe("JobRecordSchema", () => {
  const record = {
    jobId,
    status: "queued",
    type: "image",
    sourceKey: `${jobId}/source.jpg`,
    mime: "image/jpeg",
    bytes: 1024,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    progress: 0,
  };

  it("accepts a minimal record, leaving fan-out fields optional", () => {
    expect(JobRecordSchema.parse(record)).toEqual(record);
  });

  it("accepts the fan-out and completion fields", () => {
    const done = {
      ...record,
      status: "completed",
      progress: 100,
      outputs: [`${jobId}/hls/master.m3u8`],
      renditionsExpected: 3,
      renditionsDone: 3,
    };

    expect(JobRecordSchema.parse(done)).toEqual(done);
  });

  it("rejects out-of-range progress", () => {
    expect(JobRecordSchema.safeParse({ ...record, progress: 101 }).success).toBe(false);
    expect(JobRecordSchema.safeParse({ ...record, progress: -1 }).success).toBe(false);
  });

  it("rejects a non-ISO timestamp", () => {
    expect(JobRecordSchema.safeParse({ ...record, createdAt: "2026-08-07" }).success).toBe(false);
  });
});
