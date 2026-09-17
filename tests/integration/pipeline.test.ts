import { readFile } from "node:fs/promises";
import path from "node:path";
import sharp from "sharp";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, inject, it } from "vitest";
import type { JobRecord } from "../../src/domain/job.js";
import { startApi, startWorker, waitFor } from "./helpers.js";

let api: Awaited<ReturnType<typeof startApi>>;
let worker: StartedTestContainer;

beforeAll(async () => {
  api = await startApi();
  worker = await startWorker();
});

afterAll(async () => {
  await worker?.stop();
  await api?.close();
});

async function waitForStatus(
  jobId: string,
  status: JobRecord["status"],
  timeoutMs = 90_000,
): Promise<JobRecord> {
  return waitFor(
    `job ${jobId} to be ${status}`,
    async () => {
      const record = await api.jobsRepository.getJob(jobId);
      if (record?.status === "failed" && status !== "failed") {
        throw new Error(`Job failed: ${record.error}`);
      }
      return record?.status === status ? record : null;
    },
    { timeoutMs },
  );
}

describe("upload → queue → transcode → MinIO", () => {
  it("transcodes an image into full and thumb webp", async () => {
    const jpeg = await sharp({
      create: { width: 2400, height: 1600, channels: 3, background: "#3a6ea5" },
    })
      .jpeg()
      .toBuffer();

    const jobId = await api.upload(jpeg, "image/jpeg", "photo.jpg");
    const record = await waitForStatus(jobId, "completed");

    expect(record.outputs).toEqual([`${jobId}/image/full.webp`, `${jobId}/image/thumb.webp`]);
    const full = await sharp(await api.readOutput(`${jobId}/image/full.webp`)).metadata();
    const thumb = await sharp(await api.readOutput(`${jobId}/image/thumb.webp`)).metadata();
    expect(full).toMatchObject({ format: "webp", width: 1920 });
    expect(thumb).toMatchObject({ format: "webp", width: 320 });
  });

  it("fans a 720p video out into an HLS ladder with no upscaled rungs", async () => {
    const video = await readFile(path.join(inject("fixturesDir"), "ladder.mp4"));

    const jobId = await api.upload(video, "video/mp4", "ladder.mp4");
    const record = await waitForStatus(jobId, "completed");

    expect(record.renditions?.map((rendition) => rendition.name)).toEqual(["720p", "360p"]);
    const master = (await api.readOutput(`${jobId}/hls/master.m3u8`)).toString("utf8");
    expect(master).toContain("720p/index.m3u8");
    expect(master).toContain("360p/index.m3u8");
    expect(master).not.toContain("1080p");
    expect((await api.readOutput(`${jobId}/hls/360p/index.m3u8`)).toString("utf8")).toContain(
      "seg_000.ts",
    );
    expect((await api.readOutput(`${jobId}/poster.jpg`)).length).toBeGreaterThan(0);
  });

  it("loses no job when its worker is killed mid-transcode", async () => {
    const video = await readFile(path.join(inject("fixturesDir"), "long.mp4"));
    const jobId = await api.upload(video, "video/mp4", "long.mp4");

    // Progress above 0 means ffmpeg is encoding and the delivery is unacked.
    await waitFor("the encode to start", async () => {
      const record = await api.jobsRepository.getJob(jobId);
      return record && record.progress > 0;
    });

    // No grace period: the container dies before it can finish or ack.
    await worker.stop({ timeout: 0 });
    expect((await api.jobsRepository.getJob(jobId))?.status).toBe("processing");
    worker = await startWorker();

    // The broker redelivers the unacked rendition to the new worker.
    const record = await waitForStatus(jobId, "completed", 120_000);
    expect(record.renditionsDone).toBe(1);
    expect((await api.readOutput(`${jobId}/hls/master.m3u8`)).toString("utf8")).toContain(
      "360p/index.m3u8",
    );
  });
});
