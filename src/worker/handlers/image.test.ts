import { randomUUID } from "node:crypto";
import { Readable } from "node:stream";
import sharp from "sharp";
import { describe, expect, it } from "vitest";
import type { ImageJobMessage } from "../../domain/job.js";
import type { JobPatch, JobsRepository } from "../../lib/jobs-repository.js";
import type { ObjectRepository } from "../../lib/object-repository.js";
import { createImageHandler } from "./image.js";

// Real sharp throughout: it runs on the host, and its stream and error
// behaviour is exactly what these tests pin down. Only MinIO and Redis are faked.

function createFakeObjectRepository(openSource: () => Readable) {
  const stored = new Map<string, Buffer>();
  const failingKeys = new Map<string, Error>();

  const objectRepository: Pick<ObjectRepository, "getStream" | "putStream"> = {
    async getStream() {
      return openSource();
    },
    async putStream({ key, body }) {
      const failure = failingKeys.get(key);
      // Fails before reading, like a MinIO that refuses the upload outright.
      if (failure) throw failure;
      const chunks: Buffer[] = [];
      for await (const chunk of body) chunks.push(chunk as Buffer);
      stored.set(key, Buffer.concat(chunks));
      return { bytes: stored.get(key)!.length };
    },
  };

  return {
    objectRepository,
    stored,
    failUploadOf: (key: string, error: Error) => failingKeys.set(key, error),
  };
}

function build(openSource: () => Readable) {
  const objects = createFakeObjectRepository(openSource);
  const updates: JobPatch[] = [];
  const jobsRepository: Pick<JobsRepository, "updateJob"> = {
    async updateJob(_jobId, patch) {
      updates.push(patch);
      return null;
    },
  };

  const handleImage = createImageHandler({
    objectRepository: objects.objectRepository,
    jobsRepository,
    uploadsBucket: "media-uploads",
    outputsBucket: "media-outputs",
  });

  const jobId = randomUUID();
  const message: ImageJobMessage = {
    type: "image",
    jobId,
    sourceKey: `${jobId}/source.png`,
    mime: "image/png",
  };

  return { handleImage, message, jobId, updates, ...objects };
}

async function png(width: number, height: number): Promise<Buffer> {
  return sharp({ create: { width, height, channels: 3, background: "teal" } })
    .png()
    .toBuffer();
}

async function dimensionsOf(image: Buffer) {
  const { width, height, format } = await sharp(image).metadata();
  return { width, height, format };
}

describe("createImageHandler", () => {
  it("streams both outputs to their deterministic keys and completes the job", async () => {
    const source = await png(2400, 1200);
    const { handleImage, message, jobId, stored, updates } = build(() => Readable.from([source]));

    await handleImage(message);

    expect(await dimensionsOf(stored.get(`${jobId}/image/full.webp`)!)).toEqual({
      width: 1920,
      height: 960,
      format: "webp",
    });
    expect(await dimensionsOf(stored.get(`${jobId}/image/thumb.webp`)!)).toMatchObject({
      width: 320,
      height: 160,
    });
    expect(updates).toEqual([
      { status: "processing", progress: 0 },
      {
        status: "completed",
        progress: 100,
        outputs: [`${jobId}/image/full.webp`, `${jobId}/image/thumb.webp`],
      },
    ]);
  });

  it("never enlarges a source smaller than the target width", async () => {
    const source = await png(200, 100);
    const { handleImage, message, jobId, stored } = build(() => Readable.from([source]));

    await handleImage(message);

    expect(await dimensionsOf(stored.get(`${jobId}/image/full.webp`)!)).toMatchObject({
      width: 200,
      height: 100,
    });
  });

  it("applies EXIF orientation, so phone photos do not come out sideways", async () => {
    const landscapeFrame = await sharp(await png(400, 200))
      .jpeg()
      .withMetadata({ orientation: 6 })
      .toBuffer();
    const { handleImage, message, jobId, stored } = build(() => Readable.from([landscapeFrame]));

    await handleImage(message);

    expect(await dimensionsOf(stored.get(`${jobId}/image/full.webp`)!)).toMatchObject({
      width: 200,
      height: 400,
    });
  });

  it("fails without completing the job when the bytes are not an image", async () => {
    const { handleImage, message, updates } = build(() =>
      Readable.from([Buffer.from("definitely not a png")]),
    );

    await expect(handleImage(message)).rejects.toThrow("unsupported image format");
    expect(updates.some((patch) => patch.status === "completed")).toBe(false);
  });

  it("rejects, rather than hangs, when the download dies mid-stream", async () => {
    const source = await png(800, 600);
    let sent = false;
    const { handleImage, message } = build(
      () =>
        new Readable({
          read() {
            if (sent) return;
            sent = true;
            this.push(source.subarray(0, 64));
            setImmediate(() => this.destroy(new Error("socket reset")));
          },
        }),
    );

    await expect(handleImage(message)).rejects.toThrow("socket reset");
  });

  it("rejects, rather than hangs, when one of the two uploads fails", async () => {
    const source = await png(800, 600);
    const { handleImage, message, jobId, failUploadOf } = build(() => Readable.from([source]));
    failUploadOf(`${jobId}/image/full.webp`, new Error("minio unavailable"));

    await expect(handleImage(message)).rejects.toThrow("minio unavailable");
  });
});
