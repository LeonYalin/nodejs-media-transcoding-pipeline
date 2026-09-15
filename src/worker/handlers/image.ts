import sharp from "sharp";
import type { ImageJobMessage } from "../../domain/job.js";
import type { JobsRepository } from "../../lib/jobs-repository.js";
import type { ObjectRepository } from "../../lib/object-repository.js";

export interface ImageHandlerDeps {
  objectRepository: Pick<ObjectRepository, "getStream" | "putStream">;
  jobsRepository: Pick<JobsRepository, "updateJob">;
  uploadsBucket: string;
  outputsBucket: string;
}

/**
 * MinIO -> sharp -> MinIO, fully streaming: no temp file and no buffered copy
 * of the source. libvips decodes on its own thread pool, so the event loop
 * stays free while it works.
 */
export function createImageHandler({
  objectRepository,
  jobsRepository,
  uploadsBucket,
  outputsBucket,
}: ImageHandlerDeps) {
  return async function handleImage({ jobId, sourceKey }: ImageJobMessage): Promise<void> {
    await jobsRepository.updateJob(jobId, { status: "processing", progress: 0 });

    const source = await objectRepository.getStream({ bucket: uploadsBucket, key: sourceKey });

    // One sharp input, two clones: both outputs read the same source stream.
    // `autoOrient` because sharp strips EXIF, which would leave phone photos
    // sideways.
    const input = sharp();
    const full = input
      .clone()
      .autoOrient()
      .resize({ width: 1920, withoutEnlargement: true })
      .webp({ quality: 80 });
    const thumb = input
      .clone()
      .autoOrient()
      .resize({ width: 320, withoutEnlargement: true })
      .webp({ quality: 75 });

    // `.pipe` does not forward errors, and destroying `input` does not reach
    // its clones (verified: they hang). So a failed download is pushed into
    // both outputs, where each upload turns it into a rejection.
    source.on("error", (error) => {
      full.destroy(error);
      thumb.destroy(error);
    });
    source.pipe(input);

    const fullKey = `${jobId}/image/full.webp`;
    const thumbKey = `${jobId}/image/thumb.webp`;
    try {
      await Promise.all([
        objectRepository.putStream({
          bucket: outputsBucket,
          key: fullKey,
          body: full,
          contentType: "image/webp",
        }),
        objectRepository.putStream({
          bucket: outputsBucket,
          key: thumbKey,
          body: thumb,
          contentType: "image/webp",
        }),
      ]);
    } catch (error) {
      // When one upload fails, stop the other and release the S3 socket rather
      // than leave them streaming into nothing.
      full.destroy();
      thumb.destroy();
      source.destroy();
      throw error;
    }

    await jobsRepository.updateJob(jobId, {
      status: "completed",
      progress: 100,
      outputs: [fullKey, thumbKey],
    });
  };
}
