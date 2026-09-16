import { createReadStream, createWriteStream } from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import type { VideoJobMessage } from "../../domain/job.js";
import type { JobPublisher } from "../../lib/amqp.js";
import type { JobsRepository } from "../../lib/jobs-repository.js";
import type { ObjectRepository } from "../../lib/object-repository.js";
import { ROUTING_KEYS } from "../../lib/topology.js";
import { extractPoster, probeVideo } from "../../media/ffmpeg.js";
import { buildLadder } from "../../media/ladder.js";
import { createWorkspace } from "../workspace.js";

export interface VideoPlanHandlerDeps {
  objectRepository: Pick<ObjectRepository, "getStream" | "putStream">;
  jobsRepository: Pick<JobsRepository, "updateJob">;
  jobPublisher: Pick<JobPublisher, "publish">;
  uploadsBucket: string;
  outputsBucket: string;
  /** Aborted by the worker's second shutdown signal: kills a running ffmpeg. */
  abortSignal: AbortSignal;
}

/**
 * The fast half of a video job: probe, choose the ladder, extract the poster,
 * then fan out one rendition message per rung. Kept apart from the encodes so
 * the rungs run in parallel on whichever workers are free.
 */
export function createVideoPlanHandler({
  objectRepository,
  jobsRepository,
  jobPublisher,
  uploadsBucket,
  outputsBucket,
  abortSignal,
}: VideoPlanHandlerDeps) {
  return async function handleVideoPlan(message: VideoJobMessage): Promise<void> {
    const { jobId, sourceKey } = message;
    await jobsRepository.updateJob(jobId, { status: "processing", progress: 0 });

    const workspace = await createWorkspace(`plan-${jobId}-`);
    try {
      // ffprobe and the poster seek both need a seekable file, not a stream.
      const sourcePath = path.join(workspace.path, `source${path.extname(sourceKey)}`);
      await pipeline(
        await objectRepository.getStream({ bucket: uploadsBucket, key: sourceKey }),
        createWriteStream(sourcePath),
      );

      const probe = await probeVideo(sourcePath);
      const renditions = buildLadder(probe);

      // 1 s in, as specified -- or halfway through a clip shorter than 2 s,
      // where a fixed 1 s seek would land past the last frame.
      const posterPath = path.join(workspace.path, "poster.jpg");
      await extractPoster(
        sourcePath,
        posterPath,
        Math.min(1, probe.durationInSeconds / 2),
        abortSignal,
      );
      await objectRepository.putStream({
        bucket: outputsBucket,
        key: `${jobId}/poster.jpg`,
        body: createReadStream(posterPath),
        contentType: "image/jpeg",
      });

      // Recorded before the fan-out: the barrier compares against it, so it
      // must exist before the first rendition can possibly finish.
      await jobsRepository.updateJob(jobId, {
        renditions,
        renditionsExpected: renditions.length,
      });

      // If this job is redelivered after a partial fan-out, every rung is
      // published again. That is safe: output keys are deterministic, and the
      // barrier counts renditions by name, not by arrival.
      for (const rendition of renditions) {
        await jobPublisher.publish(ROUTING_KEYS.VIDEO_RENDITION, { ...message, rendition });
      }
    } finally {
      await workspace.cleanup();
    }
  };
}
