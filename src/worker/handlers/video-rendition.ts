import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readdir } from "node:fs/promises";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { VideoRenditionJobMessage } from "../../domain/job.js";
import type { JobsRepository } from "../../lib/jobs-repository.js";
import { logger } from "../../lib/logger.js";
import type { ObjectRepository } from "../../lib/object-repository.js";
import { transcodeToHls } from "../../media/ffmpeg.js";
import { buildMasterPlaylist } from "../../media/hls.js";
import { createWorkspace } from "../workspace.js";

const PLAYLIST_CONTENT_TYPE = "application/vnd.apple.mpegurl";
const CONTENT_TYPE_BY_EXTENSION: Record<string, string> = {
  ".m3u8": PLAYLIST_CONTENT_TYPE,
  ".ts": "video/mp2t",
};
const PROGRESS_INTERVAL_MS = 1_000;

export interface VideoRenditionHandlerDeps {
  objectRepository: Pick<ObjectRepository, "getStream" | "putStream">;
  jobsRepository: Pick<
    JobsRepository,
    "reportRenditionProgress" | "completeRendition" | "updateJob"
  >;
  uploadsBucket: string;
  outputsBucket: string;
  segmentSeconds: number;
}

/**
 * Encodes one rung to HLS, uploads it, then passes the fan-out barrier. The
 * worker whose completion brings the tally up to the ladder size writes the
 * master playlist and marks the job completed.
 */
export function createVideoRenditionHandler({
  objectRepository,
  jobsRepository,
  uploadsBucket,
  outputsBucket,
  segmentSeconds,
}: VideoRenditionHandlerDeps) {
  return async function handleVideoRendition(message: VideoRenditionJobMessage): Promise<void> {
    const { jobId, sourceKey, rendition } = message;

    const workspace = await createWorkspace(`rendition-${jobId}-${rendition.name}-`);
    try {
      const sourcePath = path.join(workspace.path, `source${path.extname(sourceKey)}`);
      await pipeline(
        await objectRepository.getStream({ bucket: uploadsBucket, key: sourceKey }),
        createWriteStream(sourcePath),
      );

      let lastReportAt = 0;
      let reporting: Promise<unknown> = Promise.resolve();
      const reportProgress = (percent: number) => {
        const now = Date.now();
        if (now - lastReportAt < PROGRESS_INTERVAL_MS) return;
        lastReportAt = now;
        // Chained, not awaited: ffmpeg's event cannot wait, and chaining keeps
        // writes in order. A failed progress write is cosmetic and must not
        // fail an encode that is otherwise succeeding.
        reporting = reporting
          .then(() => jobsRepository.reportRenditionProgress(jobId, rendition.name, percent))
          .catch((error: unknown) => {
            logger.warn({ err: error, jobId, rendition: rendition.name }, "Progress write failed");
          });
      };

      const outputDir = path.join(workspace.path, rendition.name);
      await mkdir(outputDir);
      await transcodeToHls(sourcePath, rendition, {
        outputDir,
        segmentSeconds,
        onProgress: reportProgress,
      });
      // Drained before completion, so a late progress write cannot land after it.
      await reporting;

      // Segments first, playlist last: a player that can fetch index.m3u8 must
      // never find a segment it lists still missing.
      const files = (await readdir(outputDir)).sort(
        (a, b) => Number(a.endsWith(".m3u8")) - Number(b.endsWith(".m3u8")),
      );
      for (const file of files) {
        await objectRepository.putStream({
          bucket: outputsBucket,
          key: `${jobId}/hls/${rendition.name}/${file}`,
          body: createReadStream(path.join(outputDir, file)),
          contentType: CONTENT_TYPE_BY_EXTENSION[path.extname(file)] ?? "application/octet-stream",
        });
      }
    } finally {
      await workspace.cleanup();
    }

    const tally = await jobsRepository.completeRendition(jobId, rendition.name);
    if (!tally || tally.expected === undefined || tally.done < tally.expected) return;

    // Every rung is uploaded. More than one worker can observe this (and so can
    // a redelivery); the playlist is deterministic, so each writes the same bytes.
    const masterKey = `${jobId}/hls/master.m3u8`;
    await objectRepository.putStream({
      bucket: outputsBucket,
      key: masterKey,
      body: Readable.from([Buffer.from(buildMasterPlaylist(tally.record.renditions ?? []))]),
      contentType: PLAYLIST_CONTENT_TYPE,
    });
    await jobsRepository.updateJob(jobId, {
      status: "completed",
      progress: 100,
      outputs: [`${jobId}/poster.jpg`, masterKey],
    });
  };
}
