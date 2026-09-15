import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { logger } from "../lib/logger.js";

/**
 * A fresh temp directory for one job. Callers remove it in `finally` -- on
 * success, on a thrown error, and therefore on the retry and park paths too.
 *
 * Only video needs this: ffmpeg wants a seekable input and writes many segment
 * files. Anything that can stream, must.
 */
export async function createWorkspace(prefix: string) {
  const dir = await mkdtemp(path.join(tmpdir(), prefix));

  /**
   * Never throws, so awaiting it in `finally` cannot replace the job's own
   * outcome -- a finished transcode must not be retried because a delete failed.
   */
  async function cleanup(): Promise<void> {
    await rm(dir, { recursive: true, force: true }).catch((error: unknown) => {
      logger.warn({ err: error, dir }, "Could not remove workspace");
    });
  }

  return { path: dir, cleanup };
}

export type Workspace = Awaited<ReturnType<typeof createWorkspace>>;
