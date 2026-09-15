import type { Rendition } from "../domain/media.js";

const AAC_LC = "mp4a.40.2";

/**
 * The H.264 level that covers a rung's frame size. Exported because the encoder
 * (media/ffmpeg.ts) must produce exactly the level this playlist advertises.
 */
export function h264Level(height: number): "3.0" | "3.1" | "4.0" {
  if (height <= 480) return "3.0";
  if (height <= 720) return "3.1";
  return "4.0";
}

const AVC_LEVEL_HEX = { "3.0": "1e", "3.1": "1f", "4.0": "28" } as const;

/**
 * H.264 Main profile, with the level that actually covers the frame size.
 * Advertising too low a level (e.g. 3.1 for 1080p) makes strict players reject
 * or mis-select the variant.
 */
function avcCodec(height: number): string {
  return `avc1.4d40${AVC_LEVEL_HEX[h264Level(height)]}`;
}

/** Builds the HLS master playlist that fans out to each variant's own index.m3u8. */
export function buildMasterPlaylist(renditions: Rendition[]): string {
  if (renditions.length === 0) {
    throw new Error("Cannot build a master playlist with no renditions");
  }

  const lines = ["#EXTM3U", "#EXT-X-VERSION:3"];

  for (const { name, width, height, bandwidth } of renditions) {
    lines.push(
      `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${width}x${height},` +
        `CODECS="${avcCodec(height)},${AAC_LC}"`,
    );
    // Variant path uses the rendition name so it can never drift from the
    // directory the worker actually wrote (media-outputs/{jobId}/hls/{name}/).
    lines.push(`${name}/index.m3u8`);
  }

  return lines.join("\n") + "\n";
}
