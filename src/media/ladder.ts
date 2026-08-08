import { CorruptMediaError, type ProbeResult, type Rendition } from "../domain/media.js";

export const HLS_LADDER_TARGETS = [
  { height: 1080, bandwidth: 5_000_000 },
  { height: 720, bandwidth: 2_800_000 },
  { height: 360, bandwidth: 800_000 },
] as const;

/** H.264 yuv420p needs even dimensions; ffmpeg's `scale=-2:h` rounds the same way. */
const toEven = (n: number) => Math.max(2, n - (n % 2));

/**
 * Selects the ladder rungs for a probed source, never upscaling.
 *
 * Throws rather than returning `[]` on unusable input: an empty ladder would set
 * `renditionsExpected = 0`, so the fan-out barrier could never fire and the job
 * would hang at `processing` forever. `CorruptMediaError` is non-retryable, so
 * the message parks immediately instead of burning three attempts first.
 */
export function buildLadder(probe: ProbeResult): Rendition[] {
  const { width, height } = probe;

  if (!Number.isInteger(width) || !Number.isInteger(height) || width <= 0 || height <= 0) {
    throw new CorruptMediaError(`Unusable source dimensions: ${width}x${height}`);
  }

  const targets = HLS_LADDER_TARGETS.filter((t) => t.height <= height);

  // Below the lowest standard rung, emit a single rung at the source height,
  // with bandwidth scaled proportionally down from 360p.
  const rungs = targets.length
    ? targets
    : [{ height, bandwidth: Math.max(100_000, Math.round((height / 360) * 800_000)) }];

  return rungs.map(({ height: rungHeight, bandwidth }) => {
    const even = toEven(rungHeight);
    return {
      name: `${even}p`,
      // Width follows the source aspect ratio, so portrait and 4:3 stay correct.
      width: toEven(Math.round((even * width) / height)),
      height: even,
      bandwidth,
    };
  });
}
