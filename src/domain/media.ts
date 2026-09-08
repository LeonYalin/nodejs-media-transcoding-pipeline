import { z } from "zod";

export const IMAGE_MIME_ALLOWLIST = [
  "image/jpeg",
  "image/png",
  "image/webp",
  "image/avif",
] as const;

export const VIDEO_MIME_ALLOWLIST = [
  "video/mp4",
  "video/quicktime", // .mov
  "video/webm",
  "video/x-matroska", // .mkv
] as const;

export const ALL_MIME_ALLOWLIST = [...IMAGE_MIME_ALLOWLIST, ...VIDEO_MIME_ALLOWLIST] as const;

/** The allowlist as a schema, so "unsupported type" is a parse failure, not an if-statement. */
export const MimeTypeSchema = z.enum(ALL_MIME_ALLOWLIST);
export type MimeType = z.infer<typeof MimeTypeSchema>;

export function isImageMime(mime: string): boolean {
  return (IMAGE_MIME_ALLOWLIST as readonly string[]).includes(mime);
}

export function isVideoMime(mime: string): boolean {
  return (VIDEO_MIME_ALLOWLIST as readonly string[]).includes(mime);
}

/**
 * Canonical extension per allowed MIME type.
 *
 * The source object's key is built from this rather than from the uploaded
 * filename: the filename is client-controlled, may be absent, and may carry an
 * extension that contradicts the declared type. ffprobe reads the container
 * from the bytes, but a truthful extension keeps the bucket browsable.
 */
export const EXTENSION_BY_MIME: Record<MimeType, string> = {
  "image/jpeg": ".jpg",
  "image/png": ".png",
  "image/webp": ".webp",
  "image/avif": ".avif",
  "video/mp4": ".mp4",
  "video/quicktime": ".mov",
  "video/webm": ".webm",
  "video/x-matroska": ".mkv",
};

/**
 * One rung of the HLS ladder.
 *
 * `width` is carried explicitly rather than derived at playlist time: the master
 * playlist's RESOLUTION attribute must match what the encoder actually produced,
 * and that depends on the *source* aspect ratio (portrait video is not 16:9).
 * Both dimensions are even because H.264 yuv420p chroma subsampling requires it.
 */
export const RenditionSchema = z.object({
  name: z.string().min(1), // e.g. "1080p" -- also the output directory name
  width: z.number().int().positive().multipleOf(2),
  height: z.number().int().positive().multipleOf(2),
  bandwidth: z.number().int().positive(),
});
export type Rendition = z.infer<typeof RenditionSchema>;

export const ProbeResultSchema = z.object({
  width: z.number().int().positive(),
  height: z.number().int().positive(),
  durationInSeconds: z.number().positive(),
});
export type ProbeResult = z.infer<typeof ProbeResultSchema>;

/**
 * Base domain error. `retryable` is what `worker/retry.ts` reads to choose
 * between the delay queue and the terminal parked queue.
 */
export abstract class MediaDomainError extends Error {
  abstract readonly retryable: boolean;

  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = this.constructor.name;
  }
}

export class UnsupportedMediaError extends MediaDomainError {
  readonly retryable = false;
}

export class CorruptMediaError extends MediaDomainError {
  readonly retryable = false;
}

export class ObjectNotFoundError extends MediaDomainError {
  readonly retryable = false;
}

/**
 * Anything that isn't a declared domain error is assumed transient (network
 * blips, broker hiccups) and therefore worth retrying.
 */
export function isRetryable(error: unknown): boolean {
  return error instanceof MediaDomainError ? error.retryable : true;
}
