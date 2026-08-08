import { describe, expect, it } from "vitest";
import {
  ALL_MIME_ALLOWLIST,
  CorruptMediaError,
  MediaDomainError,
  MimeTypeSchema,
  ObjectNotFoundError,
  RenditionSchema,
  UnsupportedMediaError,
  isImageMime,
  isRetryable,
  isVideoMime,
} from "./media.js";

describe("MIME allowlist", () => {
  it("accepts every allowlisted type and rejects anything else", () => {
    for (const mime of ALL_MIME_ALLOWLIST) {
      expect(MimeTypeSchema.safeParse(mime).success).toBe(true);
    }
    expect(MimeTypeSchema.safeParse("application/pdf").success).toBe(false);
    expect(MimeTypeSchema.safeParse("image/gif").success).toBe(false);
  });

  it("classifies image vs video", () => {
    expect(isImageMime("image/webp")).toBe(true);
    expect(isImageMime("video/mp4")).toBe(false);
    expect(isVideoMime("video/x-matroska")).toBe(true);
    expect(isVideoMime("image/png")).toBe(false);
  });
});

describe("RenditionSchema", () => {
  it("rejects odd dimensions, which H.264 yuv420p cannot encode", () => {
    const base = { name: "240p", width: 426, height: 240, bandwidth: 533_333 };

    expect(RenditionSchema.safeParse(base).success).toBe(true);
    expect(RenditionSchema.safeParse({ ...base, width: 427 }).success).toBe(false);
    expect(RenditionSchema.safeParse({ ...base, height: 241 }).success).toBe(false);
  });
});

describe("domain errors", () => {
  it("marks every declared media error non-retryable", () => {
    expect(new UnsupportedMediaError("bad format").retryable).toBe(false);
    expect(new CorruptMediaError("missing chunks").retryable).toBe(false);
    expect(new ObjectNotFoundError("no such key").retryable).toBe(false);
  });

  it("keeps the subclass name and prototype chain intact", () => {
    const error = new CorruptMediaError("missing chunks");

    expect(error.name).toBe("CorruptMediaError");
    expect(error).toBeInstanceOf(CorruptMediaError);
    expect(error).toBeInstanceOf(MediaDomainError);
    expect(error).toBeInstanceOf(Error);
  });

  it("preserves the underlying cause when wrapping", () => {
    const cause = new Error("ffmpeg exited 1");

    expect(new CorruptMediaError("transcode failed", { cause }).cause).toBe(cause);
  });

  it("treats undeclared errors as transient", () => {
    expect(isRetryable(new Error("ECONNRESET"))).toBe(true);
    expect(isRetryable(new UnsupportedMediaError("nope"))).toBe(false);
  });
});
