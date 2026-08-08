import { describe, expect, it } from "vitest";
import { CorruptMediaError, type ProbeResult } from "../domain/media.js";
import { buildLadder } from "./ladder.js";

const probe = (width: number, height: number): ProbeResult => ({
  width,
  height,
  durationInSeconds: 30,
});

describe("buildLadder", () => {
  it("keeps every rung when the source matches the top tier", () => {
    const ladder = buildLadder(probe(1920, 1080));

    expect(ladder.map((r) => r.name)).toEqual(["1080p", "720p", "360p"]);
    expect(ladder.map((r) => r.width)).toEqual([1920, 1280, 640]);
  });

  it("never upscales past the source height", () => {
    expect(buildLadder(probe(1280, 720)).map((r) => r.name)).toEqual(["720p", "360p"]);
    expect(buildLadder(probe(640, 360)).map((r) => r.name)).toEqual(["360p"]);
  });

  it("falls back to a single source-height rung below the lowest tier", () => {
    const [only, ...rest] = buildLadder(probe(426, 240));

    expect(rest).toHaveLength(0);
    expect(only.name).toBe("240p");
    expect(only.height).toBe(240);
    expect(only.bandwidth).toBe(533_333); // (240 / 360) * 800_000
  });

  it("derives width from the source aspect ratio, not a hardcoded 16:9", () => {
    // 4:3 -- a 16:9 assumption would wrongly claim 1280x720.
    expect(buildLadder(probe(1440, 1080))[1]).toMatchObject({ name: "720p", width: 960 });

    // Portrait phone video.
    expect(buildLadder(probe(1080, 1920))[0]).toMatchObject({ name: "1080p", width: 608 });
  });

  it("always emits even dimensions, which H.264 yuv420p requires", () => {
    for (const source of [probe(427, 241), probe(1001, 563), probe(999, 1777)]) {
      for (const rung of buildLadder(source)) {
        expect(rung.width % 2).toBe(0);
        expect(rung.height % 2).toBe(0);
      }
    }
  });

  it("throws rather than returning an empty ladder, which would hang the barrier", () => {
    expect(() => buildLadder(probe(0, 0))).toThrow(CorruptMediaError);
    expect(() => buildLadder(probe(1920, -1080))).toThrow(CorruptMediaError);
    expect(() => buildLadder(probe(1920, 1080.5))).toThrow(CorruptMediaError);
  });

  it("does not alias the shared target table", () => {
    const first = buildLadder(probe(1920, 1080));
    first[0].bandwidth = 1;

    expect(buildLadder(probe(1920, 1080))[0].bandwidth).toBe(5_000_000);
  });
});
