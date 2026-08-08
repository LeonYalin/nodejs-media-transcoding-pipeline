import { describe, expect, it } from "vitest";
import type { ProbeResult } from "../domain/media.js";
import { buildMasterPlaylist } from "./hls.js";
import { buildLadder } from "./ladder.js";

const probe = (width: number, height: number): ProbeResult => ({
  width,
  height,
  durationInSeconds: 30,
});

describe("buildMasterPlaylist", () => {
  it("emits a header and one STREAM-INF + path pair per rung", () => {
    const playlist = buildMasterPlaylist(buildLadder(probe(1920, 1080)));

    expect(playlist).toBe(
      [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        '#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,CODECS="avc1.4d4028,mp4a.40.2"',
        "1080p/index.m3u8",
        '#EXT-X-STREAM-INF:BANDWIDTH=2800000,RESOLUTION=1280x720,CODECS="avc1.4d401f,mp4a.40.2"',
        "720p/index.m3u8",
        '#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360,CODECS="avc1.4d401e,mp4a.40.2"',
        "360p/index.m3u8",
        "",
      ].join("\n"),
    );
  });

  it("advertises an H.264 level that covers the frame size", () => {
    const playlist = buildMasterPlaylist(buildLadder(probe(1920, 1080)));

    // 1080p needs L4.0 (4d4028); claiming L3.1 makes strict players reject it.
    expect(playlist).toContain('RESOLUTION=1920x1080,CODECS="avc1.4d4028');
    expect(playlist).toContain('RESOLUTION=1280x720,CODECS="avc1.4d401f');
    expect(playlist).toContain('RESOLUTION=640x360,CODECS="avc1.4d401e');
  });

  it("reports the real resolution for non-16:9 sources", () => {
    expect(buildMasterPlaylist(buildLadder(probe(1080, 1920)))).toContain("RESOLUTION=608x1080");
    expect(buildMasterPlaylist(buildLadder(probe(1440, 1080)))).toContain("RESOLUTION=1440x1080");
  });

  it("points each variant at the directory the worker actually writes", () => {
    const ladder = buildLadder(probe(426, 240));

    expect(buildMasterPlaylist(ladder)).toContain(`${ladder[0].name}/index.m3u8`);
  });

  it("refuses to build an empty playlist", () => {
    expect(() => buildMasterPlaylist([])).toThrow(/no renditions/);
  });
});
