import type ffmpeg from "fluent-ffmpeg";
import { describe, expect, it } from "vitest";
import { CorruptMediaError, type Rendition } from "../domain/media.js";
import { buildHlsOutputOptions, parseProbe } from "./ffmpeg.js";

function probeData(streams: Partial<ffmpeg.FfprobeStream>[]): ffmpeg.FfprobeData {
  return {
    streams: streams.map((stream, index) => ({ index, ...stream })),
    format: { duration: 12.5 },
    chapters: [],
  };
}

/** The value following `flag` in an ffmpeg argument list. */
function optionValue(options: string[], flag: string): string | undefined {
  const index = options.indexOf(flag);
  return index === -1 ? undefined : options[index + 1];
}

const rung720: Rendition = { name: "720p", width: 1280, height: 720, bandwidth: 2_800_000 };

describe("parseProbe", () => {
  it("reads the video stream's size and the container duration", () => {
    const data = probeData([
      { codec_type: "audio" },
      { codec_type: "video", width: 1920, height: 1080 },
    ]);

    expect(parseProbe(data)).toEqual({ width: 1920, height: 1080, durationInSeconds: 12.5 });
  });

  it.each([
    { name: "a display-matrix rotation", stream: { side_data_list: [{ rotation: -90 }] } },
    { name: "a legacy rotate tag", stream: { tags: { rotate: "90" } } },
  ])("swaps dimensions for $name, as ffmpeg's auto-rotate will", ({ stream }) => {
    const data = probeData([{ codec_type: "video", width: 1920, height: 1080, ...stream }]);

    expect(parseProbe(data)).toMatchObject({ width: 1080, height: 1920 });
  });

  it("does not swap for an upside-down (180°) source", () => {
    const data = probeData([
      { codec_type: "video", width: 1920, height: 1080, side_data_list: [{ rotation: 180 }] },
    ]);

    expect(parseProbe(data)).toMatchObject({ width: 1920, height: 1080 });
  });

  it("ignores cover art, which ffprobe also reports as a video stream", () => {
    const data = probeData([
      { codec_type: "video", width: 600, height: 600, disposition: { attached_pic: 1 } },
    ]);

    expect(() => parseProbe(data)).toThrow(CorruptMediaError);
  });

  it("rejects a source with no usable duration as corrupt, not transient", () => {
    const data = { ...probeData([{ codec_type: "video", width: 640, height: 360 }]), format: {} };

    expect(() => parseProbe(data)).toThrow(CorruptMediaError);
  });
});

describe("buildHlsOutputOptions", () => {
  const options = buildHlsOutputOptions(rung720, { outputDir: "/work/720p", segmentSeconds: 4 });

  it("scales to the rung's exact, aspect-correct size", () => {
    expect(optionValue(options, "-vf")).toBe("scale=1280:720");
  });

  it("encodes at the H.264 level the master playlist advertises", () => {
    expect(optionValue(options, "-level:v")).toBe("3.1");
    const hd = buildHlsOutputOptions(
      { name: "1080p", width: 1920, height: 1080, bandwidth: 5_000_000 },
      { outputDir: "/work/1080p", segmentSeconds: 4 },
    );
    expect(optionValue(hd, "-level:v")).toBe("4.0");
  });

  it("keeps audio plus video within the advertised bandwidth", () => {
    expect(Number(optionValue(options, "-b:v")) + Number(optionValue(options, "-b:a"))).toBe(
      rung720.bandwidth,
    );
  });

  it("puts keyframes on the segment clock so every rung cuts at the same instants", () => {
    expect(optionValue(options, "-hls_time")).toBe("4");
    expect(optionValue(options, "-force_key_frames")).toBe("expr:gte(t,n_forced*4)");
    expect(optionValue(options, "-sc_threshold")).toBe("0");
  });

  it("names segments per the data contract, inside the rendition's directory", () => {
    expect(optionValue(options, "-hls_segment_filename")).toBe("/work/720p/seg_%03d.ts");
    expect(optionValue(options, "-hls_playlist_type")).toBe("vod");
  });
});
