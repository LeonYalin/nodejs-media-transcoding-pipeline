import ffmpeg from "fluent-ffmpeg";
import path from "node:path";
import {
  CorruptMediaError,
  ProbeResultSchema,
  type ProbeResult,
  type Rendition,
} from "../domain/media.js";
import { h264Level } from "./hls.js";

const AUDIO_BITRATE = 128_000;

/** Newer ffprobe reports rotation as a display matrix; older builds as a `rotate` tag. */
function rotationOf(stream: ffmpeg.FfprobeStream): number {
  const sideData = (stream.side_data_list as { rotation?: number }[] | undefined)?.find(
    (entry) => entry.rotation !== undefined,
  );
  return Number(sideData?.rotation ?? stream.tags?.rotate ?? 0) || 0;
}

/**
 * Pure: ffprobe's output -> the dimensions the ladder is built from, as the
 * viewer will see them.
 *
 * Phones record portrait video as landscape frames plus a rotation flag. ffmpeg
 * auto-rotates while encoding, so using the raw frame size here would advertise
 * -- and scale -- a portrait clip as landscape.
 */
export function parseProbe(data: ffmpeg.FfprobeData): ProbeResult {
  // Cover art in an audio file is a "video" stream too; it is not the picture.
  const video = data.streams.find(
    (stream) => stream.codec_type === "video" && !stream.disposition?.attached_pic,
  );
  if (!video?.width || !video.height) {
    throw new CorruptMediaError("Source has no video stream");
  }

  const sideways = Math.abs(rotationOf(video)) % 180 === 90;
  const result = ProbeResultSchema.safeParse({
    width: sideways ? video.height : video.width,
    height: sideways ? video.width : video.height,
    durationInSeconds: Number(data.format.duration ?? video.duration),
  });
  if (!result.success) {
    throw new CorruptMediaError("Source has unusable dimensions or duration", {
      cause: result.error,
    });
  }
  return result.data;
}

export interface HlsSettings {
  outputDir: string;
  segmentSeconds: number;
}

/** Pure: the ffmpeg output options that encode one rung of the ladder as HLS. */
export function buildHlsOutputOptions(
  rendition: Rendition,
  { outputDir, segmentSeconds }: HlsSettings,
): string[] {
  // BANDWIDTH in the master playlist is the peak for audio + video together.
  const videoBitrate = Math.max(rendition.bandwidth - AUDIO_BITRATE, 100_000);

  return [
    // `?` keeps silent sources encodable instead of failing on a missing stream.
    ...["-map", "0:v:0", "-map", "0:a:0?"],
    ...["-vf", `scale=${rendition.width}:${rendition.height}`],
    ...["-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p"],
    // Must match the CODECS string media/hls.ts writes for this rung.
    ...["-profile:v", "main", "-level:v", h264Level(rendition.height)],
    ...[
      "-b:v",
      `${videoBitrate}`,
      "-maxrate",
      `${videoBitrate}`,
      "-bufsize",
      `${videoBitrate * 2}`,
    ],
    // Keyframes on a fixed clock rather than at scene cuts, so every rung
    // starts its segments at the same instants and a player can switch rungs
    // at any segment boundary.
    ...["-force_key_frames", `expr:gte(t,n_forced*${segmentSeconds})`, "-sc_threshold", "0"],
    ...["-c:a", "aac", "-b:a", `${AUDIO_BITRATE}`, "-ac", "2"],
    ...["-f", "hls", "-hls_time", `${segmentSeconds}`, "-hls_playlist_type", "vod"],
    ...["-hls_segment_filename", path.join(outputDir, "seg_%03d.ts")],
  ];
}

/**
 * Aborting `abortSignal` kills ffmpeg outright: exiting Node alone would leave
 * the child encoding on as an orphan.
 */
function run(command: ffmpeg.FfmpegCommand, abortSignal?: AbortSignal): Promise<void> {
  const kill = () => command.kill("SIGKILL");
  abortSignal?.addEventListener("abort", kill, { once: true });

  return new Promise<void>((resolve, reject) => {
    // fluent-ffmpeg's error message already carries the tail of ffmpeg's stderr.
    command
      .on("end", () => resolve())
      .on("error", reject)
      .run();
  }).finally(() => abortSignal?.removeEventListener("abort", kill));
}

export function probeVideo(file: string): Promise<ProbeResult> {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(file, (error: unknown, data) => {
      // ffprobe fails on bytes it cannot demux, and a retry reads the same bytes.
      if (error) {
        reject(new CorruptMediaError("ffprobe could not read the source", { cause: error }));
        return;
      }
      try {
        resolve(parseProbe(data));
      } catch (parseError) {
        reject(parseError);
      }
    });
  });
}

export function extractPoster(
  input: string,
  output: string,
  atSeconds: number,
  abortSignal?: AbortSignal,
): Promise<void> {
  return run(
    ffmpeg(input)
      .seekInput(atSeconds)
      .frames(1)
      // `-update 1` tells the image muxer this is one file, not a numbered sequence.
      .outputOptions(["-q:v", "2", "-update", "1"])
      .output(output),
    abortSignal,
  );
}

export interface TranscodeSettings extends HlsSettings {
  /** Fires on every ffmpeg progress line; callers throttle. */
  onProgress?: (percent: number) => void;
  abortSignal?: AbortSignal;
}

/** Writes `index.m3u8` plus its segments into `outputDir`, which must already exist. */
export function transcodeToHls(
  input: string,
  rendition: Rendition,
  { onProgress, abortSignal, ...settings }: TranscodeSettings,
): Promise<void> {
  const command = ffmpeg(input)
    .outputOptions(buildHlsOutputOptions(rendition, settings))
    .output(path.join(settings.outputDir, "index.m3u8"));

  command.on("progress", ({ percent }) => {
    // Absent or NaN when ffmpeg cannot read the input's duration.
    if (onProgress && percent !== undefined && Number.isFinite(percent)) onProgress(percent);
  });

  return run(command, abortSignal);
}
