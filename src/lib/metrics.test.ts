import { describe, expect, it } from "vitest";
import { mediaMetrics, registry } from "./metrics.js";

describe("metrics registry", () => {
  it("exposes every pipeline metric under the media_ prefix", async () => {
    const names = (await registry.getMetricsAsJSON()).map((m) => m.name);

    for (const name of [
      "media_uploads_received_total",
      "media_upload_bytes",
      "media_uploads_rejected_total",
      "media_publish_confirm_duration_seconds",
      "media_jobs_total",
      "media_transcode_duration_seconds",
      "media_worker_busy",
      "media_retries_total",
      "media_parked_total",
    ]) {
      expect(names).toContain(name);
    }
  });

  it("collects default metrics, including event-loop lag", async () => {
    const names = (await registry.getMetricsAsJSON()).map((m) => m.name);

    expect(names).toContain("nodejs_eventloop_lag_seconds");
    expect(names).toContain("process_cpu_seconds_total");
  });

  it("records labelled observations", async () => {
    mediaMetrics.jobsTotal.inc({ type: "video", status: "completed" });
    mediaMetrics.transcodeDuration.observe({ type: "video", rendition: "720p" }, 12.5);

    const text = await registry.metrics();

    expect(text).toContain('media_jobs_total{type="video",status="completed"} 1');
    expect(text).toContain('media_transcode_duration_seconds_count{type="video",rendition="720p"}');
  });
});
