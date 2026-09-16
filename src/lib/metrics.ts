import client from "prom-client";

// A custom registry rather than the global one: tests can build a second
// registry without colliding, and nothing leaks in from library defaults.
export const registry = new client.Registry();

// Default metrics (CPU, memory, event-loop lag). Event-loop lag is the one that
// matters most here -- it is how we prove sharp/ffmpeg work stays off the loop.
client.collectDefaultMetrics({ register: registry });

/**
 * Pipeline metrics. Names are prefixed `media_` so they never collide with the
 * RabbitMQ exporter's own series.
 *
 * Queue depth is deliberately absent: RabbitMQ's `rabbitmq_prometheus` plugin
 * already publishes it per queue, and duplicating it here would drift.
 */
export const mediaMetrics = {
  // Ingest (API)
  uploadsReceived: new client.Counter({
    name: "media_uploads_received_total",
    help: "Uploads accepted by the API and published to the broker",
    labelNames: ["type"],
    registers: [registry],
  }),

  uploadBytes: new client.Histogram({
    name: "media_upload_bytes",
    help: "Size distribution of accepted uploads, in bytes",
    buckets: [100_000, 1_000_000, 10_000_000, 50_000_000, 100_000_000, 500_000_000],
    registers: [registry],
  }),

  uploadRejected: new client.Counter({
    name: "media_uploads_rejected_total",
    help: "Uploads refused before publishing",
    labelNames: ["reason"],
    registers: [registry],
  }),

  publishConfirmDuration: new client.Histogram({
    name: "media_publish_confirm_duration_seconds",
    help: "Time awaiting the broker's publisher confirm before replying 202",
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1],
    registers: [registry],
  }),

  // Jobs (worker)
  jobsTotal: new client.Counter({
    name: "media_jobs_total",
    // One per *stage*, not per upload: a video counts its plan and each rung,
    // because that is what a worker settles.
    help: "Job stages finishing, by media type and terminal status",
    labelNames: ["type", "status"],
    registers: [registry],
  }),

  transcodeDuration: new client.Histogram({
    name: "media_transcode_duration_seconds",
    help: "Wall-clock transcode time per job",
    labelNames: ["type", "rendition"],
    // Spans a fast image resize through a multi-minute 1080p encode.
    buckets: [0.5, 1, 2.5, 5, 10, 30, 60, 120, 300],
    registers: [registry],
  }),

  workerBusy: new client.Gauge({
    name: "media_worker_busy",
    help: "Deliveries this worker is processing -- 1 while busy, 0 when idle, at prefetch 1",
    registers: [registry],
  }),

  // Reliability
  retriesTotal: new client.Counter({
    name: "media_retries_total",
    help: "Messages nacked into the retry delay queue",
    labelNames: ["queue"],
    registers: [registry],
  }),

  parkedTotal: new client.Counter({
    name: "media_parked_total",
    help: "Messages routed to the terminal parked queue",
    labelNames: ["queue", "reason"],
    registers: [registry],
  }),
};
