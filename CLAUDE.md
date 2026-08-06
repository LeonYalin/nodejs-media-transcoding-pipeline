# Distributed Media Transcoding Pipeline (RabbitMQ + MinIO + ffmpeg)

A **Fastify API** streams uploads straight into **MinIO** and returns `202 Accepted` → a durable **RabbitMQ** job is published with publisher confirms → N **worker containers** transcode images with `sharp` and video into an **HLS ladder** with `ffmpeg`, writing derivatives back to MinIO. **Redis** holds job state and feeds live progress to the browser over SSE. Prometheus + Grafana + Jaeger for observability.

> Status: greenfield. Full build order & design → [IMPLEMENTATION.md](IMPLEMENTATION.md).

## Data flow
`POST /uploads → (stream) MinIO media-uploads → confirm-publish to media.jobs → q.image | q.video.plan → fan-out q.video.rendition → sharp/ffmpeg → MinIO media-outputs → Redis + SSE`
Failures → `media.retry` → `q.retry` (TTL) → back to the work queue; after `MAX_ATTEMPTS` → **`q.parked`** (terminal), job marked failed.

## Where things run
- **Host:** the API (`tsx`), unit tests, scripts. **ffmpeg is never installed on the host.**
- **Docker:** RabbitMQ, MinIO, Redis, RedisInsight, Prometheus, Grafana, Jaeger, **and the workers** — scaled as compose replicas. ffmpeg/ffprobe live only inside the worker image.

## Component map
- `src/api/` — `app.ts` (`createApp`, no listen), `index.ts` (entrypoint), `routes/{uploads,jobs,health}.ts`, `sse.ts`.
- `src/worker/` — `index.ts` (composition root + signals), `consumer.ts` (channel/prefetch/ack orchestration), `retry.ts` (pure retry-vs-park), `workspace.ts` (temp dirs), `handlers/{image,video-plan,video-rendition}.ts`.
- `src/media/` — pure/near-pure media core: `ladder.ts` (rendition selection), `hls.ts` (master playlist), `ffmpeg.ts`, `images.ts`.
- `src/lib/` — `topology.ts` (the AMQP definition), `amqp`, `s3`, `object-store` (the only S3 caller), `redis`, `job-store` (the only Redis writer), `logger`, `metrics`, `metrics-server`, `tracing`.
- `src/config/` — env → zod-validated typed config (the only place that reads `process.env`).
- `src/domain/` — `job.ts` (wire + record schemas), `media.ts` (MIME allowlist, error classes).
- `tests/integration/` — testcontainers suite; unit tests sit beside their source as `*.test.ts`.

## Conventions (non-negotiable)
- TypeScript strict, ESM. Env only via `src/config`; logs only via `src/lib/logger`; metrics only via `src/lib/metrics`; S3 only via `src/lib/object-store`; Redis writes only via `src/lib/job-store`; AMQP topology only from `src/lib/topology`.
- **Messaging invariants:** publish `persistent` on a **confirm channel** and `waitForConfirms()` *before* replying `202` — never acknowledge a client for a job the broker hasn't accepted. `ack` **only after** the derivatives are uploaded and Redis is updated. `prefetch(1, true)` — channel-global, so one container = one job in flight. Failures `nack(requeue:false)` into the retry path; non-retryable errors and `MAX_ATTEMPTS` go straight to `q.parked`, never a poison-message loop.
- **Retry counting:** read the `x-death` entry matching *this* work queue with `reason: rejected` — not `x-death[0]`, which may be the `q.retry`/`expired` entry.
- **Memory:** never buffer a media file. Uploads stream request→MinIO; image transcodes stream MinIO→sharp→MinIO. Only video uses a temp dir (ffmpeg needs a seekable file), always removed in `finally`, including on the park path.
- **Idempotency:** output keys are deterministic from `jobId` (+ rendition) — at-least-once redelivery must overwrite, never duplicate.
- **Shutdown:** SIGTERM → cancel consumers, finish the in-flight job, ack, close, flush traces.
- **Dependency injection:** modules export `createX(deps)` factories; entrypoints are the only place that builds real clients and the only place with import-time side effects (guarded by `import.meta.url === pathToFileURL(process.argv[1]).href`). Config is the one deliberate singleton.
- **Tests:** no module mocking (`vi.mock` must stay at zero) — inject a fake, or use a real container. Fakes only for what a real dependency can't do (failure injection, timer control).
- Env vars load via Node `--env-file=.env` (no `dotenv`).

## How to run / inspect
- Operational commands (build the worker image, infra up/down, scaling workers, dev API, load test, one-off ffprobe, every UI/port) → **`run-pipeline` skill**.
- Queue/storage/job diagnostics → **`queue-ops` skill**.
- End-to-end verification → **`transcode-verifier` agent**. Ladder/HLS/encoder design → **`ffmpeg-expert` agent**. Reviewing new TS against the messaging invariants → **`queue-reliability-reviewer` agent**. Structure/tooling/house-style audit after each build step → **`project-standards-reviewer` agent**.
- Interactive Grafana/Redis access → MCP servers in `.mcp.json` (Docker-based, project-scoped). Tool schemas load on demand via Claude Code's tool search, so they add negligible context per turn. RabbitMQ and MinIO have no published MCP server — use the `queue-ops` skill.

Don't restate commands here — those skills are the single source.
