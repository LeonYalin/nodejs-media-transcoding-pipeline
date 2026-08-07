# Distributed Media Transcoding Pipeline (RabbitMQ + MinIO + ffmpeg) — Implementation Plan

> This document is the executable build spec. The Claude Code config files
> (`CLAUDE.md`, `.claude/`, `.mcp.json`) described in Step 14 already exist in the
> repo; everything else here is still to be implemented.

## Context

The repo starts essentially empty (only `README.md` + `.gitignore`). The goal is to master **asynchronous job queuing**, **heavy multi-core CPU background work**, and **S3-compatible object storage** — the production problem of accepting an upload in milliseconds while the actual transcode takes minutes, without losing the job when a process dies and without ever holding a media file in RAM.

This plan builds that as a **small but production-shaped learning app**: a Fastify API that streams uploads straight into MinIO and returns `202 Accepted`, a durable RabbitMQ topology with manual acks, bounded retry and a dead-letter path, and a fleet of containerized workers that transcode images with `sharp` and video into an **HLS ladder** with `ffmpeg`. Everything runs locally on free, open-source software via `docker compose`.

### Decisions locked in
- **Language:** TypeScript (strict), ESM, `tsx` for dev, `zod` for runtime validation.
- **HTTP:** Fastify 5 (deliberately different from the previous project's Express).
- **The host stays clean.** `ffmpeg`/`ffprobe` are **never installed on the machine** — they are baked into the worker image from a static build. Host prerequisites are Docker and Node, nothing else.
- **Multi-core model:** N **worker containers** (compose replicas), each with one AMQP channel at channel-global `prefetch(1, true)` → exactly one job in flight per worker. Scaling is `--scale worker=N`, not threads.
- **Memory model:** uploads stream request → MinIO via `@aws-sdk/lib-storage`; image transcodes stream MinIO → `sharp` → MinIO. Only video touches disk (ffmpeg needs a seekable file), in a temp dir removed in `finally`.
- **Durability:** durable exchanges/queues, `persistent` messages, and a **confirm channel** — the API does not return `202` until the broker has confirmed the job.
- **Reliability:** manual `ack` after success only; failures `nack(requeue:false)` into a **TTL delay queue** that dead-letters back to the work queue; after `MAX_ATTEMPTS` (read from the `x-death` header) the message is **parked** in a terminal DLQ and the job is marked failed.
- **Job state:** Redis — job records + progress, with pub/sub feeding **SSE** to the browser.
- **Observability:** pino logs, `prom-client` → Prometheus → Grafana, **plus OpenTelemetry traces → Jaeger** so a single trace spans API → RabbitMQ → worker. Every datastore also gets a browser UI.
- **Code structure:** modules export `createX(deps)` factories with structurally-typed dependencies; each process entrypoint is the composition root and the only place with import-time side effects. Config is the one deliberate singleton. No DI container.
- **Testing:** Vitest unit tests (pure modules + injected fakes) + a testcontainers integration suite (RabbitMQ + MinIO + Redis + the built worker image). No module mocking.
- **Env loading:** no `dotenv` — Node's built-in `--env-file=.env`, then zod-validated in `src/config`.
- **Included this time (deferred in the previous project):** ESLint + Prettier and a GitHub Actions CI workflow.

---

## Architecture

```
                         ┌───────────────────────────────────────────────┐
  POST /uploads          │ Fastify API  (host, tsx)                      │
 (multipart stream) ────▶│ stream→MinIO (lib-storage Upload, no buffer)  │
                         │ Redis job record (queued)                     │
                         │ publish (confirm channel, persistent) ──┐     │
                         │ 202 Accepted { jobId }                  │     │
                         └────────┬──────────────────────────────┬─┘     │
                          GET /jobs/:id/events (SSE)             │
                                  ▲                              ▼
                          Redis pub/sub                   exchange media.jobs (topic)
                                  ▲                  ┌───────────┼───────────┐
                                  │            job.image.*  job.video.plan  job.video.rendition
                                  │                  │           │           │
                                  │               q.image   q.video.plan  q.video.rendition
                                  │                  └───────────┴───────────┘
                                  │                              │  docker compose --scale worker=N
                                  │                              │  each: 1 channel, prefetch(1, true)
                                  │                              ▼
                                  │        ┌──────────────────────────────────────┐
                                  └────────│ Worker container (node + static      │──▶ MinIO
                                  progress │ ffmpeg): download → sharp/ffmpeg →   │   media-outputs
                                           │ upload derivatives → ack             │
                                           └───────┬──────────────────────────────┘
                                    fail → nack(requeue:false) → media.retry
                                              → q.retry (x-message-ttl 10s, no consumers)
                                              → dead-letters back to media.jobs (orig. routing key)
                                    x-death count >= MAX_ATTEMPTS → media.parked → q.parked (terminal)

Video fan-out: q.video.plan (ffprobe, fast) emits 1..3 rendition jobs + Redis renditionsExpected=N.
Barrier:      each rendition HINCRBYs renditionsDone; whoever reaches N writes master.m3u8.

Infra (docker compose): RabbitMQ(+management,+prometheus), MinIO, Redis, RedisInsight,
Prometheus, Grafana, Jaeger, worker×N.   Host (npm): api, scripts, tests.
```

---

## Tech Stack

| Concern | Choice |
|---|---|
| Runtime | Node 24, TypeScript (strict), ESM, `tsx` for dev |
| HTTP / API | `fastify@5` + `@fastify/multipart` + `@fastify/static` |
| Queue client | `amqplib@2` (promise API is the root export; **ships its own types — no `@types/amqplib`**) |
| Object storage | `@aws-sdk/client-s3` + `@aws-sdk/lib-storage` against MinIO (`forcePathStyle: true`) |
| Image transcode | `sharp` |
| Video transcode | `fluent-ffmpeg` driving the **static ffmpeg/ffprobe inside the worker image** |
| Job state | `ioredis` |
| Validation | `zod` |
| Logging | `pino` (+ `pino-pretty` in dev) |
| Metrics | `prom-client` |
| Tracing | `@opentelemetry/sdk-node` + http/fastify/amqplib/ioredis/aws-sdk instrumentations → OTLP/HTTP → Jaeger |
| Load testing | `autocannon` |
| Testing | `vitest` (unit) + `testcontainers` (RabbitMQ, MinIO, Redis, worker image) |
| Lint / format | `eslint` + `prettier` |
| Env | Node built-in `--env-file=.env` (no `dotenv`), values zod-validated |
| Infra | `docker compose`: RabbitMQ, MinIO, Redis, RedisInsight, Prometheus, Grafana, Jaeger, worker replicas |

### Pinned images (all verified to exist, multi-arch incl. arm64)
| Image | Why this tag |
|---|---|
| `node:24-bookworm-slim` | worker base, matches the host Node major |
| `mwader/static-ffmpeg:8.1.2` | ffmpeg + ffprobe copied into the worker image — keeps the host clean |
| `rabbitmq:4.3-management-alpine` | management UI + `rabbitmq_prometheus` plugin |
| `minio/minio:RELEASE.2025-04-22T22-12-26Z` | **pinned deliberately**: later community releases removed the web console |
| `redis:7-alpine` + `redis/redisinsight:3.8` | Redis and its official free browser UI |
| `prom/prometheus`, `grafana/grafana`, `jaegertracing/all-in-one` | standard |

---

## Project Structure

```
.
├── docker-compose.yml · Dockerfile.worker · .dockerignore
├── .env.example · Makefile · package.json · tsconfig.json
├── vitest.config.ts · eslint.config.js · .prettierrc.json
├── .github/workflows/ci.yml            # lint + typecheck + unit + worker image build
├── rabbitmq/enabled_plugins            # rabbitmq_management, rabbitmq_prometheus
├── prometheus/prometheus.yml           # api (host.docker.internal), worker (dns_sd), rabbitmq
├── grafana/provisioning/…              # datasource + dashboards/{pipeline,runtime}.json
├── public/index.html                   # upload form + job table + SSE progress + hls.js player
├── scripts/
│   ├── infra-init.ts                   # assert MinIO buckets + AMQP topology; verify reachability
│   └── load.ts                         # autocannon: concurrent multipart uploads, p99 + 202 rate
├── tests/integration/                  # testcontainers: rabbitmq + minio + redis + built worker image
└── src/
    ├── config/index.ts                 # env → zod → typed config singleton (only reader of process.env)
    ├── domain/
    │   ├── job.ts                      # zod: JobMessage (wire), JobRecord (Redis), JobStatus
    │   └── media.ts                    # zod: mime allowlist, Rendition, ProbeResult; error classes
    ├── lib/
    │   ├── tracing.ts                  # OTel NodeSDK bootstrap — imported first in each entrypoint
    │   ├── logger.ts · metrics.ts · metrics-server.ts
    │   ├── amqp.ts                     # connection/channel factories + reconnect
    │   ├── topology.ts                 # THE single definition of exchanges/queues/bindings/args
    │   ├── s3.ts                       # S3Client for MinIO (forcePathStyle, static creds)
    │   ├── object-store.ts             # the only place that calls S3 (putStream/getStream/putDir/…)
    │   ├── redis.ts
    │   └── job-store.ts                # the only place that writes Redis (record + progress publish)
    ├── media/                          # PURE / near-pure, unit-tested without infra
    │   ├── ladder.ts                   # sourceHeight → rendition list (never upscale)
    │   ├── hls.ts                      # master playlist generation
    │   ├── ffmpeg.ts                   # fluent-ffmpeg wrapper: probe(), toHls(), poster(), progress
    │   └── images.ts                   # sharp pipeline builders
    ├── api/
    │   ├── app.ts                      # createApp(deps) — plugins, routes, no listen()
    │   ├── index.ts                    # composition root (tracing→config→clients→listen→signals)
    │   ├── routes/{uploads,jobs,health}.ts
    │   └── sse.ts                      # SSE stream helper (heartbeat + cleanup on close)
    └── worker/
        ├── index.ts                    # composition root; signals; graceful drain
        ├── consumer.ts                 # channel, prefetch(1,true), 3 consumers, ack/nack orchestration
        ├── retry.ts                    # PURE: x-death → retry | park decision
        ├── workspace.ts                # temp-dir lifecycle (mkdtemp / rm -rf in finally)
        └── handlers/{image,video-plan,video-rendition}.ts
```

Plus Claude Code config (already created; see Step 14):
```
├── CLAUDE.md                           # canonical project context (single source of truth)
├── .mcp.json                           # Grafana + Redis MCP servers (Docker-based, project-scoped)
└── .claude/
    ├── settings.json                   # permission allowlist + enabledMcpjsonServers
    ├── skills/
    │   ├── run-pipeline/SKILL.md       # operational commands (the only home for them)
    │   └── queue-ops/SKILL.md          # RabbitMQ / MinIO / Redis diagnostics
    └── agents/
        ├── transcode-verifier.md       # e2e verification subagent
        ├── queue-reliability-reviewer.md # reviews new TS against the messaging invariants
        └── ffmpeg-expert.md            # ladder / HLS / encoder-flag design
```

Single TypeScript package, two entrypoints — `api` (runs on the host) and `worker` (runs in the image) — sharing `config`/`lib`/`domain`/`media`.

---

## Execution model

**Host runs:** the Fastify API under `tsx` (fast reload, and the target `autocannon` hammers), unit tests, and the helper scripts. No ffmpeg, no system packages.

**Docker runs:** RabbitMQ, MinIO, Redis, RedisInsight, Prometheus, Grafana, Jaeger — **and the workers**, as scalable compose replicas.

Consequences:
- The worker service bind-mounts **only** `./src` plus `package.json` and `tsconfig.json` (read-only) — deliberately *not* the project root. `/app/node_modules`, installed by `npm ci` at image build, therefore stays the container's own Linux build. (The host's `npm install` produces a macOS `sharp` binary in `./node_modules`; mounting the root would shadow the Linux one and break the worker. Narrow mounts avoid the problem instead of patching around it with an anonymous volume.)
- Compose supplies service-name hosts to in-container processes (`amqp://rabbitmq:5672`, `http://minio:9000`, `redis://redis:6379`); `.env` keeps `localhost` for the host-run API. One `src/config` schema serves both.
- Every worker exposes metrics on the **same** port inside its own network namespace, so Prometheus discovers replicas with `dns_sd_configs` against the `worker` service name (Docker's DNS returns every replica IP). One `WORKER_METRICS_PORT`, no port-base arithmetic.
- `stop_grace_period: 60s` on the worker service so SIGTERM can finish an in-flight transcode before Docker escalates to SIGKILL.

---

## Data contracts

**Object keys**
```
media-uploads/{jobId}/source{ext}
media-outputs/{jobId}/image/{full.webp, thumb.webp}
media-outputs/{jobId}/poster.jpg
media-outputs/{jobId}/hls/{height}p/{index.m3u8, seg_000.ts, …}
media-outputs/{jobId}/hls/master.m3u8
```
Keys are **deterministic** — a redelivered job overwrites its own outputs, which is what makes at-least-once delivery safe here.

**Redis** — `job:{id}` hash (`status`, `type`, `sourceKey`, `mime`, `bytes`, `createdAt`, `updatedAt`, `progress`, `error`, `outputs`, `renditionsExpected`, `renditionsDone`), TTL `JOB_TTL_SECONDS`; `jobs:recent` sorted set for the UI; pub/sub channel `job:{id}:events`.

**Routing keys** — `job.image.transform`, `job.video.plan`, `job.video.rendition`.

---

## Implementation Steps

### 1. Scaffolding & config — DONE
- `package.json` (ESM, `type: module`), `tsconfig.json` (strict, `moduleResolution: NodeNext`).
- **Note:** TypeScript is pinned to `^5` — `typescript-eslint@8` declares `typescript >=4.8.4 <6.1.0`, so TS 7 would break linting. (The ETL repo could run TS 7 only because it deferred ESLint.)
- Install deps: `fastify @fastify/multipart @fastify/static amqplib @aws-sdk/client-s3 @aws-sdk/lib-storage sharp fluent-ffmpeg ioredis zod pino prom-client @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http @opentelemetry/instrumentation-{http,fastify,amqplib,ioredis,aws-sdk}`; dev: `tsx typescript @types/node @types/fluent-ffmpeg pino-pretty vitest testcontainers @testcontainers/rabbitmq autocannon eslint prettier concurrently`.
- npm scripts pass `--env-file=.env` to `tsx` (no `dotenv`). `eslint.config.js` (flat config) + `.prettierrc.json`. `Makefile` mirrors the npm scripts.

### 2. Config (`src/config/index.ts`) — DONE
- Read `process.env`, validate with zod, export typed `config` + a pure `loadConfig(env)` that **throws** rather than exiting (so tests can assert on it).
- Keys: `AMQP_URL`, `AMQP_PREFETCH`, `RETRY_TTL_MS`, `MAX_ATTEMPTS`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `BUCKET_UPLOADS`, `BUCKET_OUTPUTS`, `REDIS_URL`, `API_PORT`, `MAX_UPLOAD_BYTES`, `WORKER_METRICS_PORT`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `JOB_TTL_SECONDS`, `HLS_SEGMENT_SECONDS`, `NODE_ENV`. Mirror every key in `.env.example`.
- `src/lib/logger.ts` (pino, silent under `NODE_ENV=test`), `src/lib/metrics.ts` (own Registry + `collectDefaultMetrics`), `src/lib/metrics-server.ts` (workers only — the API serves `/metrics` through Fastify).
- Queue depth is **not** a `media_*` metric: RabbitMQ's `rabbitmq_prometheus` plugin already publishes it per queue, and a second source would drift.

### 3. Domain + media core (pure first)
- `domain/job.ts`: `JobMessageSchema` (the wire contract shared by API and worker), `JobRecordSchema`, `JobStatus` = `queued|processing|completed|failed`.
- `domain/media.ts`: MIME allowlist (`image/{jpeg,png,webp,avif}`, `video/{mp4,quicktime,webm,x-matroska}`), `Rendition`, `ProbeResult`, and the **error classes that drive retry-vs-park**: `UnsupportedMediaError`, `CorruptMediaError`, `ObjectNotFoundError` (all `retryable = false`); everything else defaults to retryable.
- `media/ladder.ts` — pure `buildLadder(sourceHeight)`: from `[1080p 5000k, 720p 2800k, 360p 800k]`, keep renditions whose height ≤ source height, **never upscale**; if the source is smaller than the smallest rung, emit a single source-height rendition.
- `media/hls.ts` — pure `buildMasterPlaylist(renditions)` → `#EXT-X-STREAM-INF:BANDWIDTH=…,RESOLUTION=…,CODECS="avc1.4d401f,mp4a.40.2"` + relative variant paths.
- Write these two with their unit tests **before** any infra exists — they need none.

### 4. Infra (`docker-compose.yml`)
- **RabbitMQ** `4.3-management-alpine`, `rabbitmq/enabled_plugins` mounted to enable `rabbitmq_management` + `rabbitmq_prometheus`; ports 5672 (AMQP), 15672 (UI), 15692 (metrics); healthcheck `rabbitmq-diagnostics -q ping`.
- **MinIO** on the pinned console-bearing release; ports 9000 (S3) / 9001 (console); a short-lived `mc` bootstrap service creates `media-uploads` + `media-outputs`; healthcheck on `/minio/health/live`.
- **Redis** `7-alpine` with `--appendonly yes`; healthcheck `redis-cli ping`. **RedisInsight** `3.8` alongside it on port 5540 for browsing job hashes and watching pub/sub.
- **Prometheus** (mounted config), **Grafana** (provisioned datasource + both dashboards, host port 3001 → 3000 because the API owns 3000), **Jaeger all-in-one** (OTLP/HTTP 4318, UI 16686).
- **worker**: built from `Dockerfile.worker`, `depends_on` healthy rabbitmq/minio/redis, `stop_grace_period: 60s`, no fixed `container_name` (it must be scalable), service-name env overrides, narrow bind-mounts per the execution model.
- Named network `media_pipeline_net` so MCP containers can join by name.

### 5. Worker image (`Dockerfile.worker`)
- `FROM node:24-bookworm-slim`; `COPY --from=mwader/static-ffmpeg:8.1.2 /ffmpeg /ffprobe /usr/local/bin/`; `npm ci`; run as a non-root user; entry `npx tsx watch src/worker/index.ts`.
- `.dockerignore`: `node_modules`, `.git`, `tmp`, `docs`.
- Acceptance check for this step: inside the image, `ffmpeg -version` and `ffprobe -version` succeed and `node -e "require('sharp')"` loads.
- *(Fallback only if the static build ever misbehaves: install ffmpeg from Debian packages in the image instead. The host is never touched either way.)*

### 6. AMQP topology (`src/lib/topology.ts`)
The single definition of every exchange, queue, binding and argument — asserted idempotently by both entrypoints and by `scripts/infra-init.ts`.

| Object | Type | Args |
|---|---|---|
| `media.jobs` | topic exchange, durable | — |
| `q.image` | durable queue | `x-dead-letter-exchange: media.retry` |
| `q.video.plan` | durable queue | `x-dead-letter-exchange: media.retry` |
| `q.video.rendition` | durable queue | `x-dead-letter-exchange: media.retry` |
| `media.retry` | topic exchange, durable | — |
| `q.retry` | durable, **no consumers**, bound `#` | `x-message-ttl: RETRY_TTL_MS`, `x-dead-letter-exchange: media.jobs` |
| `media.parked` | topic exchange, durable | — |
| `q.parked` | durable, bound `#` | — (terminal; drained by hand) |

Two subtleties to encode in comments, because they are the actual lesson:
- Dead-lettering **preserves the original routing key**, so a message expiring out of `q.retry` re-enters `media.jobs` and lands back on the queue it came from. No per-queue retry queues needed.
- A **uniform** per-queue TTL avoids the classic delay-queue trap: with per-*message* TTLs, a message with a long TTL at the head blocks shorter-TTL messages behind it, because RabbitMQ only expires from the head.

### 7. API (`src/api/`)
- `app.ts` exports `createApp(deps)` — registers `@fastify/multipart` (with `limits.fileSize = MAX_UPLOAD_BYTES`), `@fastify/static` for `public/`, routes, and a central error handler. **No `listen()`** — so tests can drive it directly.
- `POST /uploads`: take `req.file()`, validate the MIME against the allowlist, pipe `file` straight into `@aws-sdk/lib-storage` `Upload` targeting `media-uploads/{jobId}/source{ext}` — the bytes never accumulate in RAM. After the upload resolves, **check `file.truncated`**; if the size limit was hit, delete the partial object and return `413`.
- Then: write the Redis job record (`queued`), publish `JobMessage` on a **confirm channel** with `persistent: true`, `await waitForConfirms()`, and only then reply `202 { jobId, statusUrl, eventsUrl }`. If the confirm fails, mark the job failed and return `503` — never a `202` for a job the broker did not accept.
- `GET /jobs/:id` (record), `GET /jobs` (recent, for the table), `GET /jobs/:id/events` (SSE), `GET /healthz` (broker + bucket + redis reachability), `GET /metrics`.
- `sse.ts`: set the SSE headers, subscribe a **dedicated** ioredis connection to `job:{id}:events` (subscriber-mode connections can't run normal commands), emit a comment heartbeat every 15 s, and unsubscribe + quit on `req.raw.on('close')`.

### 8. Worker consumer (`src/worker/consumer.ts`)
- One connection and one channel per container; `await ch.prefetch(1, true)` — the `global` flag makes it **channel-wide**, so the three consumers share a single in-flight slot and one container = one job at a time.
- Three consumers (`q.image`, `q.video.plan`, `q.video.rendition`), `noAck: false`. Per message: zod-parse → dispatch to the handler → `ch.ack(msg)` **only after full success** (derivatives uploaded, Redis updated).
- On throw: `retry.ts` decides, then either `ch.nack(msg, false, false)` (→ `media.retry` → delay → back) or publish to `media.parked` + `ch.ack` (terminal) + Redis `failed` with the reason.
- `retry.ts` is **pure and heavily unit-tested**:
  - `attempts` = the `count` of the `x-death` entry whose `queue` is *this* work queue and whose `reason` is `rejected` (0 when the header is absent). Selecting the right entry matters — the array accumulates entries for `q.retry` (`reason: expired`) too, and naively reading `x-death[0].count` gives the wrong number.
  - `error.retryable === false` → park. `attempts + 1 >= MAX_ATTEMPTS` → park (`max-attempts`). Otherwise → retry.

### 9. Handlers (`src/worker/handlers/`)
- **`image.ts`** — `GetObject` body → `sharp` → `Upload`, **fully streaming, no temp file**: one pipeline for `full.webp` (max 1920 wide, `withoutEnlargement`), one for `thumb.webp` (320 wide). `sharp` releases the event loop to libvips' thread pool, so this stays non-blocking.
- **`video-plan.ts`** — download the source into a workspace, `ffprobe` it, `buildLadder(height)`, extract `poster.jpg` at 1 s, write `renditionsExpected` to Redis, then publish one `job.video.rendition` message per rung (persistent, on a confirm channel) and ack. Fast job, deliberately separated from the heavy one.
- **`video-rendition.ts`** — download the source, run ffmpeg → HLS (`-c:v libx264 -preset veryfast -c:a aac -hls_time HLS_SEGMENT_SECONDS -hls_playlist_type vod`) into the workspace, forward `.on('progress')` **throttled to ~1/s** into `job-store` (Redis hash + pub/sub → SSE), upload the segments + variant playlist, then `HINCRBY renditionsDone 1`; the worker whose increment returns `renditionsExpected` writes `master.m3u8` via `buildMasterPlaylist()` and marks the job `completed`. The `HINCRBY` return value is the atomic barrier — no locks.
- **`workspace.ts`** — `mkdtemp` per job, `rm -rf` in `finally`, always, including on the park path.
- The **streaming (image) vs. temp-file (video)** split is deliberate and documented: ffmpeg needs a seekable input and writes many segment files, so disk is the correct answer there; anything that *can* stream, must.

### 10. Observability
- `prom-client`: `media_jobs_total{type,status}`, `media_transcode_duration_seconds{type,rendition}`, `media_upload_bytes`, `media_retries_total`, `media_parked_total{reason}`, `media_worker_busy`, plus default metrics (incl. event-loop lag).
- `prometheus.yml` scrapes: the host API via `host.docker.internal`, RabbitMQ's own `:15692/metrics`, and worker replicas via `dns_sd_configs: [{ names: [worker], type: A, port: <WORKER_METRICS_PORT> }]`.
- **Two Grafana dashboards, not one** — split by the question each answers, which is the standard overview→drill-down pattern:
  - **`pipeline.json` — "is the pipeline keeping up?"** Queue depth per queue, jobs/min by status, transcode duration p50/p95 by rendition, retry + parked rate, worker busy ratio, upload throughput and `202` latency.
  - **`runtime.json` — "why is it slow, and is any process unhealthy?"** Per-instance event-loop lag (p99), heap vs. RSS, GC pause time, CPU, active handles/requests, process uptime/restarts — all from `collectDefaultMetrics`.
- The overview additionally carries a **compact "runtime health" row** with exactly two default metrics: **event-loop lag p99** and **RSS**. This is deliberate, not duplication — those two are the direct evidence for the project's two central claims ("we never block the event loop", "we never buffer a media file"). Everything else stays on the runtime dashboard.
- Wire them together: a shared `instance` template variable plus Grafana **dashboard links / data links** on the runtime row, so clicking a lag spike lands on that instance's panels. Both dashboards are auto-provisioned from `grafana/provisioning/dashboards/`.
- **Aggregation rule for scaled workers:** default metrics are per-instance, so `sum()` across replicas hides a single sick one. Use `max by (instance)` / per-instance series for lag, heap and RSS; reserve `sum()` for genuinely additive work counters. The `job` label separates the host API from the worker replicas.
- Use `nodejs_eventloop_lag_p99_seconds` rather than the mean `nodejs_eventloop_lag_seconds` — a blocked loop shows up in the tail long before the average moves.
- `lib/tracing.ts`: `NodeSDK` with the http/fastify/amqplib/ioredis/aws-sdk instrumentations and an OTLP/HTTP exporter → Jaeger. **Imported first** in both entrypoints (before any instrumented library). Acceptance check: one Jaeger trace contains the API span *and* the worker's ffmpeg span — proving the amqplib instrumentation propagated context through the message headers.

### 11. Reliability
- **Graceful shutdown:** SIGTERM → `ch.cancel(consumerTag)` for all three consumers (stop new deliveries), await the in-flight job, ack it, close channel + connection, flush the tracer, exit 0. A second signal kills the ffmpeg child immediately.
- **No-loss proof:** kill a worker mid-transcode; the unacked message is redelivered when the connection drops, another replica picks it up, deterministic keys make the rewrite harmless.
- **Idempotency:** output keys derive from `jobId` + rendition, so redelivery overwrites rather than duplicating; the barrier uses absolute state where it can and `HINCRBY` only once per rendition completion.

### 12. Developer ergonomics + UI
- npm scripts: `up`, `up:workers` (`--scale worker=N`), `down`, `build:worker`, `infra:init`, `dev:api`, `logs:worker`, `load`, `lint`, `format`, `typecheck`, `test`, `test:watch`, `test:integration`. `Makefile` mirrors them.
- `scripts/infra-init.ts`: assert MinIO buckets + the full AMQP topology, then verify reachability of RabbitMQ, MinIO and Redis — the one command to run after `up`.
- `scripts/load.ts`: `autocannon` firing concurrent multipart uploads of a fixture, reporting p99 latency and the `202` rate — the proof that ingest stays fast while workers churn.
- `public/index.html`: vanilla JS — upload form, job table polled from `GET /jobs`, per-job progress bars driven by SSE, and an `hls.js` player pointed at the finished `master.m3u8`. No framework, no build step.

### 13. Tests
- **Unit** (`*.test.ts` beside the source, no infra): `media/ladder` (no upscaling, tiny sources, exact rungs), `media/hls` (playlist text), `worker/retry` (the full `x-death` matrix: absent header, first rejection, mixed `rejected`/`expired` entries, non-retryable error, max attempts), `domain/*` schemas, `config` validation, and each handler against injected fake object-store/job-store/ffmpeg.
- **Integration** (`tests/integration/`): boot RabbitMQ + MinIO + Redis containers and build the **worker image** via `GenericContainer.fromDockerfile` (cached between runs). Drive `createApp()` with a real upload, then assert: derivatives land in `media-outputs`; the HLS master playlist references every expected rung; a handler forced to fail increments `x-death` and reappears after the TTL; it parks in `q.parked` after `MAX_ATTEMPTS`; and SIGTERM mid-transcode leads to redelivery with no loss.
- **No module mocking** — `vi.mock` stays at zero. Inject a fake, or use a real container. Fakes only for what a real dependency can't do on cue (failure injection, timer control).

### 14. Claude Code configuration (DRY, token-lean) — DONE

**Guiding principle:** one fact lives in exactly one place. `CLAUDE.md` holds durable context; skills hold procedures/reference; agents orchestrate by *pointing to* skills. No prose is duplicated across files, and each file stays terse (bullets/tables/commands, not paragraphs) to minimize tokens loaded per turn.

- **`CLAUDE.md` (root)** — the single source of truth, deliberately compact: summary, data-flow line, component map, conventions/invariants, and pointers to the skills. Commands are *not* copied here.
- **`.claude/settings.json`** — permission allowlist for this project's safe, frequent commands + `enabledMcpjsonServers`. No secrets.
- **`.claude/skills/run-pipeline/SKILL.md`** — the *only* home for operational commands (build the worker image, infra up/down, scale workers, `infra:init`, dev API, load test, one-off `ffprobe` in a container, every UI/port).
- **`.claude/skills/queue-ops/SKILL.md`** — the *only* home for diagnostics (queue depths, inspecting `x-death` on parked messages, replaying `q.parked`, `mc` on both buckets, `redis-cli` job records).
- **`.claude/agents/transcode-verifier.md`** — e2e verification subagent; orchestration only, invokes the two skills, zero duplicated commands.
- **`.claude/agents/queue-reliability-reviewer.md`** — reviews new/changed TS against this app's messaging invariants (ack placement, confirms, prefetch, streaming vs. buffering, temp-dir cleanup, shutdown, retry-vs-park, module boundaries). Does not restate general TS/Fastify style.
- **`.claude/agents/ffmpeg-expert.md`** — ladder/HLS/encoder-flag design and transcode performance; defers detail to `src/media/*` and the skills.
- **`.claude/agents/project-standards-reviewer.md`** — audits structure, module boundaries, house style and tooling hygiene against this spec after every build step. Complements (does not overlap) the reliability reviewer.
- **`.mcp.json`** — Grafana + Redis MCP servers (Docker-based, project-scoped). There is **no** published RabbitMQ or MinIO/S3 MCP server, so those are covered by the `queue-ops` skill via their CLIs and HTTP APIs rather than a guessed image. Tool schemas load on demand via Claude Code's tool search, so they add negligible per-turn cost.

This structure is the token-saving lesson: `CLAUDE.md` loads every turn so it stays small; heavier procedural detail lives in skills that load **on demand**; agents run in isolated context and pull only the skill(s) they need.

---

## Best practices demonstrated (learning goals)
- **RabbitMQ:** durable topology, persistent messages, **publisher confirms before acknowledging the client**, manual `ack` after side effects, channel-global `prefetch(1)` as the unit of concurrency, DLX-based **bounded retry via a TTL delay queue**, `x-death` introspection, terminal parking, topic-exchange **fan-out with an atomic barrier**.
- **Object storage / S3:** path-style addressing for MinIO, **multipart streaming uploads** with `lib-storage`, streaming reads, deterministic keys for idempotency, separate raw/derivative buckets.
- **Node:** never hold a media file in memory, never block the event loop (native work in libvips threads and ffmpeg child processes), backpressure via `prefetch`, temp-dir lifecycles that survive failure paths, graceful drain on SIGTERM, typed config + runtime validation, structured logging, first-class metrics and traces.
- **Scaling:** horizontal worker replicas as the multi-core strategy, with DNS-based scrape discovery so observability scales with them.

---

## Verification (end-to-end)

0. `npm run lint`, `npm run typecheck`, `npm test` pass; `npm run test:integration` boots the containers and the upload→transcode→retry→park assertions pass.
1. Bring up infra and workers, confirm every container is healthy and all UIs are reachable (RabbitMQ management, MinIO console, RedisInsight, Prometheus, Grafana, Jaeger).
2. `infra:init` → both buckets exist and every exchange/queue/binding from Step 6 is present.
3. Start the API. Upload an image → `202` returns in milliseconds; `media-outputs/{jobId}/image/` gains `full.webp` + `thumb.webp` within seconds.
4. Upload a 1080p video → the plan job fans out three rendition jobs; the UI shows three progress bars advancing in parallel across replicas; `master.m3u8` appears when the last one finishes and plays in the browser.
5. Upload a source smaller than 720p → the ladder contains no upscaled rungs.
6. Upload a deliberately corrupt file → it retries the configured number of times (visible as `q.retry` depth oscillating), then lands in `q.parked` with the reason in its headers and the job marked `failed`.
7. Run the load script with several worker replicas → `202` p99 stays low while queue depth grows, then drains as workers catch up. The **pipeline** dashboard shows queue depth, transcode duration and worker-busy tracking it; the **runtime** dashboard shows event-loop lag staying flat under load (proving the native work never blocks the loop) and RSS staying flat regardless of upload size (proving nothing is buffered).
8. Jaeger: one trace spans the API upload span through the AMQP publish into the worker's transcode span.
9. **Reliability check:** kill a worker mid-transcode → the message is redelivered to another replica and the job still completes. Then stop workers entirely, upload jobs, restart → the durable queue still holds them and they process.

---

## Open follow-ups (not in the initial build)
Presigned-URL uploads (client → MinIO directly, API never touches bytes), a DLQ replay script, and hardware-accelerated encoding (VideoToolbox) — all easy to add on top of this structure.
