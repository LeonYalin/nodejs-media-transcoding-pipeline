# Distributed Media Transcoding Pipeline (RabbitMQ + MinIO + ffmpeg) — Implementation Plan

> This document is the executable build spec. The Claude Code config files
> (`CLAUDE.md`, `.claude/`, `.mcp.json`) described in Step 14 already exist in the
> repo. Steps 1–9 are implemented and were re-checked against the code; where the
> code departs from the original text, the deviation is recorded under its step.

## Context

The repo starts essentially empty (only `README.md` + `.gitignore`). The goal is to master **asynchronous job queuing**, **heavy multi-core CPU background work**, and **S3-compatible object storage** — the production problem of accepting an upload in milliseconds while the actual transcode takes minutes, without losing the job when a process dies and without ever holding a media file in RAM.

This plan builds that as a **small but production-shaped learning app**: a Fastify API that streams uploads straight into MinIO and returns `202 Accepted`, a durable RabbitMQ topology with manual acks, bounded retry and a dead-letter path, and a fleet of containerized workers that transcode images with `sharp` and video into an **HLS ladder** with `ffmpeg`. Everything runs locally on free, open-source software via `docker compose`.

### Decisions locked in
- **Language:** TypeScript (strict), ESM, `tsx` for dev, `zod` for runtime validation.
- **HTTP:** Fastify 5 (deliberately different from the previous project's Express).
- **The host stays clean.** `ffmpeg`/`ffprobe` are **never installed on the machine** — they are baked into the worker image from a static build. Host prerequisites are Docker and Node, nothing else.
- **Multi-core model:** N **worker containers** (compose replicas), each consuming on one AMQP channel at channel-global `prefetch(1, true)` → exactly one job in flight per worker. Scaling is `--scale worker=N`, not threads.
- **Memory model:** uploads stream request → MinIO via `@aws-sdk/lib-storage`; image transcodes stream MinIO → `sharp` → MinIO. Only video touches disk (ffmpeg needs a seekable file), in a temp dir removed in `finally`.
- **Durability:** durable exchanges/queues, `persistent` messages, and a **confirm channel** — the API does not return `202` until the broker has confirmed the job.
- **Reliability:** manual `ack` after success only; failures `nack(requeue:false)` into a **TTL delay queue** that dead-letters back to the work queue; after `MAX_ATTEMPTS` (read from the `x-death` header) the message is **parked** in a terminal DLQ and the job is marked failed.
- **Job state:** Redis — job records + progress, with pub/sub feeding **SSE** to the browser.
- **Observability:** pino logs, `prom-client` → Prometheus → Grafana, **plus OpenTelemetry traces → Jaeger** so a single trace spans API → RabbitMQ → worker. Every datastore also gets a browser UI.
- **Code structure:** modules export `createX(deps)` factories with structurally-typed dependencies (a dep object, or a single positional collaborator when there is exactly one); each process entrypoint is the composition root and the only place with import-time side effects. Config is the one deliberate singleton. No DI container.
- **Testing:** Vitest unit tests (pure modules + injected fakes) + a testcontainers integration suite (RabbitMQ + MinIO + Redis + the built worker image). No module mocking.
- **Env loading:** no `dotenv` — Node's built-in `--env-file=.env`, then zod-validated in `src/config`.

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
                                  │            job.image.transform  job.video.plan  job.video.rendition
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
                                    non-retryable, or this queue's x-death rejections + 1 >= MAX_ATTEMPTS
                                              → media.parked → q.parked (terminal)

Video fan-out: q.video.plan (ffprobe, fast) records the ladder + renditionsExpected=N, then emits 1..3 rendition jobs.
Barrier:      each rendition marks its own entry in job:{id}:renditions; whoever sees N done writes master.m3u8.

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
| `minio/mc:RELEASE.2025-08-13T08-35-41Z` | one-shot bucket bootstrap (`minio-init`) |
| `prom/prometheus:v3.13.2`, `grafana/grafana:13.0.6`, `jaegertracing/all-in-one:1.76.0` | standard; pinned for reproducible builds |

---

## Project Structure

```
.
├── docker-compose.yml · Dockerfile.worker · .dockerignore
├── .env.example · Makefile · package.json · tsconfig.json
├── vitest.config.ts · eslint.config.js · .prettierrc.json · .prettierignore
├── rabbitmq/enabled_plugins            # rabbitmq_management, rabbitmq_prometheus
├── prometheus/prometheus.yml           # api (host.docker.internal), worker (dns_sd), rabbitmq
├── grafana/provisioning/…              # datasource + dashboards/{pipeline,runtime}.json   (step 10)
├── public/index.html                   # upload form + job table + SSE progress + hls.js player   (step 12)
├── scripts/                                                                                     (step 12)
│   ├── infra-init.ts                   # assert MinIO buckets + AMQP topology; verify reachability
│   └── load.ts                         # autocannon: concurrent multipart uploads, p99 + 202 rate
├── tests/integration/                  # testcontainers: rabbitmq + minio + redis + built worker image   (step 13)
└── src/
    ├── config/index.ts                 # env → zod → typed config singleton (only reader of process.env)
    ├── domain/
    │   ├── job.ts                      # zod: JobMessage (wire), JobRecord (Redis), JobStatus
    │   └── media.ts                    # zod: mime allowlist, Rendition, ProbeResult; error classes
    ├── lib/
    │   ├── tracing.ts                  # OTel NodeSDK bootstrap — imported first in each entrypoint
    │   ├── logger.ts · metrics.ts · metrics-server.ts
    │   ├── amqp.ts                     # connectAmqp, confirm-channel provider, per-routing-key job publisher
    │   ├── topology.ts                 # THE single definition of exchanges/queues/bindings/args
    │   ├── s3.ts                       # S3Client for MinIO (forcePathStyle, static creds)
    │   ├── object-repository.ts        # the only place that calls S3 (putStream/getStream/…)
    │   ├── redis.ts
    │   └── jobs-repository.ts          # the only place that writes Redis (record, progress publish, rendition barrier)
    ├── media/                          # PURE / near-pure, unit-tested without infra
    │   ├── ladder.ts                   # ProbeResult → rendition list (never upscale, aspect-correct)
    │   ├── hls.ts                      # master playlist generation + H.264 level per rung
    │   └── ffmpeg.ts                   # pure parseProbe/buildHlsOutputOptions + probeVideo/transcodeToHls/extractPoster
    ├── api/
    │   ├── app.ts                      # createApp(deps) — plugins, routes, no listen()
    │   ├── contracts.ts                # *Request / *Reply types shared by every route
    │   ├── index.ts                    # composition root (tracing→config→clients→listen→signals)
    │   ├── upload-service.ts           # the accept-an-upload saga; routes stay thin
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
        ├── ffmpeg-expert.md            # ladder / HLS / encoder-flag design
        └── project-standards-reviewer.md # structure / house-style audit after each step
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
media-outputs/{jobId}/hls/{rendition}/{index.m3u8, seg_000.ts, …}    # rendition name, e.g. 720p
media-outputs/{jobId}/hls/master.m3u8
```
Keys are **deterministic** — a redelivered job overwrites its own outputs, which is what makes at-least-once delivery safe here.

**Redis** — `job:{id}` hash (`status`, `type`, `sourceKey`, `mime`, `bytes`, `createdAt`, `updatedAt`, `progress`, `error`, `outputs`, `renditions`, `renditionsExpected`, `renditionsDone`), TTL `JOB_TTL_SECONDS`; `job:{id}:renditions` hash (rendition name → percent, 100 = uploaded; the fan-out barrier), same TTL; `jobs:recent` sorted set for the UI; pub/sub channel `job:{id}:events`.

**Routing keys** — `job.image.transform`, `job.video.plan`, `job.video.rendition`.

---

## Implementation Steps

### 1. Scaffolding & config — DONE
- `package.json` (ESM, `type: module`), `tsconfig.json` (strict, `moduleResolution: NodeNext`).
- **Note:** TypeScript is pinned to `^5` — `typescript-eslint@8` declares `typescript >=4.8.4 <6.1.0`, so TS 7 would break linting. (The ETL repo could run TS 7 only because it deferred ESLint.)
- Install deps: `fastify @fastify/multipart @fastify/static amqplib @aws-sdk/client-s3 @aws-sdk/lib-storage sharp fluent-ffmpeg ioredis zod pino prom-client @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http @opentelemetry/instrumentation-{http,fastify,amqplib,ioredis,aws-sdk}`; dev: `tsx typescript @types/node @types/fluent-ffmpeg pino-pretty vitest testcontainers @testcontainers/rabbitmq autocannon eslint @eslint/js typescript-eslint prettier`.

### 2. Config (`src/config/index.ts`) — DONE
- Read `process.env`, validate with zod, export typed `config` + a pure `loadConfig(env)` that **throws** rather than exiting (so tests can assert on it).
- Keys: `AMQP_URL`, `AMQP_PREFETCH`, `RETRY_TTL_MS`, `MAX_ATTEMPTS`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `BUCKET_UPLOADS`, `BUCKET_OUTPUTS`, `REDIS_URL`, `API_PORT`, `MAX_UPLOAD_BYTES`, `WORKER_METRICS_PORT`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `JOB_TTL_SECONDS`, `HLS_SEGMENT_SECONDS`, `NODE_ENV`. Mirror every key in `.env.example`.
- `src/lib/logger.ts` (pino, silent under `NODE_ENV=test`), `src/lib/metrics.ts` (own Registry + `collectDefaultMetrics`), `src/lib/metrics-server.ts` (workers only — the API serves `/metrics` through Fastify).
- Queue depth is **not** a `media_*` metric: RabbitMQ's `rabbitmq_prometheus` plugin already publishes it per queue, and a second source would drift.

### 3. Domain + media core (pure first) — DONE
- `domain/job.ts`: `JobMessageSchema` (the wire contract shared by API and worker) plus one schema per queue (`ImageJobMessageSchema`, `VideoJobMessageSchema`, `VideoRenditionJobMessageSchema` — the delivering queue, not a payload field, picks the schema), `JobRecordSchema`, `JobStatus` = `queued|processing|completed|failed`.
- `domain/media.ts`: MIME allowlist (`image/{jpeg,png,webp,avif}`, `video/{mp4,quicktime,webm,x-matroska}`), `Rendition`, `ProbeResult`, and the **error classes that drive retry-vs-park**: `UnsupportedMediaError`, `CorruptMediaError`, `ObjectNotFoundError` (all `retryable = false`); everything else defaults to retryable.
- `media/ladder.ts` — pure `buildLadder(probe)`: from `[1080p 5000k, 720p 2800k, 360p 800k]`, keep renditions whose height ≤ source height, **never upscale**; if the source is smaller than the smallest rung, emit a single source-height rendition. Takes the whole `ProbeResult` (not just height) so each rung's **width follows the source aspect ratio** — portrait and 4:3 sources must not be advertised as 16:9. Both dimensions are forced **even** (H.264 yuv420p requirement, matching ffmpeg's `scale=-2:h`). **Throws `CorruptMediaError` on unusable dimensions** rather than returning `[]`: an empty ladder would set `renditionsExpected = 0` and hang the fan-out barrier forever.
- `media/hls.ts` — pure `buildMasterPlaylist(renditions)` → `#EXT-X-STREAM-INF:BANDWIDTH=…,RESOLUTION=…,CODECS="…"` + relative variant paths taken from `rendition.name`, so they can't drift from the directory the worker writes. The AVC codec string is **per rung, not fixed**: L3.0 `avc1.4d401e` ≤480p, L3.1 `avc1.4d401f` ≤720p, L4.0 `avc1.4d4028` above — a single hardcoded L3.1 under-declares 1080p and strict players reject it.
- Write these two with their unit tests **before** any infra exists — they need none.

### 4. Infra (`docker-compose.yml`) — DONE
- **RabbitMQ** `4.3-management-alpine`, `rabbitmq/enabled_plugins` mounted to enable `rabbitmq_management` + `rabbitmq_prometheus`; ports 5672 (AMQP), 15672 (UI), 15692 (metrics); healthcheck `rabbitmq-diagnostics -q ping`; **named volume** so `docker compose down` doesn't discard durable queues.
- **Credentials: a real `media` user via `RABBITMQ_DEFAULT_USER/PASS`, not `guest`.** The built-in `guest` account is restricted to loopback, so worker containers cannot authenticate with it — verified: `guest` returns **401** from another container while `media` returns 200.
- **MinIO** on the pinned console-bearing release; ports 9000 (S3) / 9001 (console); a short-lived `mc` bootstrap service creates `media-uploads` + `media-outputs` with `&&` chaining so a failure surfaces instead of exiting 0. Healthcheck is **`mc ready local`**, not `curl` — the MinIO image ships no curl.
- **Redis** `7-alpine` with `--appendonly yes`; healthcheck `redis-cli ping`. **RedisInsight** `3.8` alongside it on port 5540 for browsing job hashes and watching pub/sub.
- **Prometheus** (mounted config, already carrying the api / rabbitmq / worker scrape jobs), **Grafana** (host port 3001 → 3000 because the API owns 3000; `grafana/provisioning/` is mounted but still empty — the datasource and both dashboards arrive in step 10), **Jaeger all-in-one** (OTLP/HTTP 4318, UI 16686).
- **worker**: built from `Dockerfile.worker`, `depends_on` healthy rabbitmq/minio/redis, `stop_grace_period: 60s`, `restart: unless-stopped` (added in step 8), no fixed `container_name` (it must be scalable), service-name env overrides, narrow bind-mounts per the execution model. Scaled inline by `npm run up` (`--scale worker=${WORKERS:-4}`) — **one compose file, one `up` command**, deliberately chosen over Compose profiles or a second override file: this is a learning project that won't grow, so fewer commands to remember beats an infra-only mode nobody would use.
- **Env-key discipline:** the worker's `environment:` keys must match `src/config` exactly. Every key has a default, so a typo does **not** fail loudly — it silently falls back to a `localhost` URL that resolves to the container itself.
- Named network `media_pipeline_net` so MCP containers can join by name.

### 5. Worker image (`Dockerfile.worker`) — DONE
- `FROM node:24-bookworm-slim`; `COPY --from=mwader/static-ffmpeg:8.1.2 /ffmpeg /ffprobe /usr/local/bin/`; `npm ci && npm cache clean --force` (the cache is ~120MB in-layer otherwise); run as the base image's non-root `node` user.
- **Entry is `node_modules/.bin/tsx watch src/worker/index.ts` — never `npx`.** Verified by signal test: under `npx`, SIGTERM is not forwarded to the child, so the graceful-drain handler never runs and the container exits 1; run directly, the handler fires and it exits 0. `watch` itself forwards signals correctly, so live reload is safe to keep.
- **Single stage, not multi-stage.** `sharp` ships prebuilt glibc binaries, so no python3/make/g++ is needed; the build is ~3× faster at the same final image size once the npm cache is cleaned.
- **The image carries `src`** rather than relying on the compose bind mount, so it can run standalone — which is exactly what the Step 13 testcontainers suite does. Compose still mounts `./src` on top for live reload.
- `.dockerignore`: host `node_modules` (wrong platform for `sharp`), `.git`, `.env`, `tmp`, `dist`, plus `tests`, `public`, infra config, docs, and `src/**/*.test.ts`.
- Acceptance check, enforced as a build step so a broken binary fails the build rather than the first transcode: `ffmpeg -version`, `ffprobe -version`, and `node -e "require('sharp')"` all succeed.
- *(Fallback only if the static build ever misbehaves: install ffmpeg from Debian packages in the image instead. The host is never touched either way.)*

### 6. AMQP topology (`src/lib/topology.ts`) — DONE
The single definition of every exchange, queue, binding and argument — asserted idempotently by both entrypoints on every (re)connect, through `connectAmqp`'s recovery `setup`. `scripts/infra-init.ts` will reuse it in step 12.

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
- `RETRY_TTL_MS` comes from `src/config` — never hardcoded here. Note that **queue arguments are immutable**: changing the TTL against a broker that already has `q.retry` fails with `PRECONDITION_FAILED` (406) until that queue is deleted.
- `assertTopology` takes its channel as a **structural** `TopologyChannel` interface (the three methods it uses), so tests pass a plain recording fake — no module mocking, no casts.
- **Verified against a live broker**, not just unit-tested: publish → `nack(requeue:false)` → lands in `q.retry` → after the TTL returns to `q.image` with routing key `job.image.transform` intact. The resulting header is:
  ```
  x-death[0]  queue=q.retry  reason=expired    count=1
  x-death[1]  queue=q.image  reason=rejected   count=1
  ```
  This is the concrete evidence for the retry-counting invariant: `x-death[0]` is the **q.retry/expired** entry, so `retry.ts` must select by *queue name + `reason: rejected`* or it will count the wrong thing.

### 7. API (`src/api/`) — DONE
This step also had to build the `src/lib` I/O modules both entrypoints share, since
nothing before it needed them: `redis.ts`, `s3.ts`, `object-repository.ts`, `amqp.ts`,
`jobs-repository.ts` and `tracing.ts` (the last one minimal; Step 10 fills in the dashboards
and the trace-continuity check).

Five deliberate deviations from the text below, each for a stated reason:
- **Per-message confirm callback, not `waitForConfirms()`.** `waitForConfirms` resolves
  for *every* outstanding publish on the channel, so under concurrent uploads each
  request inherits the latency of the slowest one — which defeats the "202 in
  milliseconds" claim. `ConfirmChannel.publish`'s callback confirms exactly one message
  and upholds the same invariant. Lives in `lib/amqp.ts`'s `createJobPublisher`.
- **The object key is `{jobId}/source{ext}`, with the bucket passed separately.** The
  data-contract notation `media-uploads/{jobId}/source{ext}` is bucket + key; folding
  the bucket name into the key too would have written `media-uploads/media-uploads/…`.
- **The extension comes from the MIME type** (`EXTENSION_BY_MIME` in `domain/media.ts`),
  not `path.extname(filename)`: the filename is client-controlled and may be absent or
  contradict the declared type.
- **The upload saga lives in `api/upload-service.ts`, not in the route.** Written first as a
  108-line handler with four `try/catch` blocks, it was extracted after comparing against the
  ETL project, whose routes are ~4 lines because a service sits behind them. The route now only
  maps a returned `UploadOutcome` union to a status code. Same reason the I/O modules are
  `*-repository`, not `*-store`: the ETL project uses the word "store" nowhere.
- **`GET /health`, not `/healthz`.** The `z` suffix is a Kubernetes-ism; nothing here runs on
  Kubernetes, and the ETL project uses plain `/health`.

Also worth recording: `@fastify/multipart` is registered with `throwFileSizeLimit: false`
so an oversized body ends the stream quietly and sets `file.truncated`. Throwing mid-pipe
would skip the upload service's cleanup, leaving the partial object in the bucket forever. And the
confirm channel is handed out by a provider that re-opens it after a reconnect — amqplib's
recovery restores the *connection*, but channels opened from it are dead afterwards. Every
route declares its Fastify generics from `api/contracts.ts` (`*Request` / `*Reply`), so reply
bodies are compile-checked.

- `app.ts` exports `createApp(deps)` — registers `@fastify/multipart` (with `limits.fileSize = MAX_UPLOAD_BYTES`), `@fastify/static` for `public/`, routes, and a central error handler. **No `listen()`** — so tests can drive it directly.
- `POST /uploads`: the route delegates to `upload-service.ts`, which takes `req.file()`, validates the MIME against the allowlist, pipes `file` straight into `@aws-sdk/lib-storage` `Upload` targeting `media-uploads/{jobId}/source{ext}` — the bytes never accumulate in RAM. After the upload resolves, **check `file.truncated`**; if the size limit was hit, delete the partial object and return `413`.
- Then: write the Redis job record (`queued`), publish `JobMessage` on a **confirm channel** with `persistent: true`, await the broker's confirm for *that message* (see the deviation note above), and only then reply `202 { jobId, statusUrl, eventsUrl }`. If the confirm fails, mark the job failed and return `503` — never a `202` for a job the broker did not accept.
- `GET /jobs/:id` (record), `GET /jobs` (recent, for the table), `GET /jobs/:id/events` (SSE), `GET /health` (broker + bucket + redis reachability), `GET /metrics`.
- `sse.ts`: set the SSE headers, subscribe a **dedicated** ioredis connection to `job:{id}:events` (subscriber-mode connections can't run normal commands), emit a comment heartbeat every 15 s, and unsubscribe + quit on `req.raw.on('close')`.

### 8. Worker consumer (`src/worker/consumer.ts`) — DONE
- One connection per container, and one consumer channel on it; `await ch.prefetch(1, true)` — the `global` flag makes it **channel-wide**, so the three consumers share a single in-flight slot and one container = one job at a time.
- Three consumers (`q.image`, `q.video.plan`, `q.video.rendition`), `noAck: false`. Per message: zod-parse → dispatch to the handler → `ch.ack(msg)` **only after full success** (derivatives uploaded, Redis updated).
- On throw: `retry.ts` decides, then either `ch.nack(msg, false, false)` (→ `media.retry` → delay → back) or publish to `media.parked` + `ch.ack` (terminal) + Redis `failed` with the reason.
- `retry.ts` is **pure and heavily unit-tested**:
  - `attempts` = the `count` of the `x-death` entry whose `queue` is *this* work queue and whose `reason` is `rejected` (0 when the header is absent). Selecting the right entry matters — the array accumulates entries for `q.retry` (`reason: expired`) too, and naively reading `x-death[0].count` gives the wrong number.
  - `error.retryable === false` → park. `attempts + 1 >= MAX_ATTEMPTS` → park (`max-attempts`). Otherwise → retry.

Deviations and additions, each for a stated reason:
- **The consumer channel is not re-opened in-process.** amqplib's recovery restores only the connection, and consumers die with their channel. `worker/index.ts` exits when that channel closes, compose's `restart: unless-stopped` brings the container back, and the broker redelivers everything unacked. The plan stage's fan-out publishes use a separate confirm-channel provider, which does re-open.
- **A body that is not JSON, or fails its queue's schema, throws `InvalidJobMessageError`** (`retryable = false`, in `domain/media.ts`), so it parks on the first attempt instead of cycling through the delay queue.
- **Parking confirms before it acks.** The copy published to `media.parked` keeps the routing key and headers and adds `park-queue` / `park-error`; the original is acked only after the broker confirms it. Marking Redis `failed` is best-effort and uses `messageId`, which the publisher sets to the `jobId`, so even an unparseable body is traced to its job.
- **Handlers own `processing` / `completed`**, not the consumer: for video, only the last rendition knows the job is finished.
- **The in-flight limit comes from `AMQP_PREFETCH`** (default 1), still applied with `global: true`.
- **Known gap:** a worker killed mid-job (OOM, SIGKILL) has its message redelivered without an `x-death` entry, so the attempt count never rises and a job that crashes its worker loops. Classic queues cannot cap this; a quorum queue's `delivery-limit` can — a step 11 candidate.
- The worker metrics are declared in `lib/metrics.ts` but not yet recorded — step 10.

### 9. Handlers (`src/worker/handlers/`) — DONE
- **`image.ts`** — `GetObject` body → `sharp` → `Upload`, **fully streaming, no temp file**: one pipeline for `full.webp` (max 1920 wide, `withoutEnlargement`), one for `thumb.webp` (320 wide). `sharp` releases the event loop to libvips' thread pool, so this stays non-blocking.
- **`video-plan.ts`** — download the source into a workspace, `ffprobe` it, `buildLadder(probe)`, extract `poster.jpg` at 1 s, write `renditionsExpected` to Redis, then publish one `job.video.rendition` message per rung (persistent, on a confirm channel) and ack. Fast job, deliberately separated from the heavy one.
- **`video-rendition.ts`** — download the source, run ffmpeg → HLS (`-c:v libx264 -preset veryfast -c:a aac -hls_time HLS_SEGMENT_SECONDS -hls_playlist_type vod`) into the workspace, forward `.on('progress')` **throttled to ~1/s** into `jobs-repository` (Redis hash + pub/sub → SSE), upload the segments + variant playlist, then `HINCRBY renditionsDone 1`; the worker whose increment returns `renditionsExpected` writes `master.m3u8` via `buildMasterPlaylist()` and marks the job `completed`. The `HINCRBY` return value is the atomic barrier — no locks.
- **`workspace.ts`** — `mkdtemp` per job, `rm -rf` in `finally`, always, including on the park path. `createWorkspace()` returns `{ path, cleanup }`; `cleanup` logs instead of throwing, so it never replaces the job's own outcome.
- The **streaming (image) vs. temp-file (video)** split is deliberate and documented: ffmpeg needs a seekable input and writes many segment files, so disk is the correct answer there; anything that *can* stream, must.

Deviations from the text above, each for a stated reason:
- **The barrier is a per-rendition hash, not `HINCRBY renditionsDone`.** `job:{id}:renditions` maps rendition name → percent. A counter is not redelivery-safe: a rendition that increments and then dies before its ack is counted twice, which can complete the job while another rung is still encoding. Keyed by name, a redelivery overwrites its own entry. The same hash yields job `progress` as the mean across the whole ladder; an encode reports at most 99, and 100 is written only after upload, which is what the barrier counts.
- **`updateJob` writes only the fields it changed.** One job's renditions update Redis from different workers at once; writing back a whole merged record let one worker's stale read undo another's write.
- **The ladder is stored on the job record** (`renditions`), because the last rendition has to build `master.m3u8` from all rungs and its own message carries only one.
- **The publisher validates against the routing key's own schema.** Parsing a rendition with the plain video schema silently stripped its `rendition` field.
- **No `media/images.ts`.** The two sharp pipelines (`full`, `thumb`) read more clearly inline in the handler. On a failed download each clone must be destroyed directly: destroying the shared input does not reach them (verified: they hang). `object-repository.putStream` now also fails an upload whose body closes early, so abandoning one of the two uploads cannot leave it waiting forever.
- **`worker/index.ts` was built here**, not left for step 11: the consumer cannot run without real handlers. It wires the clients, handlers and consumer, exits when the consumer channel is lost, and drains on SIGTERM.
- **Handlers own `processing` / `completed`**, not the consumer (see step 8).
- **ffmpeg details** (`media/ffmpeg.ts`): probe dimensions honour rotation metadata, since ffmpeg auto-rotates portrait phone video; keyframes are forced on the segment clock with `-sc_threshold 0`, so every rung cuts at the same instants; `-level:v` comes from `hls.ts`, so the encoded level matches the advertised `CODECS`; the poster is taken at `min(1 s, duration / 2)`.
- **The video path has not yet run end-to-end** (Verification §4–6, via `transcode-verifier`). ffmpeg does not run on the host, so the video handlers are unit-tested only through `parseProbe` / `buildHlsOutputOptions` and the repository barrier. The image handler runs against real sharp.

### 10. Observability — DONE
- `prom-client`: `media_jobs_total{type,status}`, `media_transcode_duration_seconds{type,rendition}`, `media_upload_bytes`, `media_retries_total{queue}`, `media_parked_total{queue,reason}`, `media_worker_busy`, plus default metrics (incl. event-loop lag).
- `prometheus.yml` scrapes: the host API via `host.docker.internal`, RabbitMQ's own `:15692/metrics`, and worker replicas via `dns_sd_configs: [{ names: [worker], type: A, port: <WORKER_METRICS_PORT> }]`.
- **Two Grafana dashboards, not one** — split by the question each answers, which is the standard overview→drill-down pattern:
  - **`pipeline.json` — "is the pipeline keeping up?"** Queue depth per queue, jobs/min by status, transcode duration p50/p95 by rendition, retry + parked rate, worker busy ratio, upload throughput and `202` latency.
  - **`runtime.json` — "why is it slow, and is any process unhealthy?"** Per-instance event-loop lag (p99), heap vs. RSS, GC pause time, CPU, active handles/requests, process uptime/restarts — all from `collectDefaultMetrics`.
- The overview additionally carries a **compact "runtime health" row** with exactly two default metrics: **event-loop lag p99** and **RSS**. This is deliberate, not duplication — those two are the direct evidence for the project's two central claims ("we never block the event loop", "we never buffer a media file"). Everything else stays on the runtime dashboard.
- Wire them together: a shared `instance` template variable plus Grafana **dashboard links / data links** on the runtime row, so clicking a lag spike lands on that instance's panels. Both dashboards are auto-provisioned from `grafana/provisioning/dashboards/`.
- **Aggregation rule for scaled workers:** default metrics are per-instance, so `sum()` across replicas hides a single sick one. Use `max by (instance)` / per-instance series for lag, heap and RSS; reserve `sum()` for genuinely additive work counters. The `job` label separates the host API from the worker replicas.
- Use `nodejs_eventloop_lag_p99_seconds` rather than the mean `nodejs_eventloop_lag_seconds` — a blocked loop shows up in the tail long before the average moves.
- `lib/tracing.ts`: `NodeSDK` with the http/fastify/amqplib/ioredis/aws-sdk instrumentations and an OTLP/HTTP exporter → Jaeger. **Imported first** in both entrypoints (before any instrumented library). Acceptance check: one Jaeger trace contains the API span *and* the worker's ffmpeg span — proving the amqplib instrumentation propagated context through the message headers.

Deviations from the text above, each for a stated reason:
- **The worker's metrics are recorded in `consumer.ts`, not in the handlers.** It is the one place that already knows the queue, the settlement decision and the delivery's start and end, so the handlers stay free of instrumentation. The `rendition` label is the exception that shapes the code: only the parsed body names a rung, so each entry in `handlerByQueue` now *returns* its stage label (`image`, `plan`, or the rendition name) and the consumer supplies it when it stops the timer.
- **`media_jobs_total` counts stages, not uploads.** A video increments it once for its plan and once per rung, because a stage is what a worker settles. The help text and the dashboard panel both say so rather than implying an upload count.
- **`media_worker_busy` is incremented/decremented, not set to 1/0.** At `prefetch(1)` the two are identical; the gauge keeps telling the truth if the prefetch is ever raised.
- **Only successful stages are timed.** A failure's duration is the time to the error, which would drag the encode percentiles toward zero and hide a slow ladder.
- **Queue depth needs a second scrape job.** RabbitMQ's `/metrics` aggregates every queue into one series, which cannot answer "which queue is backed up", so `rabbitmq-queues` scrapes `/metrics/detailed?family=queue_coarse_metrics` for the per-queue split. Only the coarse family, so it stays cheap at a 5 s interval.
- **No manual ffmpeg span.** The amqplib instrumentation makes the consume span the active context for the whole handler, so the S3 and Redis spans inside a transcode already hang off the API's trace. Adding a hand-rolled span would prove nothing the propagation does not.
- **The trace-continuity check has not been run** — it needs the stack up (Verification §8), like the video path from step 9.

### 11. Reliability
- **Graceful shutdown:** SIGTERM → `ch.cancel(consumerTag)` for all three consumers (stop new deliveries), await the in-flight job, ack it, close channel + connection, flush the tracer, exit 0. A second signal kills the ffmpeg child immediately. *(In place since step 9: `worker/index.ts` cancels, drains, closes and flushes. Still missing: a second signal exits the process without explicitly killing the ffmpeg child.)*
- **No-loss proof:** kill a worker mid-transcode; the unacked message is redelivered when the connection drops, another replica picks it up, deterministic keys make the rewrite harmless.
- **Idempotency:** output keys derive from `jobId` + rendition, so redelivery overwrites rather than duplicating; the barrier is keyed by rendition name (see step 9), so a redelivered rendition is never counted twice.

### 12. Developer ergonomics + UI
- npm scripts: `up` (`--scale worker=${WORKERS:-4}`), `down`, `build:worker`, `logs:worker`, `infra:init`, `dev:api`, `load`, `lint`, `format`, `typecheck`, `test`, `test:watch`, `test:integration`. `Makefile` mirrors them. *(Already in place: the npm scripts and `Makefile`. `scripts/` and `public/` are still empty, so `infra:init` and `load` do not run yet.)*
- `scripts/infra-init.ts`: assert MinIO buckets + the full AMQP topology, then verify reachability of RabbitMQ, MinIO and Redis — the one command to run after `up`.
- `scripts/load.ts`: `autocannon` firing concurrent multipart uploads of a fixture, reporting p99 latency and the `202` rate — the proof that ingest stays fast while workers churn.
- `public/index.html`: vanilla JS — upload form, job table polled from `GET /jobs`, per-job progress bars driven by SSE, and an `hls.js` player pointed at the finished `master.m3u8`. No framework, no build step.

### 13. Tests
- **Unit** (`*.test.ts` beside the source, no infra): `media/ladder` (no upscaling, tiny sources, exact rungs), `media/hls` (playlist text), `worker/retry` (the full `x-death` matrix: absent header, first rejection, mixed `rejected`/`expired` entries, non-retryable error, max attempts), `domain/*` schemas, `config` validation, `worker/consumer`, `worker/workspace`, the pure parts of `media/ffmpeg`, and the image handler against real sharp with fake repositories. *(All of these exist.)* The video handlers have no unit tests — ffmpeg is not on the host — so the integration suite is their test.
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
- **RabbitMQ:** durable topology, persistent messages, **publisher confirms before acknowledging the client**, manual `ack` after side effects, channel-global `prefetch(1)` as the unit of concurrency, DLX-based **bounded retry via a TTL delay queue**, `x-death` introspection, terminal parking, topic-exchange **fan-out with a redelivery-safe barrier**.
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
