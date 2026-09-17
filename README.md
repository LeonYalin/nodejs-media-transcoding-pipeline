## Distributed Media Transcoding Pipeline (RabbitMQ + MinIO + ffmpeg)

![Architecture: browser → Fastify API streaming into MinIO → RabbitMQ quorum work queue → worker containers (sharp, ffmpeg HLS) → MinIO, with a TTL retry queue, a parked queue, Redis job state feeding SSE progress, MinIO bucket events invoking Lambda functions, and a Prometheus/Grafana/Jaeger observability band](docs/architecture.svg)

> **Host requirements: Docker and Node. That's it.** `ffmpeg` lives inside the worker image.
> Full build order & design → [IMPLEMENTATION.md](IMPLEMENTATION.md).

### The goal

Learn how to accept an upload in milliseconds when the work it triggers takes minutes —
without losing the job if a process dies, and without ever holding a media file in RAM.
Here are the specific architectural instruments and engineering methods this project uses.

**1. Streaming ingest (never buffer the file)**

A 500 MB upload read into a `Buffer` is 500 MB of heap, per concurrent request.
*   **The Instrument:** `@fastify/multipart` + `@aws-sdk/lib-storage`'s `Upload`.
*   **The Method:** the multipart file arrives as a stream and is piped **directly** into a
    MinIO multipart upload, so the API never accumulates the file. Image transcodes stream the
    same way: MinIO → `sharp` → MinIO, with no intermediate file.

**2. Publisher confirms (don't lie to the client)**

`202 Accepted` means "I have taken responsibility for this job." If the broker never got the
message, that response was a lie.
*   **The Instrument:** an amqplib **confirm channel** + `persistent` messages + durable queues.
*   **The Method:** the API publishes and awaits the broker's confirm for *that* message before
    replying `202`. If the confirm fails, the client gets a `503`. A broker restart loses no
    queued work.

**3. Horizontal workers as the multi-core strategy**

Transcoding runs in native code (libvips, an ffmpeg child process), so `worker_threads` buy
little. What scales is more processes.
*   **The Instrument:** `docker compose up --scale worker=N` + AMQP `prefetch(1)`.
*   **The Method:** every stage (image, video plan, video rendition) lands on **one** quorum
    queue, `q.work`, and each worker runs a single consumer on it at prefetch 1: one job per
    container, so concurrency equals the replica count. One queue matters — RabbitMQ 4.x
    denies channel-global prefetch, so a worker consuming three queues would hold three jobs.

**4. Manual acks (at-least-once, no lost work)**

An `ack` on arrival means a crash mid-transcode silently drops the job.
*   **The Instrument:** manual acknowledgement plus deterministic output keys.
*   **The Method:** the worker acks **only after** the derivatives are in MinIO and Redis is
    updated. Kill a worker mid-encode and RabbitMQ redelivers the job to another replica; keys
    derive from the job ID, so the rerun overwrites rather than duplicates. A job that *keeps*
    killing its worker is capped too: the quorum queue's `x-delivery-count` spends attempts.

**5. Bounded retry with a dead-letter path**

RabbitMQ has no native "retry in 10 seconds", and requeuing immediately spins a poison message.
*   **The Instrument:** a dead-letter exchange feeding a **TTL delay queue**, plus `x-death`.
*   **The Method:** a failure `nack(requeue: false)`s into `q.retry`, which has no consumers.
    When the TTL expires, the message dead-letters **back** to `q.work` with its routing key
    intact. After `MAX_ATTEMPTS` — or at once for a non-retryable error like a corrupt file —
    it is **parked** in `q.parked` and the job is marked failed.

**6. Fan-out with a redelivery-safe barrier**

One video becomes up to three renditions that encode in parallel on different workers, but
the master playlist can only be written once all of them exist.
*   **The Instrument:** a topic exchange for fan-out + a Redis hash as the barrier.
*   **The Method:** a fast "plan" job probes the source, picks a ladder that **never
    upscales**, and publishes one job per rung. Each rendition marks its own entry in
    `job:{id}:renditions`; the one that sees every rung done writes `master.m3u8`. Keyed by
    rendition name, a redelivered rendition overwrites its entry instead of counting twice.

**7. Graceful shutdown**

On SIGTERM a worker cancels its consumer, finishes and acks the in-flight transcode, and closes
cleanly within Docker's `stop_grace_period`. Temp directories are removed in `finally` on every
path; a second signal kills ffmpeg immediately.

**8. Observability across a process boundary**

*   **The Instrument:** `prom-client` + Prometheus + Grafana, and **OpenTelemetry** → Jaeger.
*   **The Method:** metrics cover queue depth, transcode duration per rendition, retry and park
    rates and worker busy ratio, with Prometheus finding worker replicas by DNS. The amqplib
    instrumentation carries trace context **through the message headers**, so one Jaeger trace
    spans the HTTP upload, the publish, and the worker's processing in another container.

**9. Event-driven functions (S3 → Lambda)**

Not all work belongs on a queue. Small, idempotent reactions to "an object landed" are the classic
Lambda use case.
*   **The Instrument:** MinIO bucket notifications + AWS's own Lambda image
    (`public.ecr.aws/lambda/nodejs:24`, with the Runtime Interface Emulator), all local.
*   **The Method:** MinIO posts the standard S3 event JSON to the function's invoke URL. The
    `metadata` function writes `source.json` for every upload (dimensions from a 64 KB ranged
    read); the `placeholder` function writes a 20 px blurred preview whenever `full.webp`
    lands. The handlers are plain `handler(event)` exports that would deploy to AWS unchanged,
    and MinIO keeps undelivered events on disk until a stopped function comes back.

### Tech stack

*   **Queue/Storage/State:** RabbitMQ (topic exchange, quorum queue, DLX, TTL delay queue),
    MinIO (S3-compatible), Redis (job state + pub/sub).
*   **Node libraries:** `fastify`, `@fastify/multipart`, `amqplib`, `@aws-sdk/client-s3` +
    `lib-storage`, `sharp`, `fluent-ffmpeg`, `ioredis`, `zod`, `pino`, `prom-client`.
*   **Functions:** AWS Lambda Node.js 24 image, triggered by MinIO bucket events.
*   **Observability:** Prometheus, Grafana, Jaeger, RabbitMQ management UI, MinIO console,
    RedisInsight.
*   **Testing:** `vitest` (unit) + `testcontainers` (RabbitMQ + MinIO + Redis + the worker image).

### Quickstart

```bash
cp .env.example .env   # defaults work out of the box
npm install
npm run up             # infra + 4 transcode workers (WORKERS=8 npm run up to change)
npm run infra:init     # asserts AMQP topology, verifies buckets + connectivity

npm run dev            # the API on the host (workers already run in Docker)
npm run load           # optional: 30 s of concurrent uploads, reports p99 + 202 rate
```

Then open:

*   **UI:** http://localhost:3000 — upload an image or video, watch progress, play the HLS result
*   **RabbitMQ:** http://localhost:15672 → media/media
*   **MinIO console:** http://localhost:9001 · **RedisInsight:** http://localhost:5540
*   **Grafana:** http://localhost:3001 → admin/admin · **Prometheus:** http://localhost:9090
*   **Jaeger:** http://localhost:16686

`Makefile` mirrors every `npm run` script above (`make up`, `make infra-init`, `make dev`, …).

To stop: `Ctrl-C` the API and `npm run down` for the containers.

### Tests

```bash
npm test                     # unit tests — pure logic and injected fakes, no infra
npm run test:integration     # RabbitMQ + MinIO + Redis + the built worker image, via testcontainers
npm run typecheck            # tsc --noEmit
npm run lint
```

Unit tests sit beside their source as `*.test.ts`; the container-backed suite lives in
`tests/integration/`. It runs the real worker image and asserts the whole path: image and video
derivatives in MinIO, an HLS ladder with no upscaled rungs, a failing job retrying through the
delay queue and then parking, and a worker killed mid-transcode whose job still completes.

There is no module mocking anywhere — dependencies are injected, so tests either pass a plain
fake object or run against a real container. Fakes are reserved for what a real dependency
can't do on cue: failure injection and timer control.

### Status

Complete — all 15 build steps in [IMPLEMENTATION.md](IMPLEMENTATION.md) are done, from infra
and topology through the workers, reliability, UI, the test suite, and Lambda functions.
