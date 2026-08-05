## Distributed Media Transcoding Pipeline (RabbitMQ + MinIO + ffmpeg)

A **Fastify API** streams uploads straight into **MinIO** and returns `202 Accepted` in
milliseconds → a durable **RabbitMQ** job is published with publisher confirms → a fleet of
**worker containers** transcodes images with `sharp` and video into an **HLS ladder** with
`ffmpeg`, writing the derivatives back to MinIO. **Redis** holds job state and pushes live
progress to the browser over SSE.

> **Host requirements: Docker and Node. That's it.** `ffmpeg` is never installed on your
> machine — it lives inside the worker image.

> Full build order & design → [IMPLEMENTATION.md](IMPLEMENTATION.md).

### The goal

Learn how to accept an upload in milliseconds when the work it triggers takes minutes —
without losing the job if a process dies, and without ever holding a media file in RAM.
Here are the specific architectural instruments and engineering methods this project uses.

**1. Streaming ingest (never buffer the file)**

A 500 MB video upload read into a `Buffer` is 500 MB of heap, per concurrent request. Ten of
those and the API is dead.
*   **The Instrument:** `@fastify/multipart` + `@aws-sdk/lib-storage`'s `Upload`.
*   **The Method:** the multipart file arrives as a readable stream and is piped **directly**
    into a MinIO multipart upload. Bytes flow request → socket → object storage; the API
    process never accumulates the file. The same discipline applies on the way out — image
    transcodes stream MinIO → `sharp` → MinIO with no intermediate file at all.

**2. Publisher confirms (don't lie to the client)**

Returning `202 Accepted` means "I have taken responsibility for this job." If the broker
never actually received the message, that response was a lie and the upload is orphaned in
storage forever.
*   **The Instrument:** an amqplib **confirm channel** + `persistent` messages + durable queues.
*   **The Method:** the API publishes and then `await`s `waitForConfirms()` — an explicit
    broker acknowledgement that the message is on disk — *before* replying `202`. If the
    confirm fails the client gets a `503` and knows to retry. Combined with durable queues,
    a broker restart doesn't lose queued work.

**3. Horizontal workers as the multi-core strategy**

Transcoding is CPU-bound and happens in native code (libvips for images, an ffmpeg child
process for video), so `worker_threads` buy little. What actually scales is more processes.
*   **The Instrument:** `docker compose up --scale worker=N` + AMQP `prefetch(1, true)`.
*   **The Method:** each worker container opens one channel with a **channel-global** prefetch
    of 1, so it holds exactly one unacknowledged job at a time. Concurrency is therefore
    exactly the replica count — set it to your core count and every core saturates, while the
    queue absorbs everything else. The `global` flag is load-bearing: without it each of the
    three consumers gets its own slot and one container quietly runs three jobs at once.

**4. Manual acks (at-least-once, no lost work)**

An `ack` sent when the message *arrives* means a crash mid-transcode silently drops the job.
*   **The Instrument:** manual acknowledgement (`noAck: false`) plus deterministic output keys.
*   **The Method:** the worker acks **only after** the derivatives are in MinIO and Redis is
    updated. Kill a worker mid-encode and RabbitMQ redelivers the unacked message to another
    replica when the connection drops. Because output keys derive purely from the job ID,
    redelivery overwrites rather than duplicating — at-least-once delivery becomes safe.

**5. Bounded retry with a dead-letter path**

RabbitMQ has no native "retry in 10 seconds." Requeuing immediately just spins a poison
message at 100% CPU.
*   **The Instrument:** a dead-letter exchange feeding a **TTL delay queue**, plus the
    `x-death` header.
*   **The Method:** a failure `nack(requeue: false)`s the message into a retry queue that has
    no consumers and a message TTL. When it expires, RabbitMQ dead-letters it **back** to the
    work queue — preserving the original routing key, so it lands where it came from. The
    worker reads its attempt count from `x-death` and, once `MAX_ATTEMPTS` is reached (or the
    error is non-retryable, like a corrupt file), **parks** it in a terminal queue and marks
    the job failed. Transient failures heal themselves; corrupt files stop wasting CPU.

**6. Fan-out with an atomic barrier**

One video becomes three renditions. They should encode in parallel on different machines —
but the master playlist can only be written once all of them exist.
*   **The Instrument:** a topic exchange for fan-out + Redis `HINCRBY` as the barrier.
*   **The Method:** a fast "plan" job probes the source, picks a ladder that **never
    upscales**, and publishes one rendition job per rung. Each finishing rendition increments
    a counter; the worker whose increment *returns* the expected total is the one that writes
    `master.m3u8`. Using the increment's return value rather than a read-then-compare is what
    makes it race-free across replicas.

**7. Graceful shutdown**

Stopping a worker abruptly abandons a half-written HLS ladder and a job stuck at
`processing`.
*   **The Instrument:** SIGTERM handlers + Docker's `stop_grace_period`.
*   **The Method:** on SIGTERM the worker cancels its consumers (no new deliveries), finishes
    the in-flight transcode, acks it, and closes cleanly. Temp directories are removed in
    `finally` on every path, including the parked-failure path.

**8. Observability across a process boundary**

When the API and the worker are different processes in different containers, "why was this
upload slow?" is unanswerable from logs alone.
*   **The Instrument:** `prom-client` + Prometheus + Grafana, and **OpenTelemetry** → Jaeger.
*   **The Method:** metrics cover queue depth, transcode duration per rendition, retry and
    park rates, and worker busy ratio — with Prometheus finding worker replicas by DNS so
    scraping scales with `--scale`. Tracing goes further: the amqplib instrumentation
    propagates trace context **through the message headers**, so a single Jaeger trace spans
    the HTTP upload, the publish, and the ffmpeg encode that happened seconds later in a
    different container.

### Tech stack

*   **Queue/Storage/State:** RabbitMQ (topic exchange, DLX, TTL delay queue), MinIO
    (S3-compatible object storage), Redis (job state + pub/sub).
*   **Node libraries:** `fastify`, `@fastify/multipart`, `amqplib`, `@aws-sdk/client-s3` +
    `lib-storage`, `sharp`, `fluent-ffmpeg`, `ioredis`, `zod`, `pino`, `prom-client`.
*   **Observability:** Prometheus, Grafana, Jaeger, RabbitMQ management UI, MinIO console,
    RedisInsight.
*   **Testing:** `vitest` (unit) + `testcontainers` (RabbitMQ + MinIO + Redis + the worker image).

### Quickstart

```bash
cp .env.example .env         # defaults work out of the box
npm install
npm run build:worker         # bakes ffmpeg into the worker image
WORKERS=4 npm run up:workers # infra + 4 transcode workers
npm run infra:init           # creates buckets + AMQP topology, verifies connectivity

npm run dev:api              # the API on the host
```

Then open:

*   **API + dashboard:** http://localhost:3000 — drop in an image or a video and watch it transcode
*   **RabbitMQ:** http://localhost:15672 → guest/guest
*   **MinIO console:** http://localhost:9001 · **RedisInsight:** http://localhost:5540
*   **Grafana:** http://localhost:3001 → admin/admin · **Prometheus:** http://localhost:9090
*   **Jaeger:** http://localhost:16686

`Makefile` mirrors every `npm run` script if you prefer `make`.

To stop: `Ctrl-C` the API and `npm run down` for the containers. Workers drain their in-flight
job on SIGTERM; killing one outright is also safe — the job is redelivered to another replica.

### Tests

```bash
npm test                     # unit tests — pure logic and injected fakes, no infra
npm run test:integration     # RabbitMQ + MinIO + Redis + the built worker image, via testcontainers
npm run typecheck            # tsc --noEmit
npm run lint
```

Unit tests sit beside their source as `*.test.ts`; the container-backed suite lives in
`tests/integration/` and asserts the whole path — upload → queue → transcode → derivatives in
MinIO, the retry loop incrementing `x-death`, parking after the attempt limit, and a SIGTERM
mid-transcode redelivering with no loss.

There is no module mocking anywhere — dependencies are injected, so tests either pass a plain
fake object or run against a real container. Fakes are reserved for what a real dependency
can't do on cue: failure injection and timer control.

### Status

Greenfield. The build spec and Claude Code configuration are in place; the 13 implementation
steps in [IMPLEMENTATION.md](IMPLEMENTATION.md) are next.
