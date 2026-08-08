---
name: run-pipeline
description: Operational commands for the media transcoding pipeline — build the worker image, start/stop infra, scale workers, init buckets and AMQP topology, run the API, upload test media, generate load, find the UIs and metrics endpoints. Use whenever running, operating, or load-testing this project locally.
---

# Run the pipeline

Prereqs: **Docker running + Node 20+ — nothing else.** `npm install` done · `.env` present (copy from `.env.example`).
ffmpeg is *not* installed on the host; it lives in the worker image.

## Infra + workers (docker compose)
- Up (everything, 4 workers by default): `npm run up`
- Change the worker count: `WORKERS=8 npm run up`   # 12-core box: 4 is a good default, ~10 max
- Rebuild the worker image after a Dockerfile/dependency change: `npm run build:worker`
- Status / health: `docker compose ps`
- Logs: `docker compose logs -f <service>` · workers only: `npm run logs:worker`
- Down (keep data): `npm run down` — Down + wipe volumes: `docker compose down -v`

## Initialize buckets + queues
`npm run infra:init`   # asserts media-uploads / media-outputs and the full AMQP topology, then pings RabbitMQ, MinIO, Redis

## Run the API (host)
`npm run dev:api`

## Upload test media
- Image: `curl -F file=@path/to/photo.jpg http://localhost:3000/uploads`
- Video: `curl -F file=@path/to/clip.mp4 http://localhost:3000/uploads`
- Poll a job: `curl http://localhost:3000/jobs/<jobId>`
- Follow progress: `curl -N http://localhost:3000/jobs/<jobId>/events`

## Load test
`npm run load`   # autocannon, concurrent multipart uploads; reports p99 + 202 rate
Tune: `WORKERS=8 npm run up` first, then re-run and watch queue depth drain in Grafana.

## One-off ffmpeg / ffprobe (host stays clean)
`docker compose run --rm --entrypoint ffprobe worker -v error -show_streams /path/inside/container`
Mount a host file to inspect it: add `-v "$PWD/fixtures:/fixtures"` and point at `/fixtures/<file>`.

## Endpoints (defaults)
- API + dashboard: `http://localhost:3000` — `POST /uploads`, `GET /jobs`, `/jobs/:id`, `/jobs/:id/events`, `/healthz`, `/metrics`
- RabbitMQ management: `http://localhost:15672` (media/media) · broker metrics `:15692/metrics`
- MinIO console: `http://localhost:9001` · S3 endpoint `http://localhost:9000`
- RedisInsight: `http://localhost:5540`
- Prometheus: `http://localhost:9090` · Grafana: `http://localhost:3001` (admin/admin) · Jaeger: `http://localhost:16686`
- Worker metrics are scraped by Prometheus via DNS discovery; they are not published on host ports.

## Shutdown
`Ctrl-C` the API. `docker compose stop worker` sends SIGTERM — a worker finishes its in-flight transcode, acks, and exits within `stop_grace_period`. Killing a worker mid-job is safe: the unacked message is redelivered to another replica.

## Tests
`npm test` (unit) · `npm run test:integration` (RabbitMQ + MinIO + Redis + the built worker image via testcontainers) · `npm run typecheck` · `npm run lint`
