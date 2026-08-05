---
name: queue-ops
description: Diagnostic commands for the RabbitMQ / MinIO / Redis side of the media pipeline — queue depths and bindings, inspecting x-death headers on retried and parked messages, replaying the dead-letter queue, listing bucket objects, and reading job records and progress. Use when inspecting pipeline state or debugging stuck, retried, or failed jobs.
---

# Queue, storage & job diagnostics

## RabbitMQ
Prefix: `docker compose exec rabbitmq rabbitmqctl`

- Depths at a glance: `list_queues name messages messages_unacknowledged consumers`
- Topology check: `list_exchanges name type` · `list_bindings source_name routing_key destination_name`
- Who's connected: `list_consumers` (one per queue per worker replica)
- Purge a queue (destructive): `purge_queue q.parked`

Management HTTP API (same data as JSON, better for scripting):
```
curl -su guest:guest http://localhost:15672/api/queues/%2F | \
  node -e "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>console.table(JSON.parse(d).map(q=>({name:q.name,ready:q.messages_ready,unacked:q.messages_unacknowledged}))))"
```

### Inspect a parked message (why did it fail?)
Peek without consuming by rejecting back onto the queue:
```
curl -su guest:guest -H 'content-type: application/json' \
  -d '{"count":5,"ackmode":"reject_requeue_true","encoding":"auto"}' \
  http://localhost:15672/api/queues/%2F/q.parked/get
```
Read `properties.headers`: `x-death` (array of `{queue, reason, count}`) plus the park reason the worker attached.
**Reading the retry count:** take the entry whose `queue` is the *work* queue and whose `reason` is `rejected`. Entries with `reason: expired` belong to `q.retry` and are not attempts.

### Replay parked messages
Shovel them back onto `media.jobs`; dead-lettering preserved the original routing key, so they return to the right work queue:
```
docker compose exec rabbitmq rabbitmqctl set_parameter shovel replay-parked \
  '{"src-protocol":"amqp091","src-uri":"amqp://","src-queue":"q.parked","dest-protocol":"amqp091","dest-uri":"amqp://","dest-exchange":"media.jobs"}'
```
Clear it once drained: `docker compose exec rabbitmq rabbitmqctl clear_parameter shovel replay-parked`

## MinIO
`mc` runs in a container so the host stays clean — alias once per shell session:
```
alias mcx='docker run --rm --network media_pipeline_net -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 minio/mc'
```
- Buckets: `mcx ls local`
- A job's outputs: `mcx ls -r local/media-outputs/<jobId>/`
- The raw upload: `mcx stat local/media-uploads/<jobId>/`
- Read a playlist: `mcx cat local/media-outputs/<jobId>/hls/master.m3u8`
- Storage used: `mcx du local/media-outputs`

## Redis
Prefix: `docker compose exec redis redis-cli`

- Job record: `hgetall job:<jobId>` (`status`, `progress`, `error`, `outputs`, `renditionsExpected`, `renditionsDone`)
- Recent jobs: `zrevrange jobs:recent 0 19 WITHSCORES`
- Watch live progress: `psubscribe 'job:*:events'`
- Key count / memory: `dbsize` · `info memory`

Browsing UI and a pub/sub monitor: RedisInsight (port listed in the `run-pipeline` skill).

## Triage quick paths
- **Job stuck `queued`** → no consumers on its queue (`list_consumers`), or no workers running.
- **Job stuck `processing`** → check worker logs; if the container died the message is unacked and gets redelivered when the connection drops.
- **`q.retry` depth oscillating** → something is failing and retrying; peek `q.parked` after `MAX_ATTEMPTS` cycles.
- **`master.m3u8` never appears** → a rendition failed: `renditionsDone < renditionsExpected` and the missing one is sitting in `q.retry` or `q.parked`.
- **Uploads 202 but nothing transcodes** → confirm the bindings exist (`list_bindings`); a missing binding silently drops messages published to a topic exchange.