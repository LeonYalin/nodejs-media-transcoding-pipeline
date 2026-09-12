import type { FastifyReply, FastifyRequest } from "fastify";
import type { JobsRepository } from "../lib/jobs-repository.js";
import { jobEventsChannel } from "../lib/jobs-repository.js";
import type { JobEventsRequest } from "./contracts.js";

/**
 * The slice of an ioredis client an SSE stream needs. Structural so tests can
 * drive the route with a fake emitter instead of a live Redis.
 */
export interface SseSubscriber {
  subscribe(channel: string): Promise<unknown>;
  unsubscribe(channel: string): Promise<unknown>;
  on(event: "message", listener: (channel: string, message: string) => void): unknown;
  quit(): Promise<unknown>;
}

/**
 * Tracks open SSE streams so shutdown can end them.
 *
 * A hijacked, actively-streaming response is never "idle", so Fastify's
 * `close()` waits on it forever -- SIGTERM would hang until the orchestrator
 * escalates to SIGKILL, losing the trace flush and the clean AMQP close. The
 * entrypoint drains these first, which is what lets `app.close()` resolve.
 */
export interface SseRegistry {
  add(close: () => void): () => void;
  closeAll(): void;
  size(): number;
}

export function createSseRegistry(): SseRegistry {
  const open = new Set<() => void>();

  function add(close: () => void): () => void {
    open.add(close);
    return () => open.delete(close);
  }

  function closeAll(): void {
    // Copy first: each close() unregisters itself from the live set.
    for (const close of [...open]) close();
    open.clear();
  }

  function size(): number {
    return open.size;
  }

  return { add, closeAll, size };
}

export interface SseDeps {
  /** Must return a *dedicated* connection: a subscribed client cannot run commands. */
  createSubscriber: () => SseSubscriber;
  /** Read-only on purpose: a subscriber-mode connection must never reach the write path. */
  jobsRepository: Pick<JobsRepository, "getJob">;
  registry?: SseRegistry;
  heartbeatMs?: number;
}

const HEARTBEAT_DEFAULT_MS = 15_000;

export function createSseHandler({
  createSubscriber,
  jobsRepository,
  registry,
  heartbeatMs = HEARTBEAT_DEFAULT_MS,
}: SseDeps) {
  return async function sseHandler(
    request: FastifyRequest<JobEventsRequest>,
    reply: FastifyReply,
  ): Promise<void> {
    const jobId = request.params.id;
    const channel = jobEventsChannel(jobId);

    // Checked *before* hijacking, so an unknown id gets a normal 404 instead of
    // a dedicated Redis connection subscribed to a channel nobody will publish
    // on. Records expire with the job TTL, so this also covers "too old".
    if (!(await jobsRepository.getJob(jobId))) {
      return reply.status(404).send({ error: "Job not found" });
    }

    // Hand the socket to us: without this Fastify would also try to send a
    // response and warn that the reply was already sent.
    reply.hijack();
    reply.raw.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      // Disables proxy buffering, which otherwise holds events until a buffer fills.
      "x-accel-buffering": "no",
    });

    const write = (chunk: string) => {
      // The client can vanish between an event arriving and this write.
      if (!reply.raw.writableEnded) reply.raw.write(chunk);
    };

    const subscriber = createSubscriber();
    let heartbeat: NodeJS.Timeout | undefined;
    let unregister: (() => void) | undefined;
    let closed = false;

    const teardown = () => {
      if (closed) return;
      closed = true;
      clearInterval(heartbeat);
      unregister?.();

      // `quit` runs in `finally`, not chained off `unsubscribe`: on a dropped
      // connection `unsubscribe` rejects, and chaining would skip `quit` and
      // leak the socket -- in exactly the case where releasing it matters most.
      void subscriber
        .unsubscribe(channel)
        .catch((error: unknown) => {
          request.log.warn({ err: error, jobId }, "SSE unsubscribe failed");
        })
        .finally(() => {
          void subscriber.quit().catch((error: unknown) => {
            request.log.warn({ err: error, jobId }, "SSE subscriber quit failed");
          });
        });
    };

    const endStream = () => {
      teardown();
      if (!reply.raw.writableEnded) reply.raw.end();
    };

    // Registered before any await so an immediate disconnect still cleans up.
    request.raw.on("close", teardown);
    // A socket error (a proxy sending RST) would otherwise be an unhandled
    // 'error' event on the raw response.
    reply.raw.on("error", teardown);

    try {
      subscriber.on("message", (received, message) => {
        if (received === channel) write(`data: ${message}\n\n`);
      });

      // Subscribe *before* reading the snapshot, so an update landing between
      // the two is delivered rather than lost.
      await subscriber.subscribe(channel);

      const record = await jobsRepository.getJob(jobId);
      if (record) write(`data: ${JSON.stringify(record)}\n\n`);

      heartbeat = setInterval(() => write(": heartbeat\n\n"), heartbeatMs);
      unregister = registry?.add(endStream);
    } catch (error) {
      request.log.error({ err: error, jobId }, "SSE stream failed to start");
      endStream();
    }
  };
}
