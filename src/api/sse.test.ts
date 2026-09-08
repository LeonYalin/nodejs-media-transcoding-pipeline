import { EventEmitter } from "node:events";
import type { FastifyReply, FastifyRequest } from "fastify";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { JobRecord } from "../domain/job.js";
import { jobEventsChannel } from "../lib/jobs-repository.js";
import { createSseHandler, createSseRegistry, type SseSubscriber } from "./sse.js";

const JOB_ID = "11111111-1111-4111-8111-111111111111";

const record: JobRecord = {
  jobId: JOB_ID,
  status: "processing",
  type: "video",
  sourceKey: `${JOB_ID}/source.mp4`,
  mime: "video/mp4",
  bytes: 100,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  progress: 25,
};

/** Records subscribe/unsubscribe/quit and lets a test push messages in. */
function createFakeSubscriber() {
  const emitter = new EventEmitter();
  const subscribed: string[] = [];
  const unsubscribed: string[] = [];
  let quit = false;

  const subscriber: SseSubscriber = {
    async subscribe(channel) {
      subscribed.push(channel);
      return 1;
    },
    async unsubscribe(channel) {
      unsubscribed.push(channel);
      return 1;
    },
    on(event, listener) {
      emitter.on(event, listener);
      return subscriber;
    },
    async quit() {
      quit = true;
      return "OK";
    },
  };

  return {
    subscriber,
    subscribed,
    unsubscribed,
    hasQuit: () => quit,
    push: (channel: string, message: string) => emitter.emit("message", channel, message),
  };
}

function createFakeExchange() {
  const written: string[] = [];
  let hijacked = false;
  let head: { status: number; headers: Record<string, string> } | undefined;
  let sent: { status: number; body: unknown } | undefined;

  const rawRequest = new EventEmitter();
  const rawReply = Object.assign(new EventEmitter(), {
    writableEnded: false,
    writeHead(status: number, headers: Record<string, string>) {
      head = { status, headers };
    },
    write(chunk: string) {
      written.push(chunk);
      return true;
    },
    end() {
      rawReply.writableEnded = true;
    },
  });

  const request = {
    params: { id: JOB_ID },
    raw: rawRequest,
    log: { warn: () => undefined, error: () => undefined },
  } as unknown as FastifyRequest<{ Params: { id: string } }>;

  const reply = {
    hijack() {
      hijacked = true;
    },
    status(code: number) {
      return {
        send(body: unknown) {
          sent = { status: code, body };
        },
      };
    },
    raw: rawReply,
  } as unknown as FastifyReply;

  return {
    request,
    reply,
    written,
    rawReply,
    getHead: () => head,
    getSent: () => sent,
    wasHijacked: () => hijacked,
    disconnect: () => rawRequest.emit("close"),
    socketError: () => rawReply.emit("error", new Error("ECONNRESET")),
  };
}

describe("createSseHandler", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("hijacks the reply and writes SSE headers", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
    })(exchange.request, exchange.reply);

    // Without hijack, Fastify would also try to send a response.
    expect(exchange.wasHijacked()).toBe(true);
    expect(exchange.getHead()?.status).toBe(200);
    expect(exchange.getHead()?.headers["content-type"]).toBe("text/event-stream");
  });

  it("subscribes to the job's channel and sends the current record as a snapshot", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
    })(exchange.request, exchange.reply);

    expect(subscriber.subscribed).toEqual([jobEventsChannel(JOB_ID)]);
    expect(exchange.written).toEqual([`data: ${JSON.stringify(record)}\n\n`]);
  });

  it("relays events for its own job and ignores other channels", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
    })(exchange.request, exchange.reply);

    const afterSnapshot = exchange.written.length;
    subscriber.push("job:other:events", '{"progress":99}');
    subscriber.push(jobEventsChannel(JOB_ID), '{"progress":50}');

    expect(exchange.written.slice(afterSnapshot)).toEqual(['data: {"progress":50}\n\n']);
  });

  it("emits a comment heartbeat on the configured interval", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
      heartbeatMs: 1_000,
    })(exchange.request, exchange.reply);

    await vi.advanceTimersByTimeAsync(3_000);

    expect(exchange.written.filter((chunk) => chunk === ": heartbeat\n\n")).toHaveLength(3);
  });

  it("unsubscribes, quits its connection and stops the heartbeat when the client leaves", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
      heartbeatMs: 1_000,
    })(exchange.request, exchange.reply);

    exchange.disconnect();
    await vi.advanceTimersByTimeAsync(5_000);

    expect(subscriber.unsubscribed).toEqual([jobEventsChannel(JOB_ID)]);
    expect(subscriber.hasQuit()).toBe(true);
    // A leaked interval would keep writing to a dead socket forever.
    expect(exchange.written.filter((chunk) => chunk === ": heartbeat\n\n")).toHaveLength(0);
  });

  it("never writes to a socket that has already ended", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
    })(exchange.request, exchange.reply);

    const afterSnapshot = exchange.written.length;
    exchange.rawReply.writableEnded = true;
    subscriber.push(jobEventsChannel(JOB_ID), '{"progress":50}');

    expect(exchange.written).toHaveLength(afterSnapshot);
  });

  it("404s an unknown job without hijacking or opening a subscription", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => null },
    })(exchange.request, exchange.reply);

    expect(exchange.getSent()?.status).toBe(404);
    // No hijack, and above all no Redis connection for a channel nobody publishes to.
    expect(exchange.wasHijacked()).toBe(false);
    expect(subscriber.subscribed).toEqual([]);
  });

  it("tears down on a socket error, not only on a clean client close", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
    })(exchange.request, exchange.reply);

    exchange.socketError();
    await vi.advanceTimersByTimeAsync(0);

    expect(subscriber.hasQuit()).toBe(true);
  });

  it("still quits the connection when unsubscribe rejects", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();
    subscriber.subscriber.unsubscribe = () => Promise.reject(new Error("connection gone"));

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
    })(exchange.request, exchange.reply);

    exchange.disconnect();
    await vi.advanceTimersByTimeAsync(0);

    // Chaining quit off unsubscribe would leak the connection in exactly this case.
    expect(subscriber.hasQuit()).toBe(true);
  });

  it("registry.closeAll ends live streams so shutdown is not blocked on them", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();
    const registry = createSseRegistry();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
      registry,
    })(exchange.request, exchange.reply);

    expect(registry.size).toBe(1);

    registry.closeAll();
    await vi.advanceTimersByTimeAsync(0);

    expect(exchange.rawReply.writableEnded).toBe(true);
    expect(subscriber.hasQuit()).toBe(true);
    expect(registry.size).toBe(0);
  });

  it("a client disconnect deregisters the stream from the registry", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();
    const registry = createSseRegistry();

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
      registry,
    })(exchange.request, exchange.reply);

    exchange.disconnect();

    expect(registry.size).toBe(0);
  });

  it("tears the subscriber down if the stream fails to start", async () => {
    const subscriber = createFakeSubscriber();
    const exchange = createFakeExchange();
    subscriber.subscriber.subscribe = () => Promise.reject(new Error("redis down"));

    await createSseHandler({
      createSubscriber: () => subscriber.subscriber,
      jobsRepository: { getJob: async () => record },
    })(exchange.request, exchange.reply);

    await vi.advanceTimersByTimeAsync(0);

    expect(subscriber.hasQuit()).toBe(true);
    expect(exchange.rawReply.writableEnded).toBe(true);
  });
});
