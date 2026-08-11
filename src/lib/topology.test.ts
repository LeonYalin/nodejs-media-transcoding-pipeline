import { describe, expect, it } from "vitest";
import { config } from "../config/index.js";
import {
  EXCHANGES,
  QUEUES,
  ROUTING_KEYS,
  type TopologyChannel,
  assertTopology,
} from "./topology.js";

interface Recorded {
  exchanges: { name: string; type: string; options: object }[];
  queues: { name: string; options: object }[];
  bindings: { queue: string; source: string; pattern: string }[];
}

/** A plain recording fake -- no module mocking, no casts. */
function createRecordingChannel(): { channel: TopologyChannel; recorded: Recorded } {
  const recorded: Recorded = { exchanges: [], queues: [], bindings: [] };

  const channel: TopologyChannel = {
    async assertExchange(name, type, options) {
      recorded.exchanges.push({ name, type, options });
      return {};
    },
    async assertQueue(name, options) {
      recorded.queues.push({ name, options });
      return {};
    },
    async bindQueue(queue, source, pattern) {
      recorded.bindings.push({ queue, source, pattern });
      return {};
    },
  };

  return { channel, recorded };
}

async function run() {
  const { channel, recorded } = createRecordingChannel();
  await assertTopology(channel);
  return recorded;
}

const queueNamed = (r: Recorded, name: string) => r.queues.find((q) => q.name === name);
const args = (options: object) =>
  (options as { arguments?: Record<string, unknown> }).arguments ?? {};

describe("assertTopology", () => {
  it("declares every exchange durable and topic-typed", async () => {
    const recorded = await run();

    expect(recorded.exchanges).toEqual(
      [EXCHANGES.JOBS, EXCHANGES.RETRY, EXCHANGES.PARKED].map((name) => ({
        name,
        type: "topic",
        options: { durable: true },
      })),
    );
  });

  it("declares every queue durable, so a broker restart keeps queued jobs", async () => {
    const recorded = await run();

    expect(recorded.queues).toHaveLength(5);
    for (const queue of recorded.queues) {
      expect(queue.options).toMatchObject({ durable: true });
    }
  });

  it("dead-letters each work queue into the retry exchange", async () => {
    const recorded = await run();

    for (const name of [QUEUES.IMAGE, QUEUES.VIDEO_PLAN, QUEUES.VIDEO_RENDITION]) {
      const queue = queueNamed(recorded, name);
      expect(args(queue!.options)["x-dead-letter-exchange"]).toBe(EXCHANGES.RETRY);
    }
  });

  it("takes the retry TTL from config rather than hardcoding it", async () => {
    const recorded = await run();

    expect(args(queueNamed(recorded, QUEUES.RETRY)!.options)).toEqual({
      "x-message-ttl": config.RETRY_TTL_MS,
      "x-dead-letter-exchange": EXCHANGES.JOBS,
    });
  });

  it("routes the retry queue back to the jobs exchange, closing the loop", async () => {
    const recorded = await run();
    const retryArgs = args(queueNamed(recorded, QUEUES.RETRY)!.options);

    // Back to media.jobs, and NO x-dead-letter-routing-key -- the original key
    // must survive so the message returns to the queue it came from.
    expect(retryArgs["x-dead-letter-exchange"]).toBe(EXCHANGES.JOBS);
    expect(retryArgs).not.toHaveProperty("x-dead-letter-routing-key");
    expect(recorded.bindings).toContainEqual({
      queue: QUEUES.RETRY,
      source: EXCHANGES.RETRY,
      pattern: "#",
    });
  });

  it("leaves the parked queue terminal -- no onward dead-lettering", async () => {
    const recorded = await run();

    expect(args(queueNamed(recorded, QUEUES.PARKED)!.options)).toEqual({});
    expect(recorded.bindings).toContainEqual({
      queue: QUEUES.PARKED,
      source: EXCHANGES.PARKED,
      pattern: "#",
    });
  });

  it("binds each work queue to its own routing key", async () => {
    const recorded = await run();

    expect(recorded.bindings).toEqual(
      expect.arrayContaining([
        { queue: QUEUES.IMAGE, source: EXCHANGES.JOBS, pattern: ROUTING_KEYS.IMAGE_TRANSFORM },
        { queue: QUEUES.VIDEO_PLAN, source: EXCHANGES.JOBS, pattern: ROUTING_KEYS.VIDEO_PLAN },
        {
          queue: QUEUES.VIDEO_RENDITION,
          source: EXCHANGES.JOBS,
          pattern: ROUTING_KEYS.VIDEO_RENDITION,
        },
      ]),
    );
  });

  it("is safe to call repeatedly -- assert* is idempotent by contract", async () => {
    const { channel, recorded } = createRecordingChannel();
    await assertTopology(channel);
    await assertTopology(channel);

    expect(recorded.queues).toHaveLength(10);
  });
});
