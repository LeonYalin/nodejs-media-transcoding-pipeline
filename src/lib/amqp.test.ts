import { EventEmitter } from "node:events";
import type { ConfirmChannel } from "amqplib";
import { describe, expect, it } from "vitest";
import type { JobMessage } from "../domain/job.js";
import { createConfirmChannelProvider, createJobPublisher } from "./amqp.js";
import { EXCHANGES, ROUTING_KEYS } from "./topology.js";

interface Published {
  exchange: string;
  routingKey: string;
  content: Buffer;
  options: Record<string, unknown>;
}

/**
 * A confirm channel that records publishes and lets a test decide when -- and
 * whether -- the broker confirms. Real brokers cannot be made to withhold a
 * confirm on cue, which is exactly what the "fakes only for failure injection"
 * rule allows.
 */
class FakeConfirmChannel extends EventEmitter {
  readonly published: Published[] = [];
  closed = false;
  confirmError: Error | null = null;
  writeBufferFull = false;

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Record<string, unknown>,
    callback: (error: Error | null) => void,
  ): boolean {
    this.published.push({ exchange, routingKey, content, options });
    // Confirms are always asynchronous on a real broker.
    setImmediate(() => callback(this.confirmError));
    return !this.writeBufferFull;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.emit("close");
  }
}

/** Structural stand-in for amqplib's ConfirmChannel; the fake covers what we use. */
function asChannel(fake: FakeConfirmChannel): ConfirmChannel {
  return fake as unknown as ConfirmChannel;
}

function createFakeSource() {
  const created: FakeConfirmChannel[] = [];
  let failNext: Error | null = null;

  return {
    created,
    failNextWith(error: Error) {
      failNext = error;
    },
    source: {
      async createConfirmChannel(): Promise<ConfirmChannel> {
        if (failNext) {
          const error = failNext;
          failNext = null;
          throw error;
        }
        const channel = new FakeConfirmChannel();
        created.push(channel);
        return asChannel(channel);
      },
    },
  };
}

const message: JobMessage = {
  jobId: "11111111-1111-4111-8111-111111111111",
  type: "video",
  sourceKey: "11111111-1111-4111-8111-111111111111/source.mp4",
  mime: "video/mp4",
};

describe("createConfirmChannelProvider", () => {
  it("opens one channel and reuses it", async () => {
    const { source, created } = createFakeSource();
    const provider = createConfirmChannelProvider(source);

    await provider.get();
    await provider.get();

    expect(created).toHaveLength(1);
  });

  it("shares a single open across concurrent callers rather than racing", async () => {
    const { source, created } = createFakeSource();
    const provider = createConfirmChannelProvider(source);

    await Promise.all([provider.get(), provider.get(), provider.get()]);

    expect(created).toHaveLength(1);
  });

  it("re-opens after the channel closes, which is what a reconnect looks like", async () => {
    const { source, created } = createFakeSource();
    const provider = createConfirmChannelProvider(source);

    await provider.get();
    created[0].emit("close");
    await provider.get();

    expect(created).toHaveLength(2);
  });

  it("re-opens after a channel-level error instead of handing back a dead channel", async () => {
    const { source, created } = createFakeSource();
    const provider = createConfirmChannelProvider(source);

    await provider.get();
    created[0].emit("error", new Error("PRECONDITION_FAILED"));
    await provider.get();

    expect(created).toHaveLength(2);
  });

  it("does not cache a failed open", async () => {
    const { source, created, failNextWith } = createFakeSource();
    const provider = createConfirmChannelProvider(source);

    failNextWith(new Error("broker unreachable"));
    await expect(provider.get()).rejects.toThrow("broker unreachable");

    // The next call must try again rather than replay the rejection forever.
    await provider.get();
    expect(created).toHaveLength(1);
  });

  it("refuses to hand out a channel once closed", async () => {
    const { source, created } = createFakeSource();
    const provider = createConfirmChannelProvider(source);

    await provider.get();
    await provider.close();

    expect(created[0].closed).toBe(true);
    await expect(provider.get()).rejects.toThrow(/closed/);
  });
});

describe("createJobPublisher", () => {
  it("publishes persistently to the jobs exchange and resolves only on confirm", async () => {
    const { source, created } = createFakeSource();
    const provider = createConfirmChannelProvider(source);
    const publisher = createJobPublisher({ channelProvider: provider });

    await publisher.publish(ROUTING_KEYS.VIDEO_PLAN, message);

    const [published] = created[0].published;
    expect(published.exchange).toBe(EXCHANGES.JOBS);
    expect(published.routingKey).toBe(ROUTING_KEYS.VIDEO_PLAN);
    // `persistent` is what survives a broker restart -- the durability invariant.
    expect(published.options).toMatchObject({
      persistent: true,
      contentType: "application/json",
      messageId: message.jobId,
    });
    expect(JSON.parse(published.content.toString())).toEqual(message);
  });

  it("rejects when the broker nacks, so the caller never replies 202", async () => {
    const { source, created } = createFakeSource();
    const provider = createConfirmChannelProvider(source);
    const publisher = createJobPublisher({ channelProvider: provider });

    // Open the channel first so the failure can be armed on it.
    await provider.get();
    created[0].confirmError = new Error("basic.nack");

    await expect(publisher.publish(ROUTING_KEYS.IMAGE_TRANSFORM, message)).rejects.toThrow(
      "basic.nack",
    );
  });

  it("still awaits the confirm when the write buffer is full", async () => {
    const { source, created } = createFakeSource();
    const provider = createConfirmChannelProvider(source);
    const publisher = createJobPublisher({ channelProvider: provider });

    await provider.get();
    created[0].writeBufferFull = true;

    await expect(publisher.publish(ROUTING_KEYS.VIDEO_PLAN, message)).resolves.toBeUndefined();
  });

  it("refuses to publish a message that violates the wire contract", async () => {
    const { source } = createFakeSource();
    const provider = createConfirmChannelProvider(source);
    const publisher = createJobPublisher({ channelProvider: provider });

    const invalid = { ...message, mime: "application/zip" } as unknown as JobMessage;

    await expect(publisher.publish(ROUTING_KEYS.VIDEO_PLAN, invalid)).rejects.toThrow();
  });
});
