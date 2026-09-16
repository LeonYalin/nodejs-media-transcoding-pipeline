import type { ConsumeMessage, Options, Replies } from "amqplib";
import type { z } from "zod";
import {
  ImageJobMessageSchema,
  VideoJobMessageSchema,
  VideoRenditionJobMessageSchema,
  type ImageJobMessage,
  type VideoJobMessage,
  type VideoRenditionJobMessage,
} from "../domain/job.js";
import { InvalidJobMessageError } from "../domain/media.js";
import type { JobsRepository } from "../lib/jobs-repository.js";
import { logger } from "../lib/logger.js";
import { mediaMetrics } from "../lib/metrics.js";
import { EXCHANGES, QUEUES } from "../lib/topology.js";
import { decideRetry } from "./retry.js";

/** The `type` label on every worker metric; the delivering queue decides it. */
const MEDIA_TYPE_BY_QUEUE: Record<string, "image" | "video"> = {
  [QUEUES.IMAGE]: "image",
  [QUEUES.VIDEO_PLAN]: "video",
  [QUEUES.VIDEO_RENDITION]: "video",
};

/**
 * The slice of amqplib's ConfirmChannel the consumer uses, declared structurally
 * so tests drive it with a recording fake (see topology.ts for the same pattern).
 */
export interface ConsumerChannel {
  prefetch(count: number, global?: boolean): Promise<unknown>;
  /** amqplib ignores `onMessage`'s return value; the test fake awaits it. */
  consume(
    queue: string,
    onMessage: (message: ConsumeMessage | null) => unknown,
    options?: Options.Consume,
  ): Promise<Replies.Consume>;
  cancel(consumerTag: string): Promise<unknown>;
  ack(message: ConsumeMessage): void;
  nack(message: ConsumeMessage, allUpTo?: boolean, requeue?: boolean): void;
  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Options.Publish,
    callback: (error: unknown) => void,
  ): boolean;
}

/** One per work queue. Each resolves only once derivatives are uploaded and Redis is updated. */
export interface JobHandlers {
  image(message: ImageJobMessage): Promise<void>;
  videoPlan(message: VideoJobMessage): Promise<void>;
  videoRendition(message: VideoRenditionJobMessage): Promise<void>;
}

export interface JobConsumerDeps {
  /**
   * An open confirm channel. It is not re-opened here: if it closes, the
   * entrypoint exits and Docker restarts the container, and the broker
   * redelivers whatever was unacked.
   */
  channel: ConsumerChannel;
  jobHandlers: JobHandlers;
  jobsRepository: Pick<JobsRepository, "markFailed">;
  maxAttempts: number;
  /** Channel-wide in-flight limit shared by all three consumers: 1 = one job per container. */
  prefetch?: number;
}

/** Any unparseable body is deterministic, so it must park rather than retry. */
function parseBody<T>(schema: z.ZodType<T>, content: Buffer): T {
  try {
    return schema.parse(JSON.parse(content.toString("utf8")));
  } catch (error) {
    throw new InvalidJobMessageError("Message is not a valid job", { cause: error });
  }
}

/**
 * Three consumers on one channel, ack-after-success, and the retry-vs-park
 * settlement of every failure.
 */
export function createJobConsumer({
  channel,
  jobHandlers,
  jobsRepository,
  maxAttempts,
  prefetch = 1,
}: JobConsumerDeps) {
  // The payload carries no stage discriminator (see domain/job.ts): the
  // delivering queue decides the schema. Each returns the `rendition` label for
  // media_transcode_duration_seconds, which only the parsed body can name.
  const handlerByQueue: Record<string, (content: Buffer) => Promise<string>> = {
    [QUEUES.IMAGE]: async (content) => {
      await jobHandlers.image(parseBody(ImageJobMessageSchema, content));
      return "image";
    },
    [QUEUES.VIDEO_PLAN]: async (content) => {
      await jobHandlers.videoPlan(parseBody(VideoJobMessageSchema, content));
      return "plan";
    },
    [QUEUES.VIDEO_RENDITION]: async (content) => {
      const message = parseBody(VideoRenditionJobMessageSchema, content);
      await jobHandlers.videoRendition(message);
      return message.rendition.name;
    },
  };

  const consumerTags: string[] = [];
  const inFlight = new Set<Promise<void>>();

  async function park(message: ConsumeMessage, queue: string, detail: string): Promise<void> {
    // The publisher sets messageId = jobId, so even an unparseable body can
    // have its record marked. Best-effort: Redis must not block parking.
    const jobId: unknown = message.properties.messageId;
    if (typeof jobId === "string") {
      await jobsRepository.markFailed(jobId, detail).catch((error: unknown) => {
        logger.warn({ err: error, jobId }, "Could not mark parked job failed");
      });
    }

    // Confirmed before the ack: the parked copy must exist before the original
    // is released, or a broker hiccup loses the message outright.
    await new Promise<void>((resolve, reject) => {
      channel.publish(
        EXCHANGES.PARKED,
        message.fields.routingKey,
        message.content,
        {
          persistent: true,
          contentType: message.properties.contentType,
          messageId: message.properties.messageId,
          headers: { ...message.properties.headers, "park-queue": queue, "park-error": detail },
        },
        (error) => (error ? reject(error) : resolve()),
      );
    });
    channel.ack(message);
  }

  async function settleFailure(message: ConsumeMessage, queue: string, error: unknown) {
    const decision = decideRetry({
      error,
      headers: message.properties.headers,
      queue,
      maxAttempts,
    });
    const log = logger.child({ queue, jobId: message.properties.messageId, ...decision });

    if (decision.action === "retry") {
      log.warn({ err: error }, "Job failed; retrying after delay");
      mediaMetrics.retriesTotal.inc({ queue });
      // requeue:false dead-letters into media.retry. requeue:true would spin the
      // same failure straight back, with no delay and no attempt count.
      channel.nack(message, false, false);
      return;
    }

    log.error({ err: error }, "Job parked");
    mediaMetrics.parkedTotal.inc({ queue, reason: decision.reason });
    mediaMetrics.jobsTotal.inc({ type: MEDIA_TYPE_BY_QUEUE[queue], status: "failed" });
    const reason = error instanceof Error ? error.message : String(error);
    await park(message, queue, `${decision.reason}: ${reason}`);
  }

  async function handleDelivery(queue: string, message: ConsumeMessage): Promise<void> {
    const type = MEDIA_TYPE_BY_QUEUE[queue];
    // Gauge, not a flag: at prefetch 1 this is the 0/1 the dashboard reads, and
    // it still tells the truth if the limit is ever raised.
    mediaMetrics.workerBusy.inc();
    const stopTimer = mediaMetrics.transcodeDuration.startTimer({ type });

    try {
      // Timed only on success: a failure's duration is the time to the error,
      // which would drag the encode percentiles down.
      stopTimer({ rendition: await handlerByQueue[queue](message.content) });
      mediaMetrics.jobsTotal.inc({ type, status: "completed" });
      channel.ack(message);
    } catch (error) {
      await settleFailure(message, queue, error).catch((settleError: unknown) => {
        // Only reachable when the channel itself is failing. The broker requeues
        // every unacked delivery when a channel closes, and deterministic output
        // keys make the rerun overwrite rather than duplicate.
        logger.error({ err: settleError, queue }, "Could not settle delivery");
      });
    } finally {
      mediaMetrics.workerBusy.dec();
    }
  }

  async function start(): Promise<void> {
    // `global: true` makes the limit channel-wide, so the three consumers share
    // one slot. Without it RabbitMQ applies it per consumer, and one container
    // would run an image, a plan and a rendition at once.
    await channel.prefetch(prefetch, true);

    for (const queue of Object.keys(handlerByQueue)) {
      const { consumerTag } = await channel.consume(
        queue,
        (message) => {
          // null = the broker cancelled us (queue deleted). Nothing to settle.
          if (!message) return logger.error({ queue }, "Consumer cancelled by broker");
          const delivery = handleDelivery(queue, message);
          inFlight.add(delivery);
          return delivery.finally(() => inFlight.delete(delivery));
        },
        { noAck: false },
      );
      consumerTags.push(consumerTag);
    }

    logger.info({ queues: Object.keys(handlerByQueue), prefetch }, "Worker consuming");
  }

  /** Stop new deliveries, then let the in-flight job finish and settle. */
  async function stop(): Promise<void> {
    await Promise.allSettled(consumerTags.map((tag) => channel.cancel(tag)));
    consumerTags.length = 0;
    await Promise.allSettled(inFlight);
  }

  return { start, stop };
}

export type JobConsumer = ReturnType<typeof createJobConsumer>;
