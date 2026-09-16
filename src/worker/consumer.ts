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
import { EXCHANGES, QUEUES, ROUTING_KEYS } from "../lib/topology.js";
import { countPriorAttempts, decideRetry } from "./retry.js";

/** The `type` label on every worker metric. */
function mediaTypeOf(routingKey: string): "image" | "video" {
  return routingKey === ROUTING_KEYS.IMAGE_TRANSFORM ? "image" : "video";
}

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
  /** In-flight limit of the single consumer: 1 = one job per container. */
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
 * One consumer on q.work, dispatch by routing key, ack-after-success, and the
 * retry-vs-park settlement of every failure.
 */
export function createJobConsumer({
  channel,
  jobHandlers,
  jobsRepository,
  maxAttempts,
  prefetch = 1,
}: JobConsumerDeps) {
  /**
   * The payload carries no stage discriminator (see domain/job.ts): the routing
   * key decides the schema and the handler. Returns the `rendition` label for
   * media_transcode_duration_seconds, which only the parsed body can name.
   */
  async function runHandler(routingKey: string, content: Buffer): Promise<string> {
    switch (routingKey) {
      case ROUTING_KEYS.IMAGE_TRANSFORM:
        await jobHandlers.image(parseBody(ImageJobMessageSchema, content));
        return "image";
      case ROUTING_KEYS.VIDEO_PLAN:
        await jobHandlers.videoPlan(parseBody(VideoJobMessageSchema, content));
        return "plan";
      case ROUTING_KEYS.VIDEO_RENDITION: {
        const message = parseBody(VideoRenditionJobMessageSchema, content);
        await jobHandlers.videoRendition(message);
        return message.rendition.name;
      }
      default:
        throw new InvalidJobMessageError(`No handler for routing key "${routingKey}"`);
    }
  }

  let consumerTag: string | null = null;
  const inFlight = new Set<Promise<void>>();

  async function park(message: ConsumeMessage, detail: string): Promise<void> {
    // The publisher sets messageId = jobId, so even an unparseable body can
    // have its record marked. Best-effort: Redis must not block parking.
    const jobId: unknown = message.properties.messageId;
    if (typeof jobId === "string") {
      await jobsRepository.markFailed(jobId, detail).catch((error: unknown) => {
        logger.warn({ err: error, jobId }, "Could not mark parked job failed");
      });
    }

    // A replayed copy must not arrive with its crashes already counted: the
    // broker keeps a published `x-delivery-count` rather than resetting it.
    const headers: Record<string, unknown> = {
      ...message.properties.headers,
      "park-queue": QUEUES.WORK,
      "park-error": detail,
    };
    delete headers["x-delivery-count"];

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
          headers,
        },
        (error) => (error ? reject(error) : resolve()),
      );
    });
    channel.ack(message);
  }

  async function settleFailure(message: ConsumeMessage, error: unknown) {
    const { routingKey } = message.fields;
    const decision = decideRetry({
      error,
      headers: message.properties.headers,
      queue: QUEUES.WORK,
      maxAttempts,
    });
    const log = logger.child({ routingKey, jobId: message.properties.messageId, ...decision });

    if (decision.action === "retry") {
      log.warn({ err: error }, "Job failed; retrying after delay");
      mediaMetrics.retriesTotal.inc({ routing_key: routingKey });
      // requeue:false dead-letters into media.retry. requeue:true would spin the
      // same failure straight back, with no delay and no attempt count.
      channel.nack(message, false, false);
      return;
    }

    log.error({ err: error }, "Job parked");
    mediaMetrics.parkedTotal.inc({ routing_key: routingKey, reason: decision.reason });
    mediaMetrics.jobsTotal.inc({ type: mediaTypeOf(routingKey), status: "failed" });
    const reason = error instanceof Error ? error.message : String(error);
    await park(message, `${decision.reason}: ${reason}`);
  }

  async function handleDelivery(message: ConsumeMessage): Promise<void> {
    const { routingKey } = message.fields;
    const type = mediaTypeOf(routingKey);
    // Gauge, not a flag: at prefetch 1 this is the 0/1 the dashboard reads, and
    // it still tells the truth if the limit is ever raised.
    mediaMetrics.workerBusy.inc();
    const stopTimer = mediaMetrics.transcodeDuration.startTimer({ type });

    try {
      // A worker that crashed never reached `catch`, so its attempt was never
      // judged. Once crashes have used up the attempts, fail without running
      // the job again -- settleFailure then parks it as max-attempts.
      const priorAttempts = countPriorAttempts(message.properties.headers, QUEUES.WORK);
      if (priorAttempts >= maxAttempts) {
        throw new Error(`Worker crashed or lost its connection; ${priorAttempts} attempts used`);
      }

      // Timed only on success: a failure's duration is the time to the error,
      // which would drag the encode percentiles down.
      stopTimer({ rendition: await runHandler(routingKey, message.content) });
      mediaMetrics.jobsTotal.inc({ type, status: "completed" });
      channel.ack(message);
    } catch (error) {
      await settleFailure(message, error).catch((settleError: unknown) => {
        // Only reachable when the channel itself is failing. The broker requeues
        // every unacked delivery when a channel closes, and deterministic output
        // keys make the rerun overwrite rather than duplicate.
        logger.error({ err: settleError, routingKey }, "Could not settle delivery");
      });
    } finally {
      mediaMetrics.workerBusy.dec();
    }
  }

  async function start(): Promise<void> {
    // Not `global`: RabbitMQ 4.x denies global QoS. One consumer makes the
    // per-consumer limit the per-container limit.
    await channel.prefetch(prefetch);

    const consumer = await channel.consume(
      QUEUES.WORK,
      (message) => {
        // null = the broker cancelled us (queue deleted). Nothing to settle.
        if (!message) return logger.error("Consumer cancelled by broker");
        const delivery = handleDelivery(message);
        inFlight.add(delivery);
        return delivery.finally(() => inFlight.delete(delivery));
      },
      { noAck: false },
    );
    consumerTag = consumer.consumerTag;

    logger.info({ queue: QUEUES.WORK, prefetch }, "Worker consuming");
  }

  /** Stop new deliveries, then let the in-flight job finish and settle. */
  async function stop(): Promise<void> {
    // Ignored: after a lost connection the channel is already gone, and so is
    // the consumer -- there is nothing left to stop but the in-flight job.
    if (consumerTag) await channel.cancel(consumerTag).catch(() => undefined);
    consumerTag = null;
    await Promise.allSettled(inFlight);
  }

  return { start, stop };
}

export type JobConsumer = ReturnType<typeof createJobConsumer>;
