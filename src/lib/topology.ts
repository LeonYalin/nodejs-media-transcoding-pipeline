import { config } from "../config/index.js";

/** Canonical routing keys matching the architectural data contracts. */
export const ROUTING_KEYS = {
  IMAGE_TRANSFORM: "job.image.transform",
  VIDEO_PLAN: "job.video.plan",
  VIDEO_RENDITION: "job.video.rendition",
} as const;

export const EXCHANGES = {
  JOBS: "media.jobs",
  RETRY: "media.retry",
  PARKED: "media.parked",
} as const;

export const QUEUES = {
  WORK: "q.work",
  RETRY: "q.retry",
  PARKED: "q.parked",
} as const;

/**
 * The slice of amqplib's Channel this module needs, typed structurally so tests
 * can pass a plain recording fake instead of mocking the module.
 */
export interface TopologyChannel {
  assertExchange(exchange: string, type: string, options: object): Promise<unknown>;
  assertQueue(queue: string, options: object): Promise<unknown>;
  bindQueue(queue: string, source: string, pattern: string): Promise<unknown>;
}

/**
 * Asserts the complete AMQP topology idempotently: durable topic exchanges, the
 * work queue, the TTL-based retry path, and the terminal parked queue.
 *
 * NOTE: queue arguments are immutable once a queue exists. Changing
 * `RETRY_TTL_MS` in `.env` against a broker that already has `q.retry` will fail
 * with PRECONDITION_FAILED (406) -- delete the queue first, or the whole volume.
 */
export async function assertTopology(channel: TopologyChannel): Promise<void> {
  // 1. Core exchanges.
  await channel.assertExchange(EXCHANGES.JOBS, "topic", { durable: true });
  await channel.assertExchange(EXCHANGES.RETRY, "topic", { durable: true });
  await channel.assertExchange(EXCHANGES.PARKED, "topic", { durable: true });

  // 2. One work queue for every stage. Dead-letters into media.retry when a
  //    worker rejects a message with requeue: false.
  //
  //    One queue, because RabbitMQ 4.x denies global QoS: `prefetch(1, true)`
  //    silently becomes per-consumer, so a worker consuming three queues holds
  //    three jobs. With a single consumer, a plain prefetch(1) is one job.
  //    Quorum, because it stamps `x-delivery-count` on deliveries a crashed
  //    worker never settled -- the only way to cap a job that kills its worker.
  await channel.assertQueue(QUEUES.WORK, {
    durable: true,
    arguments: { "x-queue-type": "quorum", "x-dead-letter-exchange": EXCHANGES.RETRY },
  });

  await channel.bindQueue(QUEUES.WORK, EXCHANGES.JOBS, ROUTING_KEYS.IMAGE_TRANSFORM);
  await channel.bindQueue(QUEUES.WORK, EXCHANGES.JOBS, ROUTING_KEYS.VIDEO_PLAN);
  await channel.bindQueue(QUEUES.WORK, EXCHANGES.JOBS, ROUTING_KEYS.VIDEO_RENDITION);

  // 3. Retry path.
  //
  // Uniform per-QUEUE ttl, not per-message: RabbitMQ only expires messages from
  // the head of a queue, so with per-message TTLs a 60s message at the head
  // blocks a 5s message behind it. One queue-level TTL keeps every retry a
  // deterministic wait.
  //
  // q.retry has no consumers. A message simply expires and is dead-lettered to
  // media.jobs -- and RabbitMQ PRESERVES the original routing key across that
  // hop, so it re-enters q.work still carrying its stage's routing key.
  await channel.assertQueue(QUEUES.RETRY, {
    durable: true,
    arguments: {
      "x-message-ttl": config.RETRY_TTL_MS,
      "x-dead-letter-exchange": EXCHANGES.JOBS,
    },
  });
  await channel.bindQueue(QUEUES.RETRY, EXCHANGES.RETRY, "#");

  // 4. Terminal parked queue: poison messages and attempt-exhausted jobs. No
  //    consumers, no dead-lettering onward -- drained by hand.
  await channel.assertQueue(QUEUES.PARKED, { durable: true });
  await channel.bindQueue(QUEUES.PARKED, EXCHANGES.PARKED, "#");
}
