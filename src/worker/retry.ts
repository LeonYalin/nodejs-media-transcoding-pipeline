import type { XDeath } from "amqplib";
import { isRetryable } from "../domain/media.js";

export type ParkReason = "non-retryable" | "max-attempts";

export type RetryDecision =
  { action: "retry"; attempt: number } | { action: "park"; attempt: number; reason: ParkReason };

/**
 * The slice of amqplib's message headers this module reads, declared
 * structurally so tests can pass plain literals.
 */
export interface DeathHeaders {
  "x-death"?: readonly Pick<XDeath, "queue" | "reason" | "count">[];
}

export interface RetryInput {
  error: unknown;
  headers: DeathHeaders | undefined;
  /** The work queue that delivered the message -- not `q.retry`. */
  queue: string;
  maxAttempts: number;
}

/**
 * How many times *this* work queue has already rejected the message.
 *
 * Selected by queue and reason, never by position: after one round-trip the
 * broker writes `x-death[0] = { queue: q.retry, reason: expired }` ahead of the
 * `q.image / rejected` entry, so `x-death[0].count` counts the wrong thing.
 */
export function countRejections(headers: DeathHeaders | undefined, queue: string): number {
  const entry = headers?.["x-death"]?.find(
    (death) => death.queue === queue && death.reason === "rejected",
  );
  return entry?.count ?? 0;
}

/**
 * Retry through the delay queue, or park for good. Pure, so every branch of the
 * poison-message defence is unit-tested without a broker.
 */
export function decideRetry({ error, headers, queue, maxAttempts }: RetryInput): RetryDecision {
  const attempt = countRejections(headers, queue) + 1;

  // Checked first so a deterministic failure is reported as what it is, even
  // when it also happens to land on the last attempt.
  if (!isRetryable(error)) return { action: "park", attempt, reason: "non-retryable" };
  if (attempt >= maxAttempts) return { action: "park", attempt, reason: "max-attempts" };
  return { action: "retry", attempt };
}
