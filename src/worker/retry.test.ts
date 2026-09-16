import { describe, expect, it } from "vitest";
import {
  CorruptMediaError,
  InvalidJobMessageError,
  ObjectNotFoundError,
  UnsupportedMediaError,
} from "../domain/media.js";
import { QUEUES } from "../lib/topology.js";
import { countRejections, decideRetry, type DeathHeaders } from "./retry.js";

const transient = new Error("minio timeout");

function rejectedTimes(count: number): DeathHeaders {
  return {
    "x-death": [
      { queue: QUEUES.RETRY, reason: "expired", count },
      { queue: QUEUES.WORK, reason: "rejected", count },
    ],
  };
}

describe("countRejections", () => {
  it("is 0 for a message that has never been dead-lettered", () => {
    expect(countRejections(undefined, QUEUES.WORK)).toBe(0);
    expect(countRejections({}, QUEUES.WORK)).toBe(0);
  });

  it("reads this queue's rejected entry, not x-death[0]", () => {
    // The exact shape observed on a live broker after one retry round-trip.
    const headers: DeathHeaders = {
      "x-death": [
        { queue: QUEUES.RETRY, reason: "expired", count: 7 },
        { queue: QUEUES.WORK, reason: "rejected", count: 2 },
      ],
    };

    expect(countRejections(headers, QUEUES.WORK)).toBe(2);
  });

  it("ignores rejections recorded against another queue", () => {
    const headers: DeathHeaders = {
      "x-death": [{ queue: "q.other", reason: "rejected", count: 2 }],
    };

    expect(countRejections(headers, QUEUES.WORK)).toBe(0);
  });

  it("ignores this queue's entries for reasons other than rejection", () => {
    const headers: DeathHeaders = {
      "x-death": [{ queue: QUEUES.WORK, reason: "maxlen", count: 4 }],
    };

    expect(countRejections(headers, QUEUES.WORK)).toBe(0);
  });
});

describe("decideRetry", () => {
  it.each([
    { rejections: 0, expected: { action: "retry", attempt: 1 } },
    { rejections: 1, expected: { action: "retry", attempt: 2 } },
    { rejections: 2, expected: { action: "park", attempt: 3, reason: "max-attempts" } },
  ])("with MAX_ATTEMPTS=3 and $rejections prior rejections → $expected.action", (row) => {
    const decision = decideRetry({
      error: transient,
      headers: row.rejections === 0 ? undefined : rejectedTimes(row.rejections),
      queue: QUEUES.WORK,
      maxAttempts: 3,
    });

    expect(decision).toEqual(row.expected);
  });

  it.each([
    new UnsupportedMediaError("nope"),
    new CorruptMediaError("bad moov atom"),
    new ObjectNotFoundError("gone"),
    new InvalidJobMessageError("not json"),
  ])("parks $name on the first attempt", (error) => {
    expect(decideRetry({ error, headers: undefined, queue: QUEUES.WORK, maxAttempts: 3 })).toEqual({
      action: "park",
      attempt: 1,
      reason: "non-retryable",
    });
  });

  it("reports non-retryable over max-attempts when both apply", () => {
    expect(
      decideRetry({
        error: new CorruptMediaError("bad"),
        headers: rejectedTimes(2),
        queue: QUEUES.WORK,
        maxAttempts: 3,
      }),
    ).toMatchObject({ action: "park", reason: "non-retryable" });
  });

  it("parks the first failure when MAX_ATTEMPTS is 1", () => {
    expect(
      decideRetry({ error: transient, headers: undefined, queue: QUEUES.WORK, maxAttempts: 1 }),
    ).toEqual({ action: "park", attempt: 1, reason: "max-attempts" });
  });

  it("counts a crashed worker's lost delivery as an attempt", () => {
    expect(
      decideRetry({
        error: transient,
        headers: { "x-delivery-count": 1 },
        queue: QUEUES.WORK,
        maxAttempts: 3,
      }),
    ).toEqual({ action: "retry", attempt: 2 });
  });

  it("adds crashes to rejections -- the header shape observed on a live quorum queue", () => {
    const headers: DeathHeaders = {
      "x-death": [
        { queue: QUEUES.RETRY, reason: "expired", count: 1 },
        { queue: QUEUES.WORK, reason: "rejected", count: 1 },
      ],
      "x-delivery-count": 1,
    };

    expect(decideRetry({ error: transient, headers, queue: QUEUES.WORK, maxAttempts: 3 })).toEqual({
      action: "park",
      attempt: 3,
      reason: "max-attempts",
    });
  });

  it("treats a thrown non-Error as transient", () => {
    expect(
      decideRetry({
        error: "socket hang up",
        headers: undefined,
        queue: QUEUES.WORK,
        maxAttempts: 3,
      }),
    ).toEqual({ action: "retry", attempt: 1 });
  });
});
