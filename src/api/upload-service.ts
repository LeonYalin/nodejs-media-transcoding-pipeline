import { randomUUID } from "node:crypto";
import type { Readable } from "node:stream";
import type { FastifyBaseLogger } from "fastify";
import type { JobMessage } from "../domain/job.js";
import { EXTENSION_BY_MIME, MimeTypeSchema, isImageMime } from "../domain/media.js";
import type { JobPublisher } from "../lib/amqp.js";
import type { JobsRepository } from "../lib/jobs-repository.js";
import { mediaMetrics } from "../lib/metrics.js";
import type { ObjectRepository } from "../lib/object-repository.js";
import { ROUTING_KEYS } from "../lib/topology.js";

/**
 * The slice of a @fastify/multipart file part this path needs, typed
 * structurally so a test can drive the service with a plain Readable.
 */
export interface UploadPart {
  file: Readable & { truncated: boolean };
  mimetype: string;
  filename: string;
}

const MAX_NAME_LENGTH = 255;

/** Doubles as the `reason` label on the `media_uploads_rejected_total` counter. */
export type UploadRejectionReason =
  | "no_file"
  | "unsupported_mime"
  | "storage_error"
  | "too_large"
  | "job_record_error"
  | "publish_failed";

export type UploadOutcome =
  | { accepted: true; jobId: string }
  | { accepted: false; reason: UploadRejectionReason; message: string };

export interface UploadServiceDeps {
  // Narrowed to the methods this saga actually calls, matching sse.ts and
  // routes/jobs.ts. It keeps a fake honest about what the upload path touches.
  objectRepository: Pick<ObjectRepository, "putStream" | "deleteObject">;
  jobsRepository: Pick<JobsRepository, "createJob" | "markFailed">;
  jobPublisher: JobPublisher;
  bucket: string;
}

/**
 * Owns the accept-an-upload saga: stream to MinIO, record the job, then
 * confirm-publish it.
 *
 * Each of those three writes can fail *after* the previous one succeeded, and
 * each failure has its own compensating action -- which is why the try/catch
 * blocks live here, next to the cleanup they exist for, rather than in the
 * route. The route's only job is turning the outcome into a status code.
 *
 * Returns an outcome instead of throwing: a 415 for a `.exe` is the API working
 * correctly, not an exception. `MediaDomainError.retryable` deliberately stays
 * out of this -- it answers "will a redelivery succeed?" for `worker/retry.ts`,
 * which is a different question from "what should the client see".
 */
export function createUploadService({
  objectRepository,
  jobsRepository,
  jobPublisher,
  bucket,
}: UploadServiceDeps) {
  function reject(reason: UploadRejectionReason, message: string): UploadOutcome {
    mediaMetrics.uploadRejected.inc({ reason });
    return { accepted: false, reason, message };
  }

  /**
   * `reason` is carried into the log line so a failed 413 cleanup stays
   * distinguishable from a failed post-Redis orphan cleanup -- they have very
   * different implications for what is left in the bucket.
   */
  async function removeQuietly(
    sourceKey: string,
    jobId: string,
    log: FastifyBaseLogger,
    reason: string,
  ): Promise<void> {
    await objectRepository.deleteObject({ bucket, key: sourceKey }).catch((error: unknown) => {
      log.warn({ err: error, jobId, sourceKey, reason }, "Could not remove upload object");
    });
  }

  async function acceptUpload(
    part: UploadPart | undefined,
    log: FastifyBaseLogger,
  ): Promise<UploadOutcome> {
    if (!part) return reject("no_file", "Expected a multipart file field");

    // The allowlist is a schema, so an unsupported type is a parse failure
    // rather than a hand-rolled if (see domain/media.ts).
    const mimeResult = MimeTypeSchema.safeParse(part.mimetype);
    if (!mimeResult.success) {
      // The multipart body must be drained even when rejected, or the
      // connection stalls waiting for a consumer that never arrives.
      part.file.resume();
      return reject("unsupported_mime", `Unsupported media type: ${part.mimetype}`);
    }

    const mime = mimeResult.data;
    const jobId = randomUUID();
    const sourceKey = `${jobId}/source${EXTENSION_BY_MIME[mime]}`;
    const type: JobMessage["type"] = isImageMime(mime) ? "image" : "video";

    let bytes: number;
    try {
      // Request -> MinIO with nothing accumulating in between.
      ({ bytes } = await objectRepository.putStream({
        bucket,
        key: sourceKey,
        body: part.file,
        contentType: mime,
      }));
    } catch (error) {
      log.error({ err: error, jobId }, "Upload to object store failed");
      // Drain whatever is left, as the rejection path above does, so the
      // connection is not left waiting on a consumer that has gone away.
      part.file.resume();
      return reject("storage_error", "Could not store upload");
    }

    // @fastify/multipart is registered with throwFileSizeLimit: false, so an
    // oversized body ends the stream quietly and flags it here. The partial
    // object is already in the bucket and has to go.
    if (part.file.truncated) {
      await removeQuietly(sourceKey, jobId, log, "truncated_upload");
      return reject("too_large", "Upload exceeds the maximum allowed size");
    }

    const now = new Date().toISOString();
    try {
      await jobsRepository.createJob({
        jobId,
        status: "queued",
        type,
        sourceKey,
        mime,
        bytes,
        name: part.filename.slice(0, MAX_NAME_LENGTH) || undefined,
        createdAt: now,
        updatedAt: now,
        progress: 0,
      });
    } catch (error) {
      // The bytes are in the bucket but nothing references them, and no message
      // will ever be published for this key -- so remove them rather than
      // leaving orphans only a lifecycle rule could collect.
      log.error({ err: error, jobId }, "Could not write job record");
      await removeQuietly(sourceKey, jobId, log, "orphaned_source");
      return reject("job_record_error", "Could not record job");
    }

    const routingKey = type === "image" ? ROUTING_KEYS.IMAGE_TRANSFORM : ROUTING_KEYS.VIDEO_PLAN;

    const confirmTimer = mediaMetrics.publishConfirmDuration.startTimer();
    try {
      // Resolves only once the broker has confirmed the message -- the client
      // must never get a 202 for a job RabbitMQ did not accept.
      await jobPublisher.publish(routingKey, { jobId, type, sourceKey, mime });
    } catch (error) {
      log.error({ err: error, jobId }, "Publisher confirm failed");
      // The bytes are stored and the record exists, so the job is real and must
      // not be left claiming `queued` forever.
      await jobsRepository.markFailed(jobId, "Broker did not confirm the job");
      return reject("publish_failed", "Could not queue job");
    } finally {
      confirmTimer();
    }

    mediaMetrics.uploadsReceived.inc({ type });
    mediaMetrics.uploadBytes.observe(bytes);

    return { accepted: true, jobId };
  }

  return { acceptUpload };
}

export type UploadService = ReturnType<typeof createUploadService>;
