import { z } from "zod";
import { MimeTypeSchema, RenditionSchema } from "./media.js";

export const JobStatusSchema = z.enum(["queued", "processing", "completed", "failed"]);
export type JobStatus = z.infer<typeof JobStatusSchema>;

export const JobTypeSchema = z.enum(["image", "video"]);
export type JobType = z.infer<typeof JobTypeSchema>;

const jobMessageBase = {
  jobId: z.uuid(),
  sourceKey: z.string().min(1),
  mime: MimeTypeSchema,
};

export const ImageJobMessageSchema = z.object({
  type: z.literal("image"),
  ...jobMessageBase,
});

export const VideoJobMessageSchema = z.object({
  type: z.literal("video"),
  ...jobMessageBase,
});

/**
 * What `job.video.rendition` carries: the same video job plus the one rung this
 * message is responsible for encoding.
 *
 * There is no "plan vs rendition" discriminator in the payload -- the worker
 * already knows which stage it is running from *the message's routing key*, so
 * it parses with the schema that routing key expects.
 * That keeps `type` down to the two values that actually describe the media.
 */
export const VideoRenditionJobMessageSchema = VideoJobMessageSchema.extend({
  rendition: RenditionSchema,
});

/** The contract the API publishes as `job.image.transform` / `job.video.plan`. */
export const JobMessageSchema = z.discriminatedUnion("type", [
  ImageJobMessageSchema,
  VideoJobMessageSchema,
]);

export type ImageJobMessage = z.infer<typeof ImageJobMessageSchema>;
export type VideoJobMessage = z.infer<typeof VideoJobMessageSchema>;
export type VideoRenditionJobMessage = z.infer<typeof VideoRenditionJobMessageSchema>;
export type JobMessage = z.infer<typeof JobMessageSchema>;

/** Full state persisted in the Redis `job:{jobId}` hash. */
export const JobRecordSchema = z.object({
  jobId: z.uuid(),
  status: JobStatusSchema,
  type: JobTypeSchema,
  sourceKey: z.string().min(1),
  mime: MimeTypeSchema,
  bytes: z.number().int().nonnegative(),
  createdAt: z.iso.datetime(),
  updatedAt: z.iso.datetime(),
  progress: z.number().min(0).max(100),
  error: z.string().optional(),
  outputs: z.array(z.string()).optional(),
  /** The ladder the plan stage chose; the last rendition builds the master playlist from it. */
  renditions: z.array(RenditionSchema).optional(),
  renditionsExpected: z.number().int().nonnegative().optional(),
  renditionsDone: z.number().int().nonnegative().optional(),
});
export type JobRecord = z.infer<typeof JobRecordSchema>;
