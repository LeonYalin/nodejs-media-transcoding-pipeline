import { z, type ZodIssue } from "zod";
import type { JobRecord } from "../domain/job.js";

/**
 * The HTTP wire contract, in one place.
 *
 * `src/domain/job.ts` does this for the AMQP wire; this module does it for the
 * browser-facing one. Every route pairs a `*Request` type (the Fastify
 * `Params`/`Querystring`/`Body` generic) with a `*Reply` type (the response
 * body), so `reply.send()` is type-checked instead of accepting `unknown`.
 *
 * Schemas live here beside the types derived from them, so the shape Fastify is
 * told about cannot drift from the shape actually validated.
 */

// -- Shared ------------------------------------------------------------------

export const JobParamsSchema = z.object({ id: z.uuid() });
export type JobParams = z.infer<typeof JobParamsSchema>;

/** The single error envelope every route uses. */
export interface ErrorReply {
  error: string;
}

/** Only the app-level ZodError branch carries issue details. */
export interface ValidationErrorReply extends ErrorReply {
  details: ZodIssue[];
}

// -- POST /uploads -----------------------------------------------------------

/**
 * No `CreateUploadRequest`: the body is a multipart stream, typed structurally
 * by `UploadPart` in `upload-service.ts`. A Fastify `Body` generic would claim
 * a parsed JSON shape that never exists on this route.
 */
export type CreateUploadReply =
  { jobId: string; statusUrl: string; eventsUrl: string } | ErrorReply;

// -- GET /jobs ---------------------------------------------------------------

export const ListJobsQuerySchema = z.object({
  limit: z.coerce.number().int().positive().max(100).default(20),
});

/** The *parsed* query, as the handler consumes it after `safeParse`. */
export type ListJobsQuery = z.infer<typeof ListJobsQuerySchema>;

/**
 * Hand-written rather than derived: `z.coerce.number()` widens `z.input` to
 * `unknown`, which would tell Fastify nothing. Query values arrive as strings.
 */
export interface ListJobsRequest {
  Querystring: { limit?: string };
}

export type ListJobsReply = { jobs: JobRecord[] } | ErrorReply;

// -- GET /jobs/:id -----------------------------------------------------------

export interface GetJobRequest {
  Params: JobParams;
}

export type GetJobReply = JobRecord | ErrorReply;

// -- GET /jobs/:id/events ----------------------------------------------------

export interface JobEventsRequest {
  Params: JobParams;
}

/** The stream itself is a hijacked socket; only 400/404 go through `send()`. */
export type JobEventsReply = ErrorReply;

// -- GET /health, GET /metrics -----------------------------------------------

export type HealthCheckStatus = "ok" | "unavailable";

export interface HealthReply {
  status: "ok" | "degraded";
  checks: Record<string, HealthCheckStatus>;
}

/** Prometheus text exposition format. */
export type MetricsReply = string;
