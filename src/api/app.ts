import multipart from "@fastify/multipart";
import fastifyStatic from "@fastify/static";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fastify, type FastifyBaseLogger, type FastifyError, type FastifyInstance } from "fastify";
import type { Registry } from "prom-client";
import { ZodError } from "zod";
import { UnsupportedMediaError } from "../domain/media.js";
import type { JobsRepository } from "../lib/jobs-repository.js";
import { logger as defaultLogger } from "../lib/logger.js";
import type { ErrorReply, ValidationErrorReply } from "./contracts.js";
import { createHealthRoutes, type HealthChecks } from "./routes/health.js";
import { createJobRoutes } from "./routes/jobs.js";
import { createUploadRoutes } from "./routes/uploads.js";
import type { UploadService } from "./upload-service.js";
import type { SseDeps } from "./sse.js";

const PUBLIC_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "../../public");

export interface AppDeps {
  uploadService: UploadService;
  jobsRepository: JobsRepository;
  createSubscriber: SseDeps["createSubscriber"];
  /** Passed in by the entrypoint so shutdown can drain live streams. */
  sseRegistry?: SseDeps["registry"];
  healthChecks: HealthChecks;
  maxUploadBytes: number;
  metricsRegistry?: Registry;
  loggerInstance?: FastifyBaseLogger;
  heartbeatMs?: number;
}

/**
 * Builds the API without listening, so tests can drive it with `app.inject()`
 * and the entrypoint owns the socket.
 */
export async function createApp(deps: AppDeps): Promise<FastifyInstance> {
  // Widened to FastifyBaseLogger so the returned instance stays a plain
  // FastifyInstance rather than one parameterised by pino's concrete Logger.
  const loggerInstance: FastifyBaseLogger = deps.loggerInstance ?? defaultLogger;

  // Deliberately no `bodyLimit`: @fastify/multipart installs its own
  // content-type parser and enforces `limits.fileSize` below. A server-level
  // body limit would reject the request before the upload service could delete
  // the partial object it had already streamed into MinIO and answer 413 itself.
  const app = fastify({
    // The project's single pino instance -- never Fastify's own `logger: true`.
    loggerInstance,
  });

  await app.register(multipart, {
    limits: { fileSize: deps.maxUploadBytes, files: 1 },
    // Let the stream end quietly and set `file.truncated` instead of throwing
    // mid-pipe: the upload service needs to reach its cleanup path and delete
    // the partial object it already streamed into MinIO.
    throwFileSizeLimit: false,
  });

  // public/ is populated in step 12. @fastify/static throws on a missing root,
  // so guard it rather than coupling the API's boot to the UI step.
  if (existsSync(PUBLIC_DIR)) {
    await app.register(fastifyStatic, { root: PUBLIC_DIR, prefix: "/" });
  }

  // No route generic to attach here, so each payload is annotated at the point
  // of construction instead -- same contract, still compile-checked.
  app.setErrorHandler((error: FastifyError, request, reply) => {
    if (error instanceof ZodError) {
      const body: ValidationErrorReply = { error: "Invalid request", details: error.issues };
      return reply.status(400).send(body);
    }
    // Narrow on purpose: `MediaDomainError.retryable` answers "will a redelivery
    // succeed?" for worker/retry.ts, which is not the same question as "what
    // status does the client get" -- ObjectNotFoundError is non-retryable but
    // would deserve a 404 here, not a 415.
    if (error instanceof UnsupportedMediaError) {
      const body: ErrorReply = { error: error.message };
      return reply.status(415).send(body);
    }
    if (typeof error.statusCode === "number" && error.statusCode < 500) {
      const body: ErrorReply = { error: error.message };
      return reply.status(error.statusCode).send(body);
    }

    request.log.error({ err: error }, "Unhandled request error");
    const body: ErrorReply = { error: "Internal server error" };
    return reply.status(500).send(body);
  });

  await app.register(createUploadRoutes(deps.uploadService));

  await app.register(
    createJobRoutes({
      jobsRepository: deps.jobsRepository,
      createSubscriber: deps.createSubscriber,
      sseRegistry: deps.sseRegistry,
      heartbeatMs: deps.heartbeatMs,
    }),
  );

  await app.register(
    createHealthRoutes({ checks: deps.healthChecks, registry: deps.metricsRegistry }),
  );

  return app;
}
