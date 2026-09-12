import type { FastifyInstance, FastifyPluginAsync } from "fastify";
import type { CreateUploadReply } from "../contracts.js";
import type { UploadRejectionReason, UploadService } from "../upload-service.js";

/**
 * The one place HTTP status codes are attached to upload outcomes. Keyed by the
 * rejection union, so a new reason will not compile until it has a status.
 */
const STATUS_BY_REASON: Record<UploadRejectionReason, number> = {
  no_file: 400,
  unsupported_mime: 415,
  too_large: 413,
  storage_error: 503,
  job_record_error: 503,
  publish_failed: 503,
};

export function createUploadRoutes(uploadService: UploadService): FastifyPluginAsync {
  return async function uploadRoutes(app: FastifyInstance) {
    app.post<{ Reply: CreateUploadReply }>("/uploads", async (request, reply) => {
      const outcome = await uploadService.acceptUpload(await request.file(), request.log);

      if (!outcome.accepted) {
        return reply.status(STATUS_BY_REASON[outcome.reason]).send({ error: outcome.message });
      }

      return reply.status(202).send({
        jobId: outcome.jobId,
        statusUrl: `/jobs/${outcome.jobId}`,
        eventsUrl: `/jobs/${outcome.jobId}/events`,
      });
    });
  };
}
