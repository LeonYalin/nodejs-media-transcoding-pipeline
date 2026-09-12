import type { FastifyInstance, FastifyPluginAsync } from "fastify";
import type { JobsRepository } from "../../lib/jobs-repository.js";
import {
  JobParamsSchema,
  ListJobsQuerySchema,
  type GetJobRequest,
  type GetJobReply,
  type JobEventsRequest,
  type JobEventsReply,
  type ListJobsRequest,
  type ListJobsReply,
} from "../contracts.js";
import { createSseHandler, type SseDeps } from "../sse.js";

export interface JobRoutesDeps {
  jobsRepository: Pick<JobsRepository, "getJob" | "listRecentJobs">;
  createSubscriber: SseDeps["createSubscriber"];
  sseRegistry?: SseDeps["registry"];
  heartbeatMs?: number;
}

export function createJobRoutes({
  jobsRepository,
  createSubscriber,
  sseRegistry,
  heartbeatMs,
}: JobRoutesDeps): FastifyPluginAsync {
  return async function jobRoutes(app: FastifyInstance) {
    // Built once at registration, not per request: the returned handler creates
    // all its per-stream state (subscriber, heartbeat, registry entry) itself.
    const sseHandler = createSseHandler({
      createSubscriber,
      jobsRepository,
      registry: sseRegistry,
      heartbeatMs,
    });

    // Powers the UI table.
    app.get<ListJobsRequest & { Reply: ListJobsReply }>("/jobs", async (request, reply) => {
      const query = ListJobsQuerySchema.safeParse(request.query);
      if (!query.success) {
        return reply.status(400).send({ error: "Invalid limit" });
      }
      return reply.send({ jobs: await jobsRepository.listRecentJobs(query.data.limit) });
    });

    app.get<GetJobRequest & { Reply: GetJobReply }>("/jobs/:id", async (request, reply) => {
      const params = JobParamsSchema.safeParse(request.params);
      if (!params.success) {
        return reply.status(400).send({ error: "Job id must be a UUID" });
      }

      const record = await jobsRepository.getJob(params.data.id);
      // Records expire after JOB_TTL_SECONDS, so "not found" also means "too old".
      if (!record) return reply.status(404).send({ error: "Job not found" });

      return reply.send(record);
    });

    app.get<JobEventsRequest & { Reply: JobEventsReply }>(
      "/jobs/:id/events",
      async (request, reply) => {
        // Validated here rather than inside the handler so an arbitrary id can
        // never reach `SUBSCRIBE` -- the same check `GET /jobs/:id` applies.
        const params = JobParamsSchema.safeParse(request.params);
        if (!params.success) {
          return reply.status(400).send({ error: "Job id must be a UUID" });
        }
        return sseHandler(request, reply);
      },
    );
  };
}
