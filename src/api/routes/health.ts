import type { FastifyInstance, FastifyPluginAsync } from "fastify";
import type { Registry } from "prom-client";
import { registry as defaultRegistry } from "../../lib/metrics.js";

/**
 * Each check throws (or rejects) when its dependency is unreachable. Injected as
 * plain thunks so the entrypoint decides what "reachable" means for a broker, a
 * bucket and Redis, and tests can fail one on cue.
 */
export type HealthChecks = Record<string, () => Promise<unknown>>;

export interface HealthRoutesDeps {
  checks: HealthChecks;
  registry?: Registry;
}

export function createHealthRoutes({
  checks,
  registry = defaultRegistry,
}: HealthRoutesDeps): FastifyPluginAsync {
  return async function healthRoutes(app: FastifyInstance) {
    app.get("/health", async (request, reply) => {
      const names = Object.keys(checks);
      // Run them concurrently: a health probe should cost one timeout, not N.
      const settled = await Promise.allSettled(names.map((name) => checks[name]()));

      const results: Record<string, "ok" | "unavailable"> = {};
      settled.forEach((result, index) => {
        results[names[index]] = result.status === "fulfilled" ? "ok" : "unavailable";
        if (result.status === "rejected") {
          request.log.warn({ err: result.reason, check: names[index] }, "Health check failed");
        }
      });

      const healthy = settled.every((result) => result.status === "fulfilled");
      return reply
        .status(healthy ? 200 : 503)
        .send({ status: healthy ? "ok" : "degraded", checks: results });
    });

    // The API scrapes through Fastify; only the workers need lib/metrics-server.
    app.get("/metrics", async (_request, reply) => {
      return reply.header("content-type", registry.contentType).send(await registry.metrics());
    });
  };
}
