import http from "node:http";
import type { Registry } from "prom-client";
import { registry as defaultRegistry } from "./metrics.js";

export interface MetricsServerDeps {
  port: number;
  registry?: Registry;
}

/**
 * Minimal /metrics endpoint for processes that have no HTTP server of their own
 * -- i.e. the workers. The API serves its metrics through Fastify instead.
 *
 * Returns the server without awaiting `listen`, so the caller can shut it down;
 * `port: 0` binds an ephemeral port, which is how the tests avoid collisions.
 */
export function startMetricsServer({
  port,
  registry = defaultRegistry,
}: MetricsServerDeps): http.Server {
  const server = http.createServer((req, res) => {
    if (req.method !== "GET" || req.url !== "/metrics") {
      res.writeHead(404, { "content-type": "text/plain" });
      res.end("not found");
      return;
    }

    registry
      .metrics()
      .then((body) => {
        res.writeHead(200, { "content-type": registry.contentType });
        res.end(body);
      })
      .catch(() => {
        // Never throw out of a request handler: a scrape failure must not take
        // the worker down mid-transcode.
        res.writeHead(500, { "content-type": "text/plain" });
        res.end("failed to collect metrics");
      });
  });

  server.listen(port);
  return server;
}
