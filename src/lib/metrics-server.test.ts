import type { AddressInfo } from "node:net";
import { Registry, Counter } from "prom-client";
import { afterEach, describe, expect, it } from "vitest";
import { startMetricsServer } from "./metrics-server.js";

let server: ReturnType<typeof startMetricsServer> | undefined;

/** Boots the server on an ephemeral port and returns its base URL. */
async function listen(registry: Registry): Promise<string> {
  server = startMetricsServer({ port: 0, registry });
  await new Promise((resolve) => server!.once("listening", resolve));
  const { port } = server.address() as AddressInfo;
  return `http://127.0.0.1:${port}`;
}

afterEach(async () => {
  if (server) {
    await new Promise((resolve) => server!.close(resolve));
    server = undefined;
  }
});

describe("startMetricsServer", () => {
  it("serves the registry in Prometheus text format", async () => {
    const registry = new Registry();
    new Counter({ name: "test_total", help: "test", registers: [registry] }).inc(3);

    const res = await fetch(`${await listen(registry)}/metrics`);

    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/plain");
    expect(await res.text()).toContain("test_total 3");
  });

  it("404s anything that is not GET /metrics", async () => {
    const base = await listen(new Registry());

    expect((await fetch(`${base}/`)).status).toBe(404);
    expect((await fetch(`${base}/metrics`, { method: "POST" })).status).toBe(404);
  });

  it("answers 500 instead of crashing when collection fails", async () => {
    const registry = new Registry();
    // A scrape failure must never take down a worker mid-transcode.
    registry.metrics = () => Promise.reject(new Error("collection exploded"));

    const res = await fetch(`${await listen(registry)}/metrics`);

    expect(res.status).toBe(500);
  });
});
