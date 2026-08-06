import { describe, expect, it } from "vitest";
import { loadConfig } from "./index.js";

describe("loadConfig", () => {
  it("applies defaults for an empty environment", () => {
    const config = loadConfig({});

    expect(config.AMQP_URL).toBe("amqp://guest:guest@localhost:5672");
    expect(config.BUCKET_UPLOADS).toBe("media-uploads");
    expect(config.BUCKET_OUTPUTS).toBe("media-outputs");
    expect(config.NODE_ENV).toBe("development");
  });

  it("coerces numeric env vars, which always arrive as strings", () => {
    const config = loadConfig({ API_PORT: "8080", MAX_ATTEMPTS: "5" });

    expect(config.API_PORT).toBe(8080);
    expect(config.MAX_ATTEMPTS).toBe(5);
  });

  it("keeps the worker metrics port off Prometheus' 9090", () => {
    expect(loadConfig({}).WORKER_METRICS_PORT).toBe(9101);
  });

  it("rejects a malformed URL", () => {
    expect(() => loadConfig({ S3_ENDPOINT: "not-a-url" })).toThrow(
      /Invalid environment configuration/,
    );
  });

  it("rejects a non-positive port", () => {
    expect(() => loadConfig({ API_PORT: "0" })).toThrow(/Invalid environment configuration/);
  });

  it("rejects an unknown NODE_ENV", () => {
    expect(() => loadConfig({ NODE_ENV: "staging" })).toThrow(/Invalid environment configuration/);
  });

  it("throws rather than exiting, so the error is assertable", () => {
    expect(() => loadConfig({ REDIS_URL: "nope" })).toThrow(Error);
  });
});
