import { defineConfig } from "vitest/config";

// src/config validates at import time, so these must exist before any module
// under test loads. NODE_ENV=test also silences pino (see src/lib/logger.ts).
// Every key has a default in the schema, so this only needs to pin the values
// that tests actually assert against.
const baseEnv = {
  NODE_ENV: "test",
  AMQP_URL: "amqp://guest:guest@localhost:5672",
  S3_ENDPOINT: "http://localhost:9000",
  REDIS_URL: "redis://localhost:6379",
};

// globalSetup runs in Vitest's main process, where the projects' `env` blocks do
// not apply. This config file is evaluated first, so seed the vars here too.
// Integration tests build their own clients from the injected container URLs, so
// these placeholder values are never actually connected to.
for (const [key, value] of Object.entries(baseEnv)) {
  process.env[key] ??= value;
}

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "unit",
          environment: "node",
          include: ["src/**/*.test.ts"],
          env: baseEnv,
        },
      },
      {
        test: {
          name: "integration",
          environment: "node",
          include: ["tests/integration/**/*.test.ts"],
          // Containers are slow to boot and the suites share one RabbitMQ/MinIO/
          // Redis set, so run files serially with generous timeouts.
          testTimeout: 120_000,
          hookTimeout: 180_000,
          fileParallelism: false,
          env: baseEnv,
        },
      },
    ],
  },
});
