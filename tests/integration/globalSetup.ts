import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GenericContainer,
  Network,
  Wait,
  type StartedNetwork,
  type StartedTestContainer,
} from "testcontainers";
import type { TestProject } from "vitest/node";

// The same pinned images docker-compose.yml runs, so the suite tests the stack
// that ships rather than a lookalike.
const RABBITMQ_IMAGE = "rabbitmq:4.3-management-alpine";
const MINIO_IMAGE = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
const REDIS_IMAGE = "redis:7-alpine";
export const WORKER_IMAGE = "media-worker-integration";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

declare module "vitest" {
  interface ProvidedContext {
    amqpUrl: string;
    s3Endpoint: string;
    redisUrl: string;
    networkName: string;
    fixturesDir: string;
  }
}

let network: StartedNetwork;
let containers: StartedTestContainer[] = [];
let fixturesDir: string;

/** Runs the worker image's own ffmpeg, so the host still never needs it. */
async function generateVideo(fileName: string, size: string, seconds: number): Promise<void> {
  await new GenericContainer(WORKER_IMAGE)
    .withBindMounts([{ source: fixturesDir, target: "/fixtures" }])
    .withEntrypoint(["ffmpeg"])
    .withCommand([
      "-v",
      "error",
      "-y",
      "-f",
      "lavfi",
      "-i",
      `testsrc=size=${size}:rate=30`,
      "-f",
      "lavfi",
      "-i",
      "sine",
      "-t",
      String(seconds),
      "-c:v",
      "libx264",
      "-c:a",
      "aac",
      "-shortest",
      `/fixtures/${fileName}`,
    ])
    .withWaitStrategy(Wait.forOneShotStartup())
    .start();
}

export default async function setup({ provide }: TestProject) {
  network = await new Network().start();

  // The worker image builds while the infra boots. Docker's layer cache makes
  // every run after the first one quick.
  const [rabbitmq, minio, redis] = await Promise.all([
    new GenericContainer(RABBITMQ_IMAGE)
      .withNetwork(network)
      .withNetworkAliases("rabbitmq")
      // Not `guest`: that account only logs in over loopback, and the workers
      // connect from other containers.
      .withEnvironment({ RABBITMQ_DEFAULT_USER: "media", RABBITMQ_DEFAULT_PASS: "media" })
      .withExposedPorts(5672)
      .withWaitStrategy(Wait.forLogMessage(/Server startup complete/))
      .withStartupTimeout(120_000)
      .start(),
    new GenericContainer(MINIO_IMAGE)
      .withNetwork(network)
      .withNetworkAliases("minio")
      .withCommand(["server", "/data"])
      .withExposedPorts(9000)
      .withWaitStrategy(Wait.forHttp("/minio/health/live", 9000))
      .start(),
    new GenericContainer(REDIS_IMAGE)
      .withNetwork(network)
      .withNetworkAliases("redis")
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start(),
    GenericContainer.fromDockerfile(repoRoot, "Dockerfile.worker").build(WORKER_IMAGE, {
      deleteOnExit: false,
    }),
  ]);
  containers = [rabbitmq, minio, redis];

  // The same bucket commands as compose's minio-init.
  const buckets = await minio.exec([
    "sh",
    "-c",
    "mc alias set local http://localhost:9000 minioadmin minioadmin && " +
      "mc mb local/media-uploads && mc mb local/media-outputs",
  ]);
  if (buckets.exitCode !== 0) throw new Error(`Bucket setup failed: ${buckets.output}`);

  // Under the OS temp dir, which Docker Desktop shares with containers.
  fixturesDir = await mkdtemp(path.join(os.tmpdir(), "media-fixtures-"));
  // Two rungs (720p + 360p), and a clip long enough to kill a worker mid-encode.
  await generateVideo("ladder.mp4", "1280x720", 4);
  await generateVideo("long.mp4", "640x360", 120);

  provide("amqpUrl", `amqp://media:media@${rabbitmq.getHost()}:${rabbitmq.getMappedPort(5672)}`);
  provide("s3Endpoint", `http://${minio.getHost()}:${minio.getMappedPort(9000)}`);
  provide("redisUrl", `redis://${redis.getHost()}:${redis.getMappedPort(6379)}`);
  provide("networkName", network.getName());
  provide("fixturesDir", fixturesDir);

  return async () => {
    await Promise.allSettled(containers.map((container) => container.stop()));
    await network.stop();
    await rm(fixturesDir, { recursive: true, force: true });
  };
}
