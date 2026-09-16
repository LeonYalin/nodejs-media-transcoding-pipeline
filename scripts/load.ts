import { pathToFileURL } from "node:url";
import sharp from "sharp";
import { config } from "../src/config/index.js";

// Tuning knobs for this dev script, not app config.
const CONNECTIONS = 20;
const DURATION_SECONDS = 30;

interface Sample {
  status: number;
  ms: number;
}

/** The value at `fraction` of an ascending list, e.g. 0.99 for p99. */
function percentile(sorted: number[], fraction: number): number {
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))];
}

/**
 * Fires concurrent image uploads at the API for a fixed time, then reports how
 * fast `202` came back -- the proof that ingest stays fast while workers churn.
 */
export async function main(): Promise<void> {
  const url = `http://localhost:${config.API_PORT}/uploads`;
  // Generated rather than committed. This is the load client, not the
  // pipeline, so holding one small fixture in memory is fine.
  const jpeg = await sharp({
    create: { width: 1280, height: 720, channels: 3, background: "#3a6ea5" },
  })
    .jpeg()
    .toBuffer();

  const samples: Sample[] = [];
  const deadline = Date.now() + DURATION_SECONDS * 1000;

  async function uploadUntilDeadline(): Promise<void> {
    while (Date.now() < deadline) {
      const form = new FormData();
      form.append("file", new Blob([jpeg], { type: "image/jpeg" }), "load.jpg");

      const started = performance.now();
      const response = await fetch(url, { method: "POST", body: form }).catch(() => null);
      await response?.arrayBuffer();
      // 0 = no response at all (connection refused or reset).
      samples.push({ status: response?.status ?? 0, ms: performance.now() - started });
    }
  }

  console.log(`Uploading to ${url} with ${CONNECTIONS} connections for ${DURATION_SECONDS}s...`);
  const connections: Promise<void>[] = [];
  for (let i = 0; i < CONNECTIONS; i++) connections.push(uploadUntilDeadline());
  await Promise.all(connections);

  const statuses: Record<number, number> = {};
  for (const sample of samples) statuses[sample.status] = (statuses[sample.status] ?? 0) + 1;
  const latencies = samples.map((sample) => sample.ms).sort((a, b) => a - b);
  const accepted = statuses[202] ?? 0;

  console.log(
    `Requests:    ${samples.length} (${(samples.length / DURATION_SECONDS).toFixed(1)}/s)`,
  );
  console.log(`202 rate:    ${((accepted / samples.length) * 100).toFixed(1)}%`);
  console.log(`Statuses:    ${JSON.stringify(statuses)}`);
  console.log(
    `Latency ms:  p50 ${percentile(latencies, 0.5).toFixed(0)}` +
      ` · p99 ${percentile(latencies, 0.99).toFixed(0)}` +
      ` · max ${latencies[latencies.length - 1].toFixed(0)}`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exit(1);
  });
}
