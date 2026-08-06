import { z } from "zod";

const configSchema = z.object({
  // RabbitMQ
  AMQP_URL: z.url().default("amqp://guest:guest@localhost:5672"),
  AMQP_PREFETCH: z.coerce.number().int().positive().default(1),
  RETRY_TTL_MS: z.coerce.number().int().positive().default(10_000),
  MAX_ATTEMPTS: z.coerce.number().int().positive().default(3),

  // MinIO (S3-compatible)
  S3_ENDPOINT: z.url().default("http://localhost:9000"),
  S3_ACCESS_KEY: z.string().min(1).default("minioadmin"),
  S3_SECRET_KEY: z.string().min(1).default("minioadmin"),
  S3_REGION: z.string().min(1).default("us-east-1"),
  BUCKET_UPLOADS: z.string().min(1).default("media-uploads"),
  BUCKET_OUTPUTS: z.string().min(1).default("media-outputs"),

  // Redis
  REDIS_URL: z.url().default("redis://localhost:6379"),
  JOB_TTL_SECONDS: z.coerce.number().int().positive().default(86_400),

  // Transcoding
  MAX_UPLOAD_BYTES: z.coerce.number().int().positive().default(524_288_000),
  HLS_SEGMENT_SECONDS: z.coerce.number().int().positive().default(4),

  // Observability
  OTEL_EXPORTER_OTLP_ENDPOINT: z.url().default("http://localhost:4318"),
  OTEL_SERVICE_NAME: z.string().min(1).default("media-pipeline"),

  // Networking & ports
  API_PORT: z.coerce.number().int().positive().default(3000),
  WORKER_METRICS_PORT: z.coerce.number().int().positive().default(9101),

  // App env
  NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
});

export type Config = z.infer<typeof configSchema>;

/**
 * Parses and validates environment variables into typed config.
 *
 * Throws rather than calling process.exit() so this stays a pure function: the
 * entrypoints already catch, log and exit on startup failure, so fail-fast
 * behaviour is unchanged -- but tests can assert on validation without tearing
 * down the process.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const result = configSchema.safeParse(env);

  if (!result.success) {
    throw new Error(
      `Invalid environment configuration:\n${JSON.stringify(z.treeifyError(result.error), null, 2)}`,
    );
  }

  return result.data;
}

/**
 * Process-wide config singleton, resolved once at import.
 *
 * Deliberately a singleton: config is ambient process state read once from the
 * environment at boot. Only I/O clients get injected -- see src/lib/.
 */
export const config = loadConfig();
