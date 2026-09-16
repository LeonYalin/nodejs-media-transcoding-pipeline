import { pathToFileURL } from "node:url";
import { config } from "../src/config/index.js";
import { connectAmqp } from "../src/lib/amqp.js";
import { logger } from "../src/lib/logger.js";
import { createObjectRepository } from "../src/lib/object-repository.js";
import { createRedisClient } from "../src/lib/redis.js";
import { createS3Client } from "../src/lib/s3.js";

/**
 * The one command to run after `npm run up`: asserts the AMQP topology and
 * proves every dependency the API needs is reachable. Safe to re-run.
 */
export async function main(): Promise<void> {
  // Checked, not created: minio-init owns the buckets, because it also makes
  // media-outputs public-read. A bucket created here would 403 in the UI.
  const objectRepository = createObjectRepository(
    createS3Client({
      endpoint: config.S3_ENDPOINT,
      region: config.S3_REGION,
      accessKeyId: config.S3_ACCESS_KEY,
      secretAccessKey: config.S3_SECRET_KEY,
    }),
  );
  if (!(await objectRepository.bucketExists(config.BUCKET_UPLOADS))) {
    throw new Error(`Bucket ${config.BUCKET_UPLOADS} is missing; run npm run up`);
  }
  if (!(await objectRepository.bucketExists(config.BUCKET_OUTPUTS))) {
    throw new Error(`Bucket ${config.BUCKET_OUTPUTS} is missing; run npm run up`);
  }
  logger.info("MinIO ready: buckets exist");

  // connectAmqp asserts the full topology (lib/topology.ts) as it connects.
  const amqpConnection = await connectAmqp({ url: config.AMQP_URL });
  await amqpConnection.close();
  logger.info("RabbitMQ ready: topology asserted");

  const redis = createRedisClient({ url: config.REDIS_URL });
  await redis.ping();
  await redis.quit();
  logger.info("Redis ready");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error: unknown) => {
    logger.error({ err: error }, "Infra init failed");
    process.exit(1);
  });
}
