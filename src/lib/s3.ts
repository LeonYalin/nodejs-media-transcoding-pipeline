import { S3Client } from "@aws-sdk/client-s3";

export interface S3Settings {
  endpoint: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
}

/**
 * S3 client pointed at MinIO.
 *
 * `forcePathStyle` is mandatory: the SDK defaults to virtual-hosted addressing
 * (`http://bucket.host/key`), which needs wildcard DNS that a local MinIO does
 * not have. Path style keeps the bucket in the path (`http://host/bucket/key`).
 *
 * Only entrypoints call this; every S3 operation goes through `object-repository`.
 */
export function createS3Client({
  endpoint,
  region,
  accessKeyId,
  secretAccessKey,
}: S3Settings): S3Client {
  return new S3Client({
    endpoint,
    region,
    forcePathStyle: true,
    credentials: { accessKeyId, secretAccessKey },
  });
}
