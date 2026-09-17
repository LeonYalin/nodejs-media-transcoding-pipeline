import { S3Client } from "@aws-sdk/client-s3";

/**
 * The slice of an S3 event notification the handlers read. AWS and MinIO send
 * the same shape, so no `@types/aws-lambda` dependency is needed.
 */
export interface S3Event {
  Records?: {
    s3: {
      bucket: { name: string };
      object: { key: string };
    };
  }[];
}

export interface S3ObjectRef {
  bucket: string;
  key: string;
}

/**
 * The objects an event is about. Keys arrive URL-encoded, with spaces as `+`,
 * exactly as S3 sends them to Lambda.
 */
export function objectsIn(event: S3Event): S3ObjectRef[] {
  return (event.Records ?? []).map((record) => ({
    bucket: record.s3.bucket.name,
    key: decodeURIComponent(record.s3.object.key.replace(/\+/g, " ")),
  }));
}

/** Every pipeline key starts with the job id: `{jobId}/source.jpg`, `{jobId}/image/full.webp`. */
export function jobIdOf(key: string): string {
  return key.split("/")[0];
}

/**
 * On AWS the SDK finds S3 by region and credentials from the Lambda role, so
 * `S3_ENDPOINT` is unset there. Locally it points at MinIO, which needs
 * path-style addressing.
 */
export function createS3Client(): S3Client {
  const endpoint = process.env.S3_ENDPOINT;
  return new S3Client(endpoint ? { endpoint, forcePathStyle: true } : {});
}
