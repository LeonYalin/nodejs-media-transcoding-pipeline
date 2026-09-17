import { GetObjectCommand, HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import sharp from "sharp";
import { createS3Client, jobIdOf, objectsIn, type S3Event } from "./s3-event.js";

// Image headers sit at the start of the file, so this much is enough to read
// dimensions without downloading the whole source.
const HEADER_BYTES = 64 * 1024;
const OUTPUTS_BUCKET = "media-outputs";

// Created once per container, outside the handler, so warm invocations reuse it.
const s3 = createS3Client();

/**
 * Triggered by ObjectCreated on media-uploads. Writes `{jobId}/source.json`
 * describing the original upload.
 */
export async function handler(event: S3Event): Promise<void> {
  for (const { bucket, key } of objectsIn(event)) {
    const head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));

    // Video would need ffprobe, which this runtime does not have.
    let dimensions = {};
    if (head.ContentType?.startsWith("image/")) {
      const range = await s3.send(
        new GetObjectCommand({ Bucket: bucket, Key: key, Range: `bytes=0-${HEADER_BYTES - 1}` }),
      );
      const header = await range.Body?.transformToByteArray();
      if (header) {
        const { width, height, format } = await sharp(header).metadata();
        dimensions = { width, height, format };
      }
    }

    const metadata = {
      key,
      bytes: head.ContentLength,
      contentType: head.ContentType,
      etag: head.ETag,
      uploadedAt: head.LastModified?.toISOString(),
      ...dimensions,
    };

    const outputKey = `${jobIdOf(key)}/source.json`;
    await s3.send(
      new PutObjectCommand({
        Bucket: OUTPUTS_BUCKET,
        Key: outputKey,
        Body: JSON.stringify(metadata, null, 2),
        ContentType: "application/json",
      }),
    );
    // One JSON line per object, as CloudWatch Logs would collect it.
    console.log(JSON.stringify({ fn: "metadata", source: `${bucket}/${key}`, wrote: outputKey }));
  }
}
