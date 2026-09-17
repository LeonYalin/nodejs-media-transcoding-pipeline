import { GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import type { Readable } from "node:stream";
import sharp from "sharp";
import { createS3Client, jobIdOf, objectsIn, type S3Event } from "./s3-event.js";

const PLACEHOLDER_WIDTH = 20;

// Created once per container, outside the handler, so warm invocations reuse it.
const s3 = createS3Client();

/**
 * Triggered by ObjectCreated on media-outputs for `image/full.webp`. Writes a
 * tiny blurred `image/placeholder.webp` the UI shows while the image loads.
 * The trigger's suffix filter keeps this write from triggering itself.
 */
export async function handler(event: S3Event): Promise<void> {
  for (const { bucket, key } of objectsIn(event)) {
    const source = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    // On Node the SDK body is a Readable, streamed straight into sharp.
    const placeholder = await (source.Body as Readable)
      .pipe(sharp().resize(PLACEHOLDER_WIDTH).blur(2).webp({ quality: 40 }))
      .toBuffer();

    const outputKey = `${jobIdOf(key)}/image/placeholder.webp`;
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: outputKey,
        Body: placeholder,
        ContentType: "image/webp",
      }),
    );
    console.log(
      JSON.stringify({
        fn: "placeholder",
        source: `${bucket}/${key}`,
        wrote: outputKey,
        bytes: placeholder.length,
      }),
    );
  }
}
