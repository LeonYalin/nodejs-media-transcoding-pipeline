import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadBucketCommand,
  NoSuchKey,
  NotFound,
  type S3Client,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { Readable, Transform } from "node:stream";
import { ObjectNotFoundError } from "../domain/media.js";

export interface ObjectRef {
  bucket: string;
  key: string;
}

export interface PutStreamInput extends ObjectRef {
  body: Readable;
  contentType?: string;
}

export interface PutStreamResult {
  /** Bytes actually written, counted in flight -- never by buffering the body. */
  bytes: number;
}

/**
 * Counts bytes as they flow past without holding on to any of them. This is how
 * the API learns an upload's size while keeping the "never buffer a media file"
 * invariant -- the alternative (reading Content-Length) is absent on chunked
 * multipart bodies and unverified even when present.
 */
function createByteCounter(onDone: (bytes: number) => void): Transform {
  let bytes = 0;
  return new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      bytes += chunk.length;
      callback(null, chunk);
    },
    flush(callback) {
      onDone(bytes);
      callback();
    },
  });
}

/**
 * The only module in the codebase that talks to S3.
 *
 * Callers depend on the returned object rather than on the AWS SDK, so they can
 * be tested against a plain in-memory fake of the same shape -- no module
 * mocking required.
 */
export function createObjectRepository(client: S3Client) {
  async function putStream({
    bucket,
    key,
    body,
    contentType,
  }: PutStreamInput): Promise<PutStreamResult> {
    let bytes = 0;
    const counter = createByteCounter((total) => {
      bytes = total;
    });

    // `.pipe` forwards neither errors nor an early close, and either one would
    // leave `Upload` waiting on a source that will never end: an aborted
    // request, or a caller abandoning this upload after its sibling failed.
    body.on("error", (error) => counter.destroy(error));
    body.on("close", () => {
      if (!body.readableEnded) counter.destroy(new Error("Upload body closed before it ended"));
    });
    body.pipe(counter);

    // lib-storage streams the body as a multipart upload, so memory stays at
    // one part regardless of file size.
    const upload = new Upload({
      client,
      params: { Bucket: bucket, Key: key, Body: counter, ContentType: contentType },
    });

    await upload.done();
    return { bytes };
  }

  async function getStream({ bucket, key }: ObjectRef): Promise<Readable> {
    try {
      const response = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
      if (!response.Body) {
        throw new ObjectNotFoundError(`Empty body for ${bucket}/${key}`);
      }
      // On Node the SDK always yields a Readable; the union only widens for
      // browser/blob runtimes this project never runs in.
      return response.Body as Readable;
    } catch (error) {
      if (error instanceof NoSuchKey || error instanceof NotFound) {
        // Non-retryable: a missing source will still be missing next attempt,
        // so this parks the job rather than looping it through the delay queue.
        throw new ObjectNotFoundError(`No such object ${bucket}/${key}`, { cause: error });
      }
      throw error;
    }
  }

  async function deleteObject({ bucket, key }: ObjectRef): Promise<void> {
    await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
  }

  async function bucketExists(bucket: string): Promise<boolean> {
    try {
      await client.send(new HeadBucketCommand({ Bucket: bucket }));
      return true;
    } catch (error) {
      if (error instanceof NotFound) return false;
      throw error;
    }
  }

  return { putStream, getStream, deleteObject, bucketExists };
}

export type ObjectRepository = ReturnType<typeof createObjectRepository>;
