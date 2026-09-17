import { describe, expect, it } from "vitest";
import { jobIdOf, objectsIn } from "./s3-event.js";

function record(bucket: string, key: string) {
  return { s3: { bucket: { name: bucket }, object: { key } } };
}

describe("objectsIn", () => {
  it("returns every record's bucket and key", () => {
    const event = {
      Records: [
        record("media-uploads", "a/source.jpg"),
        record("media-outputs", "b/image/full.webp"),
      ],
    };

    expect(objectsIn(event)).toEqual([
      { bucket: "media-uploads", key: "a/source.jpg" },
      { bucket: "media-outputs", key: "b/image/full.webp" },
    ]);
  });

  it("decodes URL-encoded keys, with + as a space", () => {
    const event = { Records: [record("media-uploads", "my+folder/caf%C3%A9%2Bbar.jpg")] };

    expect(objectsIn(event)[0].key).toBe("my folder/café+bar.jpg");
  });

  it("returns nothing for an event without records", () => {
    expect(objectsIn({})).toEqual([]);
  });
});

describe("jobIdOf", () => {
  it("takes the first path segment", () => {
    expect(jobIdOf("0b7c/image/full.webp")).toBe("0b7c");
    expect(jobIdOf("0b7c/source.mp4")).toBe("0b7c");
  });
});
