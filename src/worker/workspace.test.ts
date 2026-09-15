import { existsSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { createWorkspace } from "./workspace.js";

describe("createWorkspace", () => {
  it("creates a real directory that cleanup removes with its contents", async () => {
    const workspace = await createWorkspace("ws-test-");
    await writeFile(path.join(workspace.path, "seg_000.ts"), "bytes");
    expect(existsSync(workspace.path)).toBe(true);

    await workspace.cleanup();

    expect(existsSync(workspace.path)).toBe(false);
  });

  it("gives each job its own directory", async () => {
    const first = await createWorkspace("ws-test-");
    const second = await createWorkspace("ws-test-");

    expect(first.path).not.toBe(second.path);
    await Promise.all([first.cleanup(), second.cleanup()]);
  });

  it("cleanup is safe to call twice, as a finally after an early cleanup would", async () => {
    const workspace = await createWorkspace("ws-test-");

    await workspace.cleanup();

    await expect(workspace.cleanup()).resolves.toBeUndefined();
  });
});
