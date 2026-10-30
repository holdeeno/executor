import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
// oxlint-disable-next-line executor/no-vitest-import -- boundary: vi.mock must come from vitest itself for mock hoisting to resolve
import { vi } from "vitest";

const buildApp = vi.hoisted(() => ({ current: () => new Promise<never>(() => undefined) }));

vi.mock("./app", () => ({ makeCloudflareApp: () => buildApp.current() }));
vi.mock("./mcp", () => ({ McpExecutionOwnerDirectoryDO: class {}, McpSessionDO: class {} }));

import worker from "./worker";
import type { CloudflareEnv } from "./config";

const env = {
  ACCESS_TEAM_DOMAIN: "team.cloudflareaccess.com",
  ACCESS_AUD: "aud",
} as Partial<CloudflareEnv> as CloudflareEnv;
const ctx = {} as ExecutionContext;

describe("worker fetch while start-up hangs", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("answers POST /mcp with a retryable error naming the stage", async () => {
    const warn = vi.spyOn(console, "warn");
    const pending = worker.fetch(
      new Request("https://executor.example.com/mcp", { method: "POST" }),
      env,
      ctx,
    );

    await vi.advanceTimersByTimeAsync(20_000);
    const response = await pending;

    expect(response.status).toBe(503);
    expect(await response.text()).toContain("handler_init");
    expect(warn.mock.calls.flat().join("\n")).toContain("mcp_post_stage_slow");
  });

  it("answers other routes with a plain 503", async () => {
    const pending = worker.fetch(new Request("https://executor.example.com/api/x"), env, ctx);

    await vi.advanceTimersByTimeAsync(20_000);
    const response = await pending;

    expect(response.status).toBe(503);
    expect(response.headers.get("content-type")).toContain("text/plain");
  });
});
