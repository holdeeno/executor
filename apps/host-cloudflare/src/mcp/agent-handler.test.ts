import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
// oxlint-disable-next-line executor/no-vitest-import -- boundary: vi.mock must come from vitest itself for mock hoisting to resolve
import { vi } from "vitest";
import { Effect, Layer } from "effect";
import { McpAuthProvider, authenticated } from "@executor-js/host-mcp";

const seams = vi.hoisted(() => ({
  validate: (): Promise<string> => Promise.resolve("ok"),
  serveFetch: (): Promise<Response> => Promise.resolve(new Response("data: {}\n\n")),
}));

vi.mock("./session-durable-object", () => ({
  McpSessionDO: { serve: () => ({ fetch: () => seams.serveFetch() }) },
}));
vi.mock("@executor-js/cloudflare/mcp/session-stub", () => ({
  mcpSessionStub: () => ({ validateMcpSessionOwner: () => seams.validate() }),
}));
vi.mock("./auth", () => ({
  cloudflareAccessMcpAuth: () =>
    Layer.succeed(McpAuthProvider)({
      discoveryRoutes: [],
      resourceMetadataUrl: () => "https://executor.example.com/meta",
      authenticate: () =>
        Effect.succeed(
          authenticated({
            accountId: "acct",
            organizationId: "org",
            organizationName: "Org",
            organizationSlug: "org",
            email: "a@example.com",
            name: null,
            avatarUrl: null,
            roles: ["member"],
            orgRoleModel: "organization",
            orgRole: "member",
          }),
        ),
    }),
}));

import type { CloudflareConfig, CloudflareEnv } from "../config";
import { makeCloudflareMcpAgentHandler } from "./agent-handler";

const env = {} as CloudflareEnv;
const ctx = {} as ExecutionContext;
const post = (sessionId?: string) =>
  new Request("https://executor.example.com/mcp", {
    method: "POST",
    headers: sessionId ? { "mcp-session-id": sessionId } : {},
  });

describe("makeCloudflareMcpAgentHandler stages", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    seams.validate = () => Promise.resolve("ok");
    seams.serveFetch = () => Promise.resolve(new Response("data: {}\n\n"));
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("times every stage up to the session hand-off", async () => {
    const info = vi.spyOn(console, "info");
    const handler = makeCloudflareMcpAgentHandler({} as CloudflareConfig);

    const response = await handler(post("session-1"), env, ctx);
    await response.text();

    const summary = info.mock.calls.flat().join("\n");
    expect(summary).toContain("mcp_post_timing");
    for (const stage of ["access_verify", "session_lookup", "do_fetch"]) {
      expect(summary).toContain(stage);
    }
  });

  it("answers a hung session lookup with a retryable error naming the stage", async () => {
    seams.validate = () => new Promise<string>(() => undefined);
    const handler = makeCloudflareMcpAgentHandler({} as CloudflareConfig);

    const pending = handler(post("session-1"), env, ctx);
    await vi.advanceTimersByTimeAsync(30_000);
    const response = await pending;

    expect(response.status).toBe(503);
    expect(await response.text()).toContain("session_lookup");
  });

  it("does not time out the session hand-off, where a call may already be running", async () => {
    let finish: (response: Response) => void = () => undefined;
    seams.serveFetch = () =>
      new Promise<Response>((resolve) => {
        finish = resolve;
      });
    const warn = vi.spyOn(console, "warn");
    const handler = makeCloudflareMcpAgentHandler({} as CloudflareConfig);

    const pending = handler(post(), env, ctx);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(warn.mock.calls.flat().join("\n")).toContain("do_fetch");
    finish(new Response("data: {}\n\n"));
    const response = await pending;

    expect(response.status).toBe(200);
  });
});
