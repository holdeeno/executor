import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
// oxlint-disable-next-line executor/no-vitest-import -- boundary: fake timers and spies come from vitest itself
import { vi } from "vitest";
import { Effect, Option, Result, Schema } from "effect";

import { makeMcpRequestTimer } from "./post-timing";

const decodeLogLine = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const logged = (spy: { readonly mock: { readonly calls: unknown[][] } }, event: string) =>
  spy.mock.calls
    .flatMap(([line]) => Option.toArray(decodeLogLine(line)))
    .filter((parsed) => parsed.event === event);

const postRequest = () =>
  new Request("https://executor.example.com/mcp", {
    method: "POST",
    headers: { "mcp-session-id": "session-1" },
  });

describe("makeMcpRequestTimer", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("logs each stage's elapsed time in one summary line", async () => {
    const info = vi.spyOn(console, "info");
    const timer = makeMcpRequestTimer(postRequest());

    const running = timer.run("access_verify", Effect.sleep("40 millis").pipe(Effect.as("ok")));
    await vi.advanceTimersByTimeAsync(40);
    const result = await running;
    timer.finish();

    expect(Result.isSuccess(result)).toBe(true);
    expect(logged(info, "mcp_post_timing")).toEqual([
      expect.objectContaining({
        method: "POST",
        path: "/mcp",
        sessionId: "session-1",
        stages: { access_verify: 40 },
        totalMs: 40,
      }),
    ]);
  });

  it("names a slow stage while it is still running", async () => {
    const warn = vi.spyOn(console, "warn");
    const timer = makeMcpRequestTimer(postRequest());

    void timer.run("handler_init", Effect.never);
    await vi.advanceTimersByTimeAsync(2000);

    expect(logged(warn, "mcp_post_stage_slow")).toEqual([
      expect.objectContaining({
        stage: "handler_init",
        elapsedMs: 2000,
        thresholdMs: 2000,
        path: "/mcp",
        sessionId: "session-1",
      }),
    ]);
  });

  it("does not warn about a stage that finishes in time", async () => {
    const warn = vi.spyOn(console, "warn");
    const timer = makeMcpRequestTimer(postRequest());

    await timer.run("session_lookup", Effect.succeed("ok"));
    await vi.advanceTimersByTimeAsync(5000);

    expect(logged(warn, "mcp_post_stage_slow")).toEqual([]);
  });

  it("fails a hung stage with a timeout that names it", async () => {
    const error = vi.spyOn(console, "error");
    const timer = makeMcpRequestTimer(postRequest());

    const running = timer.run("session_lookup", Effect.never, 30_000);
    await vi.advanceTimersByTimeAsync(30_000);
    const result = await running;

    expect(Result.isFailure(result)).toBe(true);
    expect(Result.isFailure(result) ? result.failure.stage : null).toBe("session_lookup");
    expect(logged(error, "mcp_post_stage_timeout")).toEqual([
      expect.objectContaining({ stage: "session_lookup", timeoutMs: 30_000 }),
    ]);
  });

  it("stays quiet on anything but POST, but still enforces the timeout", async () => {
    const warn = vi.spyOn(console, "warn");
    const info = vi.spyOn(console, "info");
    const timer = makeMcpRequestTimer(
      new Request("https://executor.example.com/mcp", { method: "DELETE" }),
    );

    const running = timer.run("handler_init", Effect.never, 20_000);
    await vi.advanceTimersByTimeAsync(20_000);
    const result = await running;
    timer.finish();

    expect(Result.isFailure(result)).toBe(true);
    expect(logged(warn, "mcp_post_stage_slow")).toEqual([]);
    expect(logged(info, "mcp_post_timing")).toEqual([]);
  });

  it("logs the first byte of the response and passes the body through", async () => {
    const info = vi.spyOn(console, "info");
    const timer = makeMcpRequestTimer(postRequest());
    const encoder = new TextEncoder();

    const response = timer.watchFirstByte(
      new Response(
        new ReadableStream({
          pull: async (controller) => {
            await new Promise((resolve) => setTimeout(resolve, 25));
            controller.enqueue(encoder.encode("event: message\n\n"));
            controller.close();
          },
        }),
        { status: 200, headers: { "mcp-session-id": "session-1" } },
      ),
    );

    const body = response.text();
    await vi.advanceTimersByTimeAsync(25);

    expect(await body).toBe("event: message\n\n");
    expect(response.headers.get("mcp-session-id")).toBe("session-1");
    expect(logged(info, "mcp_post_first_byte")).toEqual([
      expect.objectContaining({ elapsedMs: 25, sessionId: "session-1" }),
    ]);
  });

  it("warns when no first byte arrives, and cancelling reaches the source", async () => {
    const warn = vi.spyOn(console, "warn");
    const timer = makeMcpRequestTimer(postRequest());
    let cancelled = false;

    const response = timer.watchFirstByte(
      new Response(
        new ReadableStream({
          cancel: () => {
            cancelled = true;
          },
        }),
      ),
    );
    await vi.advanceTimersByTimeAsync(2000);

    expect(logged(warn, "mcp_post_stage_slow")).toEqual([
      expect.objectContaining({ stage: "do_first_byte" }),
    ]);
    await response.body?.cancel();
    await vi.advanceTimersByTimeAsync(1);
    expect(cancelled).toBe(true);
  });
});
