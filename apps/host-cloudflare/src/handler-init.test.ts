import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
// oxlint-disable-next-line executor/no-vitest-import -- boundary: fake timers come from vitest itself
import { vi } from "vitest";
import { Effect, Exit } from "effect";

import { makeHandlerResolver } from "./handler-init";

describe("makeHandlerResolver", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromiseExit(effect);

  it("builds once and shares the handler", async () => {
    let builds = 0;
    const resolve = makeHandlerResolver(async () => {
      builds += 1;
      return "handler";
    });

    expect(await Effect.runPromise(resolve())).toBe("handler");
    expect(await Effect.runPromise(resolve())).toBe("handler");
    expect(builds).toBe(1);
  });

  it("gives up on a hung build, and the next request rebuilds", async () => {
    let builds = 0;
    const resolve = makeHandlerResolver(() => {
      builds += 1;
      return builds === 1 ? new Promise<string>(() => undefined) : Promise.resolve("handler");
    }, 20_000);

    const hung = run(resolve());
    await vi.advanceTimersByTimeAsync(20_000);
    expect(Exit.isFailure(await hung)).toBe(true);

    expect(await Effect.runPromise(resolve())).toBe("handler");
    expect(builds).toBe(2);
  });

  it("fails a request that joined a hung build, without waiting on it forever", async () => {
    const resolve = makeHandlerResolver(() => new Promise<string>(() => undefined), 20_000);

    const first = run(resolve());
    await vi.advanceTimersByTimeAsync(10_000);
    const joined = run(resolve());
    await vi.advanceTimersByTimeAsync(20_000);

    expect(Exit.isFailure(await first)).toBe(true);
    expect(Exit.isFailure(await joined)).toBe(true);
  });

  it("does not cache a failed build", async () => {
    let builds = 0;
    const resolve = makeHandlerResolver(() => {
      builds += 1;
      return builds === 1
        ? Effect.runPromise(Effect.fail("d1 unavailable"))
        : Promise.resolve("handler");
    });

    expect(Exit.isFailure(await run(resolve()))).toBe(true);
    expect(await Effect.runPromise(resolve())).toBe("handler");
    expect(builds).toBe(2);
  });
});
