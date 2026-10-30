import { Data, Effect } from "effect";

export class HandlerInitFailed extends Data.TaggedError("HandlerInitFailed")<{
  readonly cause: unknown;
}> {}

/**
 * Build the isolate's request handler once and share it, without letting a bad
 * build take the isolate down with it.
 *
 * Every request in the isolate waits on the same build, which starts in whichever
 * request got there first and does D1 work there. If that build hangs, nothing
 * else would ever answer, and a failure would be cached for the isolate's life.
 * So each wait is bounded, and a build that failed or timed out is dropped: the
 * next request starts a new one. Schema bring-up is idempotent, so an overlap
 * with a slow build that eventually finishes is safe.
 */
export const makeHandlerResolver = <H>(build: () => Promise<H>, timeoutMs = 20_000) => {
  let current: Promise<H> | null = null;
  return (): Effect.Effect<H, HandlerInitFailed> =>
    Effect.suspend(() => {
      const attempt = current ?? build();
      current = attempt;
      const drop = Effect.sync(() => {
        if (current === attempt) current = null;
      });
      return Effect.tryPromise({
        try: () => attempt,
        catch: (cause) => new HandlerInitFailed({ cause }),
      }).pipe(
        Effect.timeoutOrElse({
          duration: `${timeoutMs} millis`,
          orElse: () => Effect.fail(new HandlerInitFailed({ cause: "timed out" })),
        }),
        Effect.tapError(() => drop),
      );
    });
};
