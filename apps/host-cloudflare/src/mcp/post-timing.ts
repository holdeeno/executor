import { Data, Effect, Result } from "effect";

import { jsonRpcErrorBody } from "@executor-js/host-mcp";

/** A stage slower than this logs a warning while it is still running. */
const SLOW_STAGE_MS = 2_000;

export class McpRequestStageTimeout extends Data.TaggedError("McpRequestStageTimeout")<{
  readonly stage: string;
  readonly timeoutMs: number;
}> {}

/** A retryable answer for a request that stalled before reaching its session. */
export const stageStalledResponse = (stage: string): Response =>
  jsonRpcErrorBody(503, -32001, `The request stalled at ${stage}. Retry the call.`);

/**
 * Times the stages of one `/mcp` request before its session hand-off.
 *
 * A stage that never finishes can never log its own duration, so the slow
 * warning fires from a timer while the stage is still stuck. That is what names
 * the stage when a request stalls until the client gives up. Only POST logs
 * timings; a stage's timeout applies to every method.
 */
export const makeMcpRequestTimer = (request: Request) => {
  const logs = request.method === "POST";
  const path = new URL(request.url).pathname;
  const sessionId = request.headers.get("mcp-session-id");
  const startedAt = Date.now();
  const stages: Record<string, number> = {};
  let finished = false;

  const watchdog = (stage: string, stageStartedAt: number) =>
    logs
      ? setTimeout(() => {
          console.warn(
            JSON.stringify({
              event: "mcp_post_stage_slow",
              stage,
              elapsedMs: Date.now() - stageStartedAt,
              thresholdMs: SLOW_STAGE_MS,
              path,
              sessionId,
            }),
          );
        }, SLOW_STAGE_MS)
      : undefined;

  const run = <A, E>(
    stage: string,
    effect: Effect.Effect<A, E>,
    timeoutMs?: number,
  ): Promise<Result.Result<A, E | McpRequestStageTimeout>> => {
    const stageStartedAt = Date.now();
    const slow = watchdog(stage, stageStartedAt);
    const bounded: Effect.Effect<A, E | McpRequestStageTimeout> =
      timeoutMs === undefined
        ? effect
        : effect.pipe(
            Effect.timeoutOrElse({
              duration: `${timeoutMs} millis`,
              orElse: () =>
                Effect.sync(() => {
                  console.error(
                    JSON.stringify({
                      event: "mcp_post_stage_timeout",
                      stage,
                      timeoutMs,
                      method: request.method,
                      path,
                      sessionId,
                    }),
                  );
                }).pipe(
                  Effect.andThen(Effect.fail(new McpRequestStageTimeout({ stage, timeoutMs }))),
                ),
            }),
          );
    return Effect.runPromise(
      bounded.pipe(
        Effect.result,
        Effect.ensuring(
          Effect.sync(() => {
            clearTimeout(slow);
            stages[stage] = Date.now() - stageStartedAt;
          }),
        ),
      ),
    );
  };

  /** Log what the stages took. Safe to call more than once. */
  const finish = (): void => {
    if (!logs || finished) return;
    finished = true;
    console.info(
      JSON.stringify({
        event: "mcp_post_timing",
        method: request.method,
        path,
        sessionId,
        stages,
        totalMs: Date.now() - startedAt,
      }),
    );
  };

  /** Pass the response through, logging when its first byte goes out. */
  const watchFirstByte = (response: Response): Response => {
    if (!logs || response.body === null) return response;
    const slow = watchdog("do_first_byte", Date.now());
    let seen = false;
    const body = response.body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform: (chunk, controller) => {
          if (!seen) {
            seen = true;
            clearTimeout(slow);
            console.info(
              JSON.stringify({
                event: "mcp_post_first_byte",
                elapsedMs: Date.now() - startedAt,
                path,
                sessionId,
              }),
            );
          }
          controller.enqueue(chunk);
        },
      }),
    );
    return new Response(body, response);
  };

  return { run, finish, watchFirstByte };
};

export type McpRequestTimer = ReturnType<typeof makeMcpRequestTimer>;
