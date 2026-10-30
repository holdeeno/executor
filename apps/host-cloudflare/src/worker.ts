import { Effect, Result } from "effect";

import { makeCloudflareApp } from "./app";
import {
  cloudflareAccessConfigErrorMessage,
  missingCloudflareAccessVars,
  type CloudflareEnv,
} from "./config";
import { makeHandlerResolver } from "./handler-init";
import { makeMcpRequestTimer, stageStalledResponse, type McpRequestTimer } from "./mcp/post-timing";
import { mcpResourceFromPath } from "./mcp/resource";

// The MCP Durable Object classes, bound in wrangler.jsonc. They must be exported
// at the Worker entry module scope for the runtime to find them.
export { McpExecutionOwnerDirectoryDO, McpSessionDO } from "./mcp";

// ---------------------------------------------------------------------------
// The Worker fetch entry. Most requests go to `ExecutorApp.make`'s Effect web
// handler. `/mcp` and `/mcp/toolkits/:slug` stay at this edge boundary because
// `McpAgent.serve()` needs the Cloudflare `ExecutionContext` to pass
// authenticated session props into the hibernatable Durable Object bridge.
// ---------------------------------------------------------------------------

type WorkerHandler = {
  readonly app: (request: Request) => Promise<Response>;
  readonly mcp: (
    request: Request,
    env: CloudflareEnv,
    ctx: ExecutionContext,
    timer: McpRequestTimer,
  ) => Promise<Response>;
};

let resolver: ReturnType<typeof makeHandlerResolver<WorkerHandler>> | null = null;

const resolveHandler = (env: CloudflareEnv) => {
  resolver ??= makeHandlerResolver<WorkerHandler>(() =>
    makeCloudflareApp(env).then(({ toWebHandler, mcpAgentHandler }) => ({
      app: toWebHandler().handler,
      mcp: mcpAgentHandler,
    })),
  );
  return resolver();
};

const startupStalledResponse = (): Response =>
  new Response("The service is still starting. Retry the request.\n", {
    status: 503,
    headers: {
      "cache-control": "no-store",
      "content-type": "text/plain; charset=utf-8",
    },
  });

const accessConfigErrorResponse = (missingVars: readonly string[]): Response =>
  new Response(`${cloudflareAccessConfigErrorMessage(missingVars)}\n`, {
    status: 503,
    headers: {
      "cache-control": "no-store",
      "content-type": "text/plain; charset=utf-8",
    },
  });

export default {
  fetch: async (request: Request, env: CloudflareEnv, ctx: ExecutionContext): Promise<Response> => {
    const missingAccessVars = missingCloudflareAccessVars(env);
    if (missingAccessVars.length > 0) {
      return accessConfigErrorResponse(missingAccessVars);
    }

    const resource = mcpResourceFromPath(new URL(request.url).pathname);
    const timer = makeMcpRequestTimer(request);
    const resolved = await timer.run(
      "handler_init",
      resolveHandler(env).pipe(
        Effect.tapError(() =>
          Effect.sync(() => {
            console.error(JSON.stringify({ event: "worker_handler_init_failed" }));
          }),
        ),
      ),
    );
    if (Result.isFailure(resolved)) {
      if (resource === null) return startupStalledResponse();
      timer.finish();
      return stageStalledResponse("handler_init");
    }
    if (resource !== null) {
      return resolved.success.mcp(request, env, ctx, timer);
    }
    return resolved.success.app(request);
  },
};
