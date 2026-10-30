import { Effect, Predicate, Result } from "effect";

import {
  McpAuthProvider,
  jsonRpcErrorBody,
  orgWriteAccessForPrincipal,
  withOrgWriteAccess,
  type AuthOutcome,
  type McpResource,
  type Principal,
} from "@executor-js/host-mcp";
import {
  currentPropagationHeaders,
  readArtifactsEnabled,
  readElicitationMode,
  readSearchToolsEnabled,
  readToolMode,
  withVerifiedIdentityHeaders,
} from "@executor-js/cloudflare/mcp/do-headers";
import type { McpSessionProps } from "@executor-js/cloudflare/mcp/agent-durable-object";
import { sessionOrgRoleMetadata } from "@executor-js/cloudflare/mcp/role-metadata";
import { mcpSessionStub } from "@executor-js/cloudflare/mcp/session-stub";

import type { CloudflareConfig, CloudflareEnv } from "../config";
import { cloudflareAccessMcpAuth } from "./auth";
import { makeMcpRequestTimer, stageStalledResponse, type McpRequestTimer } from "./post-timing";
import { mcpResourceFromPath } from "./resource";
import { McpSessionDO } from "./session-durable-object";

// Bounds on the waits before the session hand-off, all well inside the 60s after
// which MCP clients give up. Nothing has run yet at these stages, so a client can
// safely retry. Access verify covers jose's own 5s limit on the certs fetch.
const ACCESS_VERIFY_TIMEOUT_MS = 10_000;
const SESSION_LOOKUP_TIMEOUT_MS = 30_000;

const corsPreflightResponse = (): Response =>
  new Response(null, {
    status: 204,
    headers: {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, POST, DELETE, OPTIONS",
      "access-control-allow-headers":
        "content-type, authorization, mcp-session-id, accept, mcp-protocol-version",
      "access-control-expose-headers": "mcp-session-id, WWW-Authenticate",
    },
  });

const jsonRpcResponse = (
  status: number,
  code: number,
  message: string,
  challenge?: string,
): Response =>
  challenge === undefined
    ? jsonRpcErrorBody(status, code, message)
    : jsonRpcErrorBody(status, code, message, { challenge });

const renderAuthError = (
  auth: McpAuthProvider["Service"],
  request: Request,
  outcome: Exclude<AuthOutcome, { readonly _tag: "Authenticated" }>,
): Response => {
  if (Predicate.isTagged(outcome, "Unauthorized")) {
    return jsonRpcResponse(
      401,
      -32001,
      "Unauthorized",
      outcome.challenge ?? `Bearer resource_metadata="${auth.resourceMetadataUrl(request)}"`,
    );
  }
  if (Predicate.isTagged(outcome, "Forbidden")) {
    return jsonRpcResponse(403, outcome.code ?? -32001, outcome.message);
  }
  return jsonRpcResponse(503, -32001, outcome.message);
};

const authenticate = (request: Request, config: CloudflareConfig) =>
  Effect.gen(function* () {
    const auth = yield* McpAuthProvider;
    const outcome = yield* auth.authenticate(request);
    return { auth, outcome };
  }).pipe(Effect.provide(cloudflareAccessMcpAuth(config)));

const propsForPrincipal = (
  request: Request,
  principal: Principal,
  resource: McpResource,
): Effect.Effect<McpSessionProps> =>
  Effect.gen(function* () {
    const propagation = yield* currentPropagationHeaders(request);
    return {
      session: {
        organizationId: principal.organizationId,
        ...sessionOrgRoleMetadata(principal),
        userId: principal.accountId,
        elicitationMode: readElicitationMode(request),
        artifactsEnabled: readArtifactsEnabled(request),
        searchToolsEnabled: readSearchToolsEnabled(request),
        toolMode: readToolMode(request),
        resource,
        webOrigin: new URL(request.url).origin,
      },
      propagation,
    };
  });

export const makeCloudflareMcpAgentHandler = (config: CloudflareConfig) => {
  const serveOptions = {
    binding: "MCP_SESSION",
    transport: "streamable-http",
  } as const;
  const serveDefault = McpSessionDO.serve("/mcp", serveOptions);
  const serveToolkit = McpSessionDO.serve("/mcp/toolkits/:slug", serveOptions);

  return async (
    request: Request,
    env: CloudflareEnv,
    ctx: ExecutionContext,
    timer: McpRequestTimer = makeMcpRequestTimer(request),
  ): Promise<Response> => {
    if (request.method === "OPTIONS") return corsPreflightResponse();
    const sessionId = request.headers.get("mcp-session-id");
    const finished = (response: Response): Response => {
      timer.finish();
      return response;
    };

    const verified = await timer.run(
      "access_verify",
      authenticate(request, config),
      ACCESS_VERIFY_TIMEOUT_MS,
    );
    if (Result.isFailure(verified)) return finished(stageStalledResponse("access_verify"));
    const { auth, outcome } = verified.success;
    if (!Predicate.isTagged(outcome, "Authenticated")) {
      if (Predicate.isTagged(outcome, "Forbidden") && sessionId) {
        await Effect.runPromise(
          Effect.ignore(
            Effect.tryPromise(() =>
              mcpSessionStub(env.MCP_SESSION, sessionId)._cf_scheduleDestroy(),
            ),
          ),
        );
      }
      return finished(renderAuthError(auth, request, outcome));
    }

    const resource = mcpResourceFromPath(new URL(request.url).pathname);
    if (resource === null) {
      return finished(jsonRpcResponse(404, -32001, "MCP route not found"));
    }

    if (!sessionId && request.method === "DELETE") {
      return finished(
        new Response(null, { status: 204, headers: { "access-control-allow-origin": "*" } }),
      );
    }

    if (sessionId) {
      const lookup = await timer.run(
        "session_lookup",
        Effect.promise(() =>
          mcpSessionStub(env.MCP_SESSION, sessionId).validateMcpSessionOwner(
            {
              accountId: outcome.principal.accountId,
              organizationId: outcome.principal.organizationId,
            },
            resource,
          ),
        ),
        SESSION_LOOKUP_TIMEOUT_MS,
      );
      if (Result.isFailure(lookup)) return finished(stageStalledResponse("session_lookup"));
      const owner = lookup.success;
      if (owner === "not_found") {
        return finished(jsonRpcResponse(404, -32001, "Session not found"));
      }
      if (owner === "terminated") {
        // DELETE-condemned but the deferred destroy alarm hasn't wiped storage
        // yet; the terminated id must read as dead immediately.
        return finished(jsonRpcResponse(404, -32001, "Session timed out, please reconnect"));
      }
      if (owner === "forbidden") {
        return finished(
          jsonRpcResponse(403, -32003, "MCP session does not belong to the current bearer"),
        );
      }
    }

    const props = await Effect.runPromise(propsForPrincipal(request, outcome.principal, resource));
    (ctx as ExecutionContext & { props?: McpSessionProps }).props = props;
    const forwarded = withOrgWriteAccess(
      withVerifiedIdentityHeaders(
        request,
        {
          accountId: outcome.principal.accountId,
          organizationId: outcome.principal.organizationId,
        },
        resource,
      ),
      orgWriteAccessForPrincipal(outcome.principal),
    );
    const target = resource.kind === "toolkit" ? serveToolkit : serveDefault;
    // No timeout: a tool call may already be running behind this hand-off, and
    // answering the client early could invite it to run the call again.
    const handedOff = await timer.run(
      "do_fetch",
      Effect.promise(() => target.fetch(forwarded, env, ctx)),
    );
    timer.finish();
    if (Result.isFailure(handedOff)) return stageStalledResponse("do_fetch");
    return timer.watchFirstByte(handedOff.success);
  };
};
