// Cloud: the real Cursor flow, in a loop. Execute pauses in session A, the
// client DELETEs A, and a NEW session B resumes. The first resume must come
// back well inside the client's own 60s request timeout, on every round.

import { expect } from "@effect/vitest";
import { Effect } from "effect";
import { composePluginApi } from "@executor-js/api/server";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

import { scenario } from "../src/scenario";
import { Api, Mcp, Target } from "../src/services";
import type { Identity } from "../src/target";

const coreApi = composePluginApi([] as const);
const emailOf = (identity: Identity): string => identity.credentials?.email ?? identity.label;

const APPROVAL_TARGET_TOOL = "executor.coreTools.policies.list";
const GATED_CODE = `
const result = await tools.executor.coreTools.policies.list({});
return JSON.stringify(result);
`;
// Stays under the per-organization execution limit, which stops pauses at 20.
const ROUNDS = 6;
const RESUME_TIMEOUT_MS = 20_000;

const connect = async (mcpUrl: string, bearer: string) => {
  const client = new Client({ name: "executor-e2e-hang", version: "0.0.1" }, { capabilities: {} });
  const transport = new StreamableHTTPClientTransport(new URL(mcpUrl), {
    requestInit: { headers: { authorization: `Bearer ${bearer}` } },
  });
  await client.connect(transport);
  return { client, transport };
};

const textOf = (result: { content?: unknown; toolResult?: unknown }): string =>
  ((result.content ?? []) as Array<{ type: string; text?: string }>)
    .filter((part) => part.type === "text")
    .map((part) => part.text)
    .join("\n");

type Round = {
  readonly round: number;
  readonly action: string;
  readonly delayMs: number;
  readonly ms: number;
  readonly outcome: string;
};

scenario(
  "MCP sessions · a resume after the client DELETEs the owner never hangs",
  { timeout: 15 * 60 * 1000 },
  Effect.gen(function* () {
    const target = yield* Target;
    const { client } = yield* Api;
    const mcp = yield* Mcp;
    const identity = yield* target.newIdentity();
    const api = yield* client(coreApi, identity);
    const bearer = yield* mcp.mintBearer(emailOf(identity));
    const policy = yield* api.policies.create({
      payload: { owner: "org", pattern: APPROVAL_TARGET_TOOL, action: "require_approval" },
    });

    yield* Effect.gen(function* () {
      const rounds = yield* Effect.promise(async () => {
        const seen: Round[] = [];
        for (let round = 0; round < ROUNDS; round += 1) {
          const action = round % 2 === 0 ? "decline" : "accept";
          // Immediately, or after the owner's closed-session alarm has run.
          const delayMs = [0, 3_000][Math.floor(round / 2) % 2] ?? 0;
          const owner = await connect(target.mcpUrl, bearer);
          const paused = await owner.client.callTool({
            name: "execute",
            arguments: { code: GATED_CODE },
          });
          const executionId = /\bexecutionId:\s*(\S+)/.exec(textOf(paused))?.[1];
          expect(executionId, `round ${round} paused`).toEqual(expect.any(String));
          await owner.transport.terminateSession();
          await owner.client.close();
          if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));

          const caller = await connect(target.mcpUrl, bearer);
          const startedAt = Date.now();
          const outcome = await caller.client
            .callTool(
              { name: "resume", arguments: { executionId, action, content: "{}" } },
              undefined,
              {
                timeout: RESUME_TIMEOUT_MS,
              },
            )
            .then(
              (result) => textOf(result).slice(0, 80),
              (error: unknown) => `HUNG: ${String(error)}`,
            );
          seen.push({ round, action, delayMs, ms: Date.now() - startedAt, outcome });
          await caller.transport.terminateSession().catch(() => undefined);
          await caller.client.close();
        }
        return seen;
      });
      expect(rounds.filter((round) => round.outcome.startsWith("HUNG"))).toEqual([]);
      for (const round of rounds) {
        expect(round.outcome, `round ${round.round} ${round.action}`).toContain(
          round.action === "accept" ? '"ok":true' : "requires approval",
        );
      }
    }).pipe(
      Effect.ensuring(
        api.policies
          .remove({ params: { policyId: policy.id }, payload: { owner: "org" } })
          .pipe(Effect.ignore),
      ),
    );
  }),
);
