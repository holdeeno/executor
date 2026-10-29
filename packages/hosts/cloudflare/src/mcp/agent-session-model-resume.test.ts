import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
// oxlint-disable-next-line executor/no-vitest-import -- boundary: vi.mock must come from vitest itself for mock hoisting to resolve
import { vi } from "vitest";
import { Cause, Effect, Exit } from "effect";
import {
  CurrentOrgWriteAccess,
  currentOrgWriteAccess,
  makeOrgWriteAccessState,
  type OrgWriteAccess,
} from "@executor-js/sdk";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { defaultMcpResource } from "@executor-js/host-mcp";
import {
  PAUSED_APPROVAL_TIMEOUT_MS,
  formatMcpExecutionOutcome,
} from "@executor-js/host-mcp/tool-server";
import type {
  ExecutionEngine,
  ExecutionResult,
  PausedExecutionDeadline,
  ResumeResponse,
} from "@executor-js/execution";

import {
  McpAgentSessionDOBase,
  type BuiltMcpServer,
  type McpModelResumeCaller,
  type McpSessionInit,
  type McpSessionModelResumeResult,
  type SessionMeta,
} from "./agent-session-durable-object";
import {
  McpExecutionOwnerDirectoryDO,
  mcpExecutionOwnerDirectoryFromNamespace,
  mcpSessionDurableObjectName,
  type McpExecutionOwnerDirectory,
  type McpExecutionOwnerDirectoryNamespace,
  type McpExecutionOwnerRecord,
  type McpExecutionOwnerRoute,
} from "./execution-owner-directory";
import { mcpSessionStub } from "./session-stub";

vi.mock("agents/mcp", () => ({
  McpAgent: class {
    protected readonly ctx: DurableObjectState;

    constructor(ctx: DurableObjectState) {
      this.ctx = ctx;
    }

    getSessionId(): string {
      return this.ctx.id.toString();
    }

    keepAlive(): Promise<() => void> {
      return Promise.resolve(() => undefined);
    }

    getStreamRequestIds(): Promise<readonly (string | number)[]> {
      return Promise.resolve([]);
    }

    getOpenStreamRequestIds(): Promise<readonly unknown[]> {
      return Promise.resolve([]);
    }

    currentSessionEpoch(): Promise<number> {
      return Promise.resolve(0);
    }

    onConnect(): Promise<void> {
      return Promise.resolve();
    }

    // Mirrors the SDK's session DELETE: a durable "condemned" marker plus an
    // alarm, which then runs `destroy()` and wipes storage.
    async _cf_scheduleDestroy(): Promise<void> {
      await this.ctx.storage.put("cf_agents_destroy_pending", true);
      await this.ctx.storage.setAlarm(Date.now() + 1000);
    }

    async alarm(): Promise<void> {
      if ((await this.ctx.storage.get("cf_agents_destroy_pending")) === true) {
        await this.destroy();
      }
    }

    async destroy(): Promise<void> {
      await this.ctx.storage.deleteAll();
    }
  },
}));

class FakeStorage implements DurableObjectStorage {
  private readonly values = new Map<string, unknown>();
  readonly sql = {} as DurableObjectStorage["sql"];
  readonly kv = {} as DurableObjectStorage["kv"];
  alarmAt: number | null = null;

  async get<T>(key: string): Promise<T | undefined>;
  async get<T>(keys: string[]): Promise<Map<string, T>>;
  async get<T>(keyOrKeys: string | string[]): Promise<T | undefined | Map<string, T>> {
    if (Array.isArray(keyOrKeys)) {
      return new Map(keyOrKeys.map((key) => [key, this.values.get(key) as T]));
    }
    return this.values.get(keyOrKeys) as T | undefined;
  }

  async put<T>(key: string, value: T): Promise<void>;
  async put<T>(entries: Record<string, T> | Map<string, T>): Promise<void>;
  async put<T>(
    keyOrEntries: string | Record<string, T> | Map<string, T>,
    value?: T,
  ): Promise<void> {
    if (typeof keyOrEntries === "string") {
      this.values.set(keyOrEntries, value);
      return;
    }
    const entries =
      keyOrEntries instanceof Map ? keyOrEntries.entries() : Object.entries(keyOrEntries);
    for (const [key, entry] of entries) this.values.set(key, entry);
  }

  async delete(key: string): Promise<boolean>;
  async delete(keys: string[]): Promise<number>;
  async delete(keyOrKeys: string | string[]): Promise<boolean | number> {
    if (!Array.isArray(keyOrKeys)) return this.values.delete(keyOrKeys);
    let deleted = 0;
    for (const key of keyOrKeys) {
      if (this.values.delete(key)) deleted += 1;
    }
    return deleted;
  }

  async list<T = unknown>(): Promise<Map<string, T>> {
    return new Map([...this.values.entries()].map(([key, value]) => [key, value as T]));
  }

  async deleteAll(): Promise<void> {
    this.values.clear();
    this.alarmAt = null;
  }

  transaction<T>(_closure: (txn: DurableObjectTransaction) => Promise<T>): Promise<T> {
    return Promise.resolve(undefined as T);
  }

  transactionSync<T>(_closure: () => T): T {
    return undefined as T;
  }

  async sync(): Promise<void> {}

  async getAlarm(): Promise<number | null> {
    return this.alarmAt;
  }

  async getCurrentBookmark(): Promise<string> {
    return "test-bookmark";
  }

  async getBookmarkForTime(_timestamp: number | Date): Promise<string> {
    return "test-bookmark";
  }

  onNextSessionRestoreBookmark(_bookmark: string): Promise<string> {
    return Promise.resolve("test-bookmark");
  }

  async setAlarm(scheduledTime: number | Date): Promise<void> {
    this.alarmAt = scheduledTime instanceof Date ? scheduledTime.getTime() : scheduledTime;
  }

  async deleteAlarm(): Promise<void> {
    this.alarmAt = null;
  }
}

class FakeDurableObjectState implements DurableObjectState {
  readonly storage = new FakeStorage();
  readonly id: DurableObjectId;
  readonly props: unknown = undefined;
  readonly facets = {} as DurableObjectState["facets"];
  readonly ctx = this;
  private waitUntilPromises: Promise<unknown>[] = [];

  constructor(name: string) {
    const id: Pick<DurableObjectId, "equals" | "name" | "toString"> = {
      equals: (other: DurableObjectId) => other.toString() === name,
      name,
      toString: () => name,
    };
    this.id = id as DurableObjectId;
  }

  waitUntil(promise: Promise<unknown>): void {
    this.waitUntilPromises.push(promise);
  }

  async flushWaitUntil(): Promise<void> {
    while (this.waitUntilPromises.length > 0) {
      const pending = this.waitUntilPromises.splice(0);
      await Promise.all(pending);
    }
  }

  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
    return callback();
  }

  acceptWebSocket(_ws: WebSocket, _tags?: string[]): void {}

  getWebSockets(_tag?: string): WebSocket[] {
    return [];
  }

  getTags(_ws: WebSocket): string[] {
    return [];
  }

  setWebSocketAutoResponse(_pair?: WebSocketRequestResponsePair): void {}

  getWebSocketAutoResponse(): WebSocketRequestResponsePair | null {
    return null;
  }

  getWebSocketAutoResponseTimestamp(_ws: WebSocket): Date | null {
    return null;
  }

  setHibernatableWebSocketEventTimeout(_timeoutMs?: number): void {}

  getHibernatableWebSocketEventTimeout(): number | null {
    return null;
  }

  abort(_reason?: string): void {}
}

class InMemoryExecutionOwnerDirectoryNamespace implements McpExecutionOwnerDirectoryNamespace<string> {
  private readonly directories = new Map<string, McpExecutionOwnerDirectoryDO>();

  idFromName(name: string): string {
    return name;
  }

  get(id: string): McpExecutionOwnerDirectoryDO {
    let directory = this.directories.get(id);
    if (!directory) {
      directory = new McpExecutionOwnerDirectoryDO(new FakeDurableObjectState(id), {});
      this.directories.set(id, directory);
    }
    return directory;
  }
}

type TestDbHandle = {
  readonly end: () => void;
};

type ResumeCall = {
  readonly executionId: string;
  readonly response: ResumeResponse;
};

const completed = (result: unknown): ExecutionResult => ({
  status: "completed",
  result: { result },
});

const makeEngine = (
  resultForResume: (executionId: string, response: ResumeResponse) => ExecutionResult | null,
) => {
  const calls: ResumeCall[] = [];
  const orgWriteAccesses: OrgWriteAccess[] = [];
  const resume = vi.fn((executionId: string, response: ResumeResponse) =>
    Effect.gen(function* () {
      orgWriteAccesses.push(yield* currentOrgWriteAccess);
      calls.push({ executionId, response });
      return resultForResume(executionId, response);
    }),
  );
  const engine: ExecutionEngine<Cause.YieldableError> = {
    execute: () => Effect.succeed({ result: "execute-result" }),
    executeWithPause: () => Effect.succeed(completed("execute-result")),
    resume,
    getPausedExecution: () => Effect.succeed(null),
    pausedExecutionCount: () => Effect.succeed(0),
    hasPausedExecutions: () => Effect.succeed(false),
    getDescription: Effect.succeed("test engine"),
    // The fake forks nothing, so there is no sandbox fiber to end.
    shutdown: Effect.void,
  };
  return { calls, engine, orgWriteAccesses, resume };
};

/** An engine holding one live pause, like the real one: resuming runs the
 * paused tool call once, a resume that lands while it runs joins it, and the
 * id is then remembered as settled. `resumeMs` is how long the call runs. */
const makePausingEngine = (executionId: string, resumeMs = 0) => {
  const paused = new Set([executionId]);
  const settled = new Set<string>();
  const running = new Map<string, Promise<ExecutionResult>>();
  let sideEffects = 0;
  const { engine } = makeEngine(() => null);
  const pausingEngine: ExecutionEngine<Cause.YieldableError> = {
    ...engine,
    resume: (id) =>
      Effect.promise(async () => {
        const joined = running.get(id);
        if (joined) return joined;
        if (!paused.delete(id)) return null;
        sideEffects += 1;
        const run = new Promise<ExecutionResult>((resolve) => {
          const finish = () => {
            settled.add(id);
            running.delete(id);
            resolve(completed("owner-result"));
          };
          if (resumeMs === 0) finish();
          else setTimeout(finish, resumeMs);
        });
        running.set(id, run);
        return run;
      }),
    pausedExecutionCount: () => Effect.sync(() => paused.size),
    hasPausedExecutions: () => Effect.sync(() => paused.size > 0),
    isExecutionSettled: (id) => Effect.sync(() => settled.has(id)),
  };
  return { engine: pausingEngine, sideEffects: () => sideEffects };
};

const sessionMeta = (input?: Partial<SessionMeta>): SessionMeta => ({
  organizationId: "org_1",
  organizationName: "Test Org",
  orgRoleModel: "organization",
  userId: "acct_1",
  elicitationMode: "model",
  resource: defaultMcpResource,
  ...input,
});

const deadline = (): PausedExecutionDeadline => ({
  expiresAt: new Date(Date.now() + PAUSED_APPROVAL_TIMEOUT_MS).toISOString(),
  ttlMs: PAUSED_APPROVAL_TIMEOUT_MS,
});

const approval = {
  action: "accept",
  content: { approved: true },
} satisfies ResumeResponse;

type PendingApprovalLeaseSnapshot = {
  readonly timeout: ReturnType<typeof setTimeout> | null;
};

class HarnessSession extends McpAgentSessionDOBase<Cloudflare.Env, TestDbHandle> {
  private readonly meta: SessionMeta;
  private readonly fakeState: FakeDurableObjectState;
  private readonly directory: McpExecutionOwnerDirectory | null;
  private readonly modelResumeForward: (
    owner: McpExecutionOwnerRoute,
    identity: McpModelResumeCaller,
    executionId: string,
    response: ResumeResponse,
  ) => Effect.Effect<McpSessionModelResumeResult, unknown>;
  activeKeepAlives = 0;

  constructor(input: {
    readonly sessionId: string;
    readonly meta?: SessionMeta;
    readonly engine: ExecutionEngine<Cause.YieldableError>;
    readonly directoryNamespace: McpExecutionOwnerDirectoryNamespace<string>;
    readonly forwardModelResumeToOwner?: HarnessSession["modelResumeForward"];
  }) {
    const state = new FakeDurableObjectState(input.sessionId);
    super(state, {} as Cloudflare.Env);
    this.fakeState = state;
    this.meta = input.meta ?? sessionMeta();
    this.directory = mcpExecutionOwnerDirectoryFromNamespace(input.directoryNamespace);
    this.modelResumeForward =
      input.forwardModelResumeToOwner ??
      (() =>
        Effect.succeed({
          status: "execution_expired",
          ttlMs: PAUSED_APPROVAL_TIMEOUT_MS,
        }));
    this.installRuntime(input.engine);
  }

  protected override get sessionId(): string {
    return this.fakeState.id.toString();
  }

  protected override executionOwnerDirectory(): McpExecutionOwnerDirectory | null {
    return this.directory;
  }

  protected override forwardModelResumeToOwner(
    owner: McpExecutionOwnerRoute,
    identity: McpModelResumeCaller,
    executionId: string,
    response: ResumeResponse,
  ): Effect.Effect<McpSessionModelResumeResult, unknown> {
    return this.modelResumeForward(owner, identity, executionId, response);
  }

  protected override openSessionDb(): TestDbHandle {
    return { end: () => undefined };
  }

  protected override resolveSessionMeta(token: McpSessionInit): Effect.Effect<SessionMeta> {
    return Effect.succeed(
      sessionMeta({ organizationId: token.organizationId, userId: token.userId }),
    );
  }

  protected override buildMcpServer(): Effect.Effect<BuiltMcpServer> {
    return Effect.succeed({
      mcpServer: new McpServer({ name: "test", version: "1.0.0" }),
      engine: this["engine"]!,
    });
  }

  override keepAlive(): Promise<() => void> {
    this.activeKeepAlives += 1;
    return Promise.resolve(() => {
      this.activeKeepAlives -= 1;
      // The SDK re-arms the alarm when the last ref goes; with a destroy
      // pending it arms it for now.
      if (this.activeKeepAlives === 0) {
        this.fakeState.waitUntil(
          this.fakeState.storage.get("cf_agents_destroy_pending").then(async (pending) => {
            if (pending === true) await this.fakeState.storage.setAlarm(Date.now());
          }),
        );
      }
    });
  }

  async storeSessionMeta(): Promise<void> {
    await this.fakeState.storage.put("session-meta", this.meta);
  }

  /** Pause as the tool server does: it awaits the hook, then answers. Work the
   * hook leaves on `waitUntil` is not flushed — a DELETE can cancel it. */
  async startPause(executionId: string): Promise<void> {
    await Effect.runPromise(
      this.pausedExecutionHooks.onExecutionPaused?.(executionId, deadline()) ?? Effect.void,
    );
  }

  /** Run the Durable Object alarm if it is due, as the platform would. */
  async fireDueAlarm(): Promise<void> {
    await this.fakeState.flushWaitUntil();
    const at = this.fakeState.storage.alarmAt;
    if (at === null || at > Date.now()) return;
    this.fakeState.storage.alarmAt = null;
    await this.alarm();
    await this.fakeState.flushWaitUntil();
  }

  async flushWaitUntil(): Promise<void> {
    await this.fakeState.flushWaitUntil();
  }

  async storedSessionMeta(): Promise<unknown> {
    return this.fakeState.storage.get("session-meta");
  }

  /** Resume as the MCP `resume` tool does, under the request's write access. */
  async resumeViaModelTool(
    executionId: string,
    response: ResumeResponse,
    orgWriteAccess: OrgWriteAccess = "denied",
  ): Promise<McpSessionModelResumeResult | null> {
    const bound = Effect.provideService(
      CurrentOrgWriteAccess,
      makeOrgWriteAccessState(orgWriteAccess),
    );
    const local = await Effect.runPromise(
      this["engine"]!.resume(executionId, response).pipe(bound),
    );
    if (local) return { status: "result", result: formatMcpExecutionOutcome(local) };
    return Effect.runPromise(this.modelResumeFallback(executionId, response).pipe(bound));
  }

  pendingLease(executionId: string): PendingApprovalLeaseSnapshot | undefined {
    return this["pendingApprovalLeases"].get(executionId);
  }

  pendingLeaseCount(): number {
    return this["pendingApprovalLeases"].size;
  }

  private installRuntime(engine: ExecutionEngine<Cause.YieldableError>): void {
    this["engine"] = engine;
    this["dbHandle"] = { end: () => undefined };
    this["initialized"] = true;
    this.server = new McpServer({ name: "test", version: "1.0.0" });
  }
}

const makeDirectory = () => {
  const namespace = new InMemoryExecutionOwnerDirectoryNamespace();
  const directory = mcpExecutionOwnerDirectoryFromNamespace(namespace);
  expect(directory).not.toBeNull();
  return { directory: directory!, namespace };
};

const putOwnerRecord = (
  directory: McpExecutionOwnerDirectory,
  input: Partial<McpExecutionOwnerRecord>,
) =>
  Effect.runPromise(
    directory.put({
      executionId: "exec_1",
      owner: { sessionId: "session-a" },
      accountId: "acct_1",
      organizationId: "org_1",
      expiresAt: new Date(Date.now() + PAUSED_APPROVAL_TIMEOUT_MS).toISOString(),
      ttlMs: PAUSED_APPROVAL_TIMEOUT_MS,
      ...input,
    }),
  );

const flushMicrotasks = async (): Promise<void> => {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
};

describe("mcpSessionDurableObjectName", () => {
  it("uses the Agents streamable-http durable object name", () => {
    expect(mcpSessionDurableObjectName("session_123")).toBe("streamable-http:session_123");
  });
});

describe("McpAgentSessionDOBase cross-session model resume", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("routes a session B resume through the real directory path to session A and releases A's lease", async () => {
    const { directory, namespace } = makeDirectory();
    const ownerEngine = makeEngine((executionId, response) =>
      executionId === "exec_owner" && response.action === "accept"
        ? completed("owner-result")
        : null,
    );
    const requesterEngine = makeEngine(() => null);
    const sessions = new Map<string, HarnessSession>();
    const sessionNamespace = {
      idFromName: (name: string) => name,
      get: (id: string) => sessions.get(id),
    };
    const forward = vi.fn(
      (
        owner: McpExecutionOwnerRoute,
        identity: McpModelResumeCaller,
        executionId: string,
        response: ResumeResponse,
      ) =>
        Effect.promise(async () => {
          return mcpSessionStub(sessionNamespace, owner.sessionId).resumeExecutionForModel(
            executionId,
            identity,
            response,
          );
        }),
    );

    const sessionA = new HarnessSession({
      sessionId: "session-a",
      engine: ownerEngine.engine,
      directoryNamespace: namespace,
    });
    const sessionB = new HarnessSession({
      sessionId: "session-b",
      engine: requesterEngine.engine,
      directoryNamespace: namespace,
      forwardModelResumeToOwner: forward,
    });
    sessions.set(mcpSessionDurableObjectName("session-a"), sessionA);
    await sessionA.storeSessionMeta();
    await sessionB.storeSessionMeta();

    await sessionA.startPause("exec_owner");

    expect(await Effect.runPromise(directory.get("exec_owner"))).toMatchObject({
      executionId: "exec_owner",
      owner: { sessionId: "session-a" },
      accountId: "acct_1",
      organizationId: "org_1",
    });
    expect(sessionA.pendingLeaseCount()).toBe(1);
    expect(sessionA.pendingLease("exec_owner")?.timeout).not.toBeNull();
    expect(vi.getTimerCount()).toBe(1);

    const outcome = await sessionB.resumeViaModelTool("exec_owner", approval);
    await flushMicrotasks();

    expect(outcome).toMatchObject({
      status: "result",
      result: {
        structuredContent: {
          status: "completed",
          result: "owner-result",
        },
      },
    });
    expect(forward).toHaveBeenCalledTimes(1);
    expect(requesterEngine.resume).toHaveBeenCalledTimes(1);
    expect(ownerEngine.resume).toHaveBeenCalledTimes(1);
    expect(ownerEngine.calls).toEqual([{ executionId: "exec_owner", response: approval }]);
    expect(sessionA.pendingLeaseCount()).toBe(0);
    expect(sessionA.activeKeepAlives).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
    expect(await Effect.runPromise(directory.get("exec_owner"))).toBeNull();

    await vi.advanceTimersByTimeAsync(PAUSED_APPROVAL_TIMEOUT_MS + 1);
    await flushMicrotasks();

    expect(ownerEngine.resume).toHaveBeenCalledTimes(1);
    expect(ownerEngine.calls).toEqual([{ executionId: "exec_owner", response: approval }]);
  });

  for (const orgWriteAccess of ["allowed", "denied"] as const) {
    it(`resumes the owning session under the requester's ${orgWriteAccess} workspace-write access`, async () => {
      const { namespace } = makeDirectory();
      const ownerEngine = makeEngine(() => completed("owner-result"));
      const requesterEngine = makeEngine(() => null);
      const sessions = new Map<string, HarnessSession>();
      const sessionNamespace = {
        idFromName: (name: string) => name,
        get: (id: string) => sessions.get(id),
      };
      const sessionA = new HarnessSession({
        sessionId: "session-a",
        engine: ownerEngine.engine,
        directoryNamespace: namespace,
      });
      const sessionB = new HarnessSession({
        sessionId: "session-b",
        engine: requesterEngine.engine,
        directoryNamespace: namespace,
        forwardModelResumeToOwner: (owner, identity, executionId, response) =>
          Effect.promise(() =>
            mcpSessionStub(sessionNamespace, owner.sessionId).resumeExecutionForModel(
              executionId,
              identity,
              response,
            ),
          ),
      });
      sessions.set(mcpSessionDurableObjectName("session-a"), sessionA);
      await sessionA.storeSessionMeta();
      await sessionB.storeSessionMeta();
      await sessionA.startPause("exec_owner");

      await sessionB.resumeViaModelTool("exec_owner", approval, orgWriteAccess);

      expect(ownerEngine.orgWriteAccesses).toEqual([orgWriteAccess]);
    });
  }

  it("treats a forwarded resume without workspace-write access as denied", async () => {
    const { namespace } = makeDirectory();
    const ownerEngine = makeEngine(() => completed("owner-result"));
    const sessionA = new HarnessSession({
      sessionId: "session-a",
      engine: ownerEngine.engine,
      directoryNamespace: namespace,
    });
    await sessionA.storeSessionMeta();
    await sessionA.startPause("exec_owner");

    // A requester still running the previous deploy sends only the owner pair.
    const legacyIdentity = { accountId: "acct_1", organizationId: "org_1" };
    await sessionA.resumeExecutionForModel(
      "exec_owner",
      legacyIdentity as McpModelResumeCaller,
      approval,
    );

    expect(ownerEngine.orgWriteAccesses).toEqual(["denied"]);
  });

  it("rejects identity mismatch without invoking the owning session engine", async () => {
    const { directory, namespace } = makeDirectory();
    const ownerEngine = makeEngine(() => completed("should-not-run"));
    const requesterEngine = makeEngine(() => null);
    const forward = vi.fn(() =>
      Effect.succeed({
        status: "execution_expired" as const,
        ttlMs: PAUSED_APPROVAL_TIMEOUT_MS,
      }),
    );
    const sessionA = new HarnessSession({
      sessionId: "session-a",
      engine: ownerEngine.engine,
      directoryNamespace: namespace,
    });
    const sessionB = new HarnessSession({
      sessionId: "session-b",
      meta: sessionMeta({ organizationId: "org_other" }),
      engine: requesterEngine.engine,
      directoryNamespace: namespace,
      forwardModelResumeToOwner: forward,
    });
    await sessionA.storeSessionMeta();
    await sessionB.storeSessionMeta();
    await sessionA.startPause("exec_forbidden");

    const outcome = await sessionB.resumeViaModelTool("exec_forbidden", approval);

    expect(outcome).toEqual({ status: "execution_forbidden" });
    expect(forward).not.toHaveBeenCalled();
    expect(ownerEngine.resume).not.toHaveBeenCalled();
    expect(sessionA.pendingLeaseCount()).toBe(1);
    expect(await Effect.runPromise(directory.get("exec_forbidden"))).toMatchObject({
      executionId: "exec_forbidden",
      owner: { sessionId: "session-a" },
    });
  });

  it("fails the pause, holding no lease, when the owner record cannot be written", async () => {
    let putAttempts = 0;
    const unwritableDirectory: McpExecutionOwnerDirectoryNamespace<string> = {
      idFromName: (name) => name,
      get: () => ({
        put: () => {
          putAttempts += 1;
          return Effect.runPromise(Effect.fail("directory unavailable"));
        },
        get: () => Promise.resolve(null),
        delete: () => Promise.resolve(),
      }),
    };
    const sessionA = new HarnessSession({
      sessionId: "session-a",
      engine: makeEngine(() => null).engine,
      directoryNamespace: unwritableDirectory,
    });
    await sessionA.storeSessionMeta();

    const pausing = Effect.runPromiseExit(
      Effect.tryPromise(() => sessionA.startPause("exec_unrecorded")),
    );
    // The retry is scheduled on the (fake) clock.
    await vi.advanceTimersByTimeAsync(1);
    expect(Exit.isFailure(await pausing)).toBe(true);

    expect(putAttempts).toBe(2);
    expect(sessionA.pendingLeaseCount()).toBe(0);
    expect(sessionA.activeKeepAlives).toBe(0);
  });

  it("answers a slow owner session forward as in progress and keeps the record", async () => {
    const { directory, namespace } = makeDirectory();
    const requesterEngine = makeEngine(() => null);
    const forward = vi.fn(() => Effect.never);
    const sessionB = new HarnessSession({
      sessionId: "session-b",
      engine: requesterEngine.engine,
      directoryNamespace: namespace,
      forwardModelResumeToOwner: forward,
    });
    await sessionB.storeSessionMeta();
    await putOwnerRecord(directory, {
      executionId: "exec_timeout",
      owner: { sessionId: "session-a" },
    });

    const pending = sessionB.resumeViaModelTool("exec_timeout", approval);
    await vi.advanceTimersByTimeAsync(10_000);

    // The owner may still be running the resumed call; it is not expired.
    await expect(pending).resolves.toEqual({ status: "execution_in_progress" });
    expect(forward).toHaveBeenCalledTimes(1);
    expect(await Effect.runPromise(directory.get("exec_timeout"))).toMatchObject({
      executionId: "exec_timeout",
      owner: { sessionId: "session-a" },
    });
  });

  describe("after the client DELETEs the owning session", () => {
    const setup = async (resumeMs = 0) => {
      const { directory, namespace } = makeDirectory();
      const owner = makePausingEngine("exec_owner", resumeMs);
      const sessions = new Map<string, HarnessSession>();
      const sessionNamespace = {
        idFromName: (name: string) => name,
        get: (id: string) => sessions.get(id),
      };
      const newSession = (sessionId: string, engine = makeEngine(() => null).engine) =>
        new HarnessSession({
          sessionId,
          engine,
          directoryNamespace: namespace,
          forwardModelResumeToOwner: (route, identity, executionId, response) =>
            Effect.promise(() =>
              mcpSessionStub(sessionNamespace, route.sessionId).resumeExecutionForModel(
                executionId,
                identity,
                response,
              ),
            ),
        });
      const sessionA = newSession("session-a", owner.engine);
      sessions.set(mcpSessionDurableObjectName("session-a"), sessionA);
      await sessionA.storeSessionMeta();
      await sessionA.startPause("exec_owner");

      // Cursor sends DELETE right after the paused call; the SDK runs the
      // destroy from an alarm about a second later.
      await sessionA._cf_scheduleDestroy();
      await vi.advanceTimersByTimeAsync(1000);
      await sessionA.fireDueAlarm();

      const sessionB = newSession("session-b");
      await sessionB.storeSessionMeta();
      return { directory, owner, sessionA, sessionB, newSession };
    };

    it("still resumes the pause from a new session", async () => {
      const { owner, sessionB } = await setup();

      const outcome = await sessionB.resumeViaModelTool("exec_owner", approval);

      expect(outcome).toMatchObject({
        status: "result",
        result: { structuredContent: { status: "completed", result: "owner-result" } },
      });
      expect(owner.sideEffects()).toBe(1);
    });

    it("runs the call once when two sessions resume at the same time", async () => {
      const { owner, sessionB, newSession } = await setup(1000);
      const sessionC = newSession("session-c");
      await sessionC.storeSessionMeta();

      const first = sessionB.resumeViaModelTool("exec_owner", approval);
      const second = sessionC.resumeViaModelTool("exec_owner", approval);
      await vi.advanceTimersByTimeAsync(1000);

      for (const outcome of await Promise.all([first, second])) {
        expect(outcome).toMatchObject({
          status: "result",
          result: { structuredContent: { status: "completed", result: "owner-result" } },
        });
      }
      expect(owner.sideEffects()).toBe(1);

      const sessionD = newSession("session-d");
      await sessionD.storeSessionMeta();
      expect(await sessionD.resumeViaModelTool("exec_owner", approval)).toEqual({
        status: "execution_already_settled",
      });
      expect(owner.sideEffects()).toBe(1);
    });

    it("answers a second resume as already settled without running the call again", async () => {
      const { owner, sessionA, sessionB, newSession } = await setup();
      await sessionB.resumeViaModelTool("exec_owner", approval);
      // The resume released the lease, so the deferred destroy now runs.
      await sessionA.fireDueAlarm();

      // Later than the original 4-minute pause deadline.
      await vi.advanceTimersByTimeAsync(5 * 60 * 1000);
      const sessionC = newSession("session-c");
      await sessionC.storeSessionMeta();
      const retry = await sessionC.resumeViaModelTool("exec_owner", approval);

      expect(retry).toEqual({ status: "execution_already_settled" });
      expect(owner.sideEffects()).toBe(1);
    });

    it("expires an overdue lease from the alarm when its timer never fired", async () => {
      const { directory, sessionA } = await setup();
      // The in-isolate expiry timer is lost; only the durable alarm is left.
      vi.clearAllTimers();

      await vi.advanceTimersByTimeAsync(PAUSED_APPROVAL_TIMEOUT_MS + 1);
      await sessionA.fireDueAlarm();

      expect(sessionA.pendingLeaseCount()).toBe(0);
      expect(await sessionA.storedSessionMeta()).toBeUndefined();
      expect(await Effect.runPromise(directory.get("exec_owner"))).toMatchObject({
        outcome: "expired",
      });
    });

    it("destroys the session once an unanswered lease expires", async () => {
      const { directory, sessionA, sessionB } = await setup();
      // Held open past the DELETE while the approval is outstanding.
      expect(await sessionA.storedSessionMeta()).toBeDefined();

      await vi.advanceTimersByTimeAsync(PAUSED_APPROVAL_TIMEOUT_MS);
      await sessionA.flushWaitUntil();
      await sessionA.fireDueAlarm();

      expect(sessionA.pendingLeaseCount()).toBe(0);
      expect(sessionA.activeKeepAlives).toBe(0);
      expect(await sessionA.storedSessionMeta()).toBeUndefined();
      expect(await Effect.runPromise(directory.get("exec_owner"))).toMatchObject({
        outcome: "expired",
      });
      expect(await sessionB.resumeViaModelTool("exec_owner", approval)).toEqual({
        status: "execution_expired",
        ttlMs: PAUSED_APPROVAL_TIMEOUT_MS,
      });
    });
  });

  it("does not recurse when the directory maps a local miss back to the same session", async () => {
    const { directory, namespace } = makeDirectory();
    const engine = makeEngine(() => null);
    const forward = vi.fn(() =>
      Effect.succeed({
        status: "result" as const,
        result: formatMcpExecutionOutcome(completed("should-not-forward")),
      }),
    );
    const sessionB = new HarnessSession({
      sessionId: "session-b",
      engine: engine.engine,
      directoryNamespace: namespace,
      forwardModelResumeToOwner: forward,
    });
    await sessionB.storeSessionMeta();
    await putOwnerRecord(directory, {
      executionId: "exec_self",
      owner: { sessionId: "session-b" },
    });

    const outcome = await sessionB.resumeViaModelTool("exec_self", approval);

    expect(outcome).toEqual({
      status: "execution_expired",
      ttlMs: PAUSED_APPROVAL_TIMEOUT_MS,
    });
    expect(engine.resume).toHaveBeenCalledTimes(1);
    expect(forward).not.toHaveBeenCalled();
    expect(await Effect.runPromise(directory.get("exec_self"))).toBeNull();
  });
});
