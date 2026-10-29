import { DurableObject } from "cloudflare:workers";
import { Data, Effect } from "effect";

export type McpExecutionOwnerRoute = {
  readonly sessionId: string;
};

/** What became of a pause after it stopped waiting: a resume is running, it
 * resumed to a result, its approval window ran out, or a forward to its owner
 * failed and nobody knows whether the call ran. */
export type McpExecutionOwnerOutcome = "resuming" | "settled" | "expired" | "unknown";

export type McpExecutionOwnerRecord = {
  readonly executionId: string;
  readonly owner: McpExecutionOwnerRoute;
  readonly accountId: string;
  readonly organizationId: string;
  readonly expiresAt: string;
  readonly ttlMs: number;
  /** Set once the pause stops waiting. The record then lives on its own
   * `expiresAt` so a late resume from any session is answered, never run
   * again. */
  readonly outcome?: McpExecutionOwnerOutcome;
};

export interface McpExecutionOwnerDirectory {
  /** Stores the record if its outcome may replace the stored one (see
   * {@link mayReplace}), and answers with the record held afterwards. */
  readonly put: (
    record: McpExecutionOwnerRecord,
  ) => Effect.Effect<McpExecutionOwnerRecord | null, unknown>;
  readonly get: (executionId: string) => Effect.Effect<McpExecutionOwnerRecord | null, unknown>;
  readonly delete: (executionId: string) => Effect.Effect<void, unknown>;
}

export interface McpExecutionOwnerDirectoryStub {
  readonly put: (record: McpExecutionOwnerRecord) => Promise<McpExecutionOwnerRecord | null>;
  readonly get: (executionId: string) => Promise<McpExecutionOwnerRecord | null>;
  readonly delete: (executionId: string) => Promise<void>;
}

export interface McpExecutionOwnerDirectoryNamespace<Id> {
  readonly idFromName: (name: string) => Id;
  readonly get: (id: Id) => unknown;
}

type McpExecutionOwnerDirectoryStorage = DurableObjectState["storage"];

const toMcpExecutionOwnerDirectoryStub = (stub: unknown): McpExecutionOwnerDirectoryStub =>
  stub as McpExecutionOwnerDirectoryStub;

export const mcpSessionDurableObjectName = (sessionId: string): string =>
  `streamable-http:${sessionId}`;

class McpExecutionOwnerDirectoryRpcError extends Data.TaggedError(
  "McpExecutionOwnerDirectoryRpcError",
)<{
  readonly cause: unknown;
}> {}

const RECORD_KEY = "owner";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isOwnerRoute = (value: unknown): value is McpExecutionOwnerRoute =>
  isRecord(value) && typeof value.sessionId === "string";

const isOwnerRecord = (value: unknown): value is McpExecutionOwnerRecord =>
  isRecord(value) &&
  typeof value.executionId === "string" &&
  isOwnerRoute(value.owner) &&
  typeof value.accountId === "string" &&
  typeof value.organizationId === "string" &&
  typeof value.expiresAt === "string" &&
  typeof value.ttlMs === "number";

const expiryMs = (record: McpExecutionOwnerRecord): number => Date.parse(record.expiresAt);

const outcomeStage = (outcome: McpExecutionOwnerOutcome | undefined): number =>
  outcome === undefined ? 0 : outcome === "resuming" ? 1 : 2;

/**
 * Outcomes only move forward: none, then resuming, then settled, expired or
 * unknown. Settled and expired are final, and unknown never replaces resuming:
 * a forward that timed out does not know more than the owner that is running
 * the call. Unknown may still be replaced by settled once the owner finishes.
 */
const mayReplace = (stored: McpExecutionOwnerRecord, next: McpExecutionOwnerRecord): boolean => {
  if (stored.outcome === "settled" || stored.outcome === "expired") return false;
  if (stored.outcome === "resuming" && next.outcome === "unknown") return false;
  return outcomeStage(next.outcome) >= outcomeStage(stored.outcome);
};

export class McpExecutionOwnerDirectoryDO extends DurableObject<unknown> {
  private readonly storage: McpExecutionOwnerDirectoryStorage;

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    this.storage = ctx.storage;
  }

  async put(record: McpExecutionOwnerRecord): Promise<McpExecutionOwnerRecord | null> {
    const stored = await this.get(record.executionId);
    const expiresAtMs = expiryMs(record);
    if (!Number.isFinite(expiresAtMs)) return stored;
    if (stored && !mayReplace(stored, record)) return stored;
    await this.storage.put(RECORD_KEY, record);
    await this.storage.setAlarm(expiresAtMs);
    return record;
  }

  async get(executionId: string): Promise<McpExecutionOwnerRecord | null> {
    const stored = await this.storage.get<unknown>(RECORD_KEY);
    if (!isOwnerRecord(stored) || stored.executionId !== executionId) return null;
    const expiresAtMs = expiryMs(stored);
    if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
      await this.delete(executionId);
      return null;
    }
    return stored;
  }

  async delete(executionId: string): Promise<void> {
    const stored = await this.storage.get<unknown>(RECORD_KEY);
    if (isOwnerRecord(stored) && stored.executionId !== executionId) return;
    await Promise.all([this.storage.delete(RECORD_KEY), this.storage.deleteAlarm()]);
  }

  override async alarm(): Promise<void> {
    await Promise.all([this.storage.delete(RECORD_KEY), this.storage.deleteAlarm()]);
  }
}

export const mcpExecutionOwnerDirectoryFromNamespace = <Id>(
  namespace: McpExecutionOwnerDirectoryNamespace<Id> | undefined,
): McpExecutionOwnerDirectory | null => {
  if (!namespace) return null;
  const stubFor = (executionId: string): McpExecutionOwnerDirectoryStub =>
    toMcpExecutionOwnerDirectoryStub(namespace.get(namespace.idFromName(executionId)));
  return {
    put: (record) =>
      Effect.tryPromise({
        try: () => stubFor(record.executionId).put(record),
        catch: (cause) => new McpExecutionOwnerDirectoryRpcError({ cause }),
      }),
    get: (executionId) =>
      Effect.tryPromise({
        try: () => stubFor(executionId).get(executionId),
        catch: (cause) => new McpExecutionOwnerDirectoryRpcError({ cause }),
      }),
    delete: (executionId) =>
      Effect.tryPromise({
        try: () => stubFor(executionId).delete(executionId),
        catch: (cause) => new McpExecutionOwnerDirectoryRpcError({ cause }),
      }),
  };
};
