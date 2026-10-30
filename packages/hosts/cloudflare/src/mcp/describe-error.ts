import { Cause } from "effect";

/**
 * A failure described for a log line. Built from whatever was thrown or
 * failed, Error or not: a tagged error usually carries no message of its own,
 * so its tag, fields and cause are what say what went wrong.
 */
export type ErrorDetail = {
  readonly kind?: "failure" | "defect" | "interrupt";
  readonly name?: string;
  readonly tag?: string;
  readonly message?: string;
  readonly status?: number;
  readonly body?: string;
  readonly fields?: Record<string, string | number | boolean | null>;
  readonly cause?: ErrorDetail;
};

const MAX_TEXT = 300;
const MAX_CAUSE_DEPTH = 3;
const DESCRIBED_KEYS = new Set([
  "_tag",
  "name",
  "message",
  "stack",
  "cause",
  "status",
  "statusCode",
  "body",
]);

const clip = (text: string): string =>
  text.length > MAX_TEXT ? `${text.slice(0, MAX_TEXT)}...` : text;

const fieldValue = (value: unknown): string | number | boolean | null => {
  if (value === null || typeof value === "number" || typeof value === "boolean") return value;
  if (typeof value === "string") return clip(value);
  if (typeof value === "object") return "[object]";
  return clip(String(value));
};

const describeValue = (value: unknown, depth: number): ErrorDetail => {
  if (value === null || typeof value !== "object") return { message: clip(String(value)) };
  const record = value as Record<string, unknown>;
  const fields = Object.fromEntries(
    Object.entries(record)
      .filter(([key]) => !DESCRIBED_KEYS.has(key))
      .map(([key, field]) => [key, fieldValue(field)]),
  );
  const status = record.status ?? record.statusCode;
  return {
    ...(typeof record.name === "string" && record.name !== "Error" ? { name: record.name } : {}),
    // oxlint-disable-next-line executor/no-manual-tag-check -- boundary: reads an unknown failure's tag for a log line; there is no tag to match against.
    ...(typeof record._tag === "string" ? { tag: record._tag } : {}),
    ...(typeof record.message === "string" && record.message !== ""
      ? { message: clip(record.message) }
      : {}),
    ...(typeof status === "number" ? { status } : {}),
    ...(typeof record.body === "string" ? { body: clip(record.body) } : {}),
    ...(Object.keys(fields).length > 0 ? { fields } : {}),
    ...(record.cause !== undefined && depth < MAX_CAUSE_DEPTH
      ? { cause: describeValue(record.cause, depth + 1) }
      : {}),
  };
};

/** Describe what ended an effect, failure, defect or interruption. */
export const describeCause = (cause: Cause.Cause<unknown>): ErrorDetail => {
  if (Cause.hasInterruptsOnly(cause)) return { kind: "interrupt" };
  return {
    kind: Cause.hasFails(cause) ? "failure" : "defect",
    ...describeValue(Cause.squash(cause), 0),
  };
};

/** One readable line: `Tag (HTTP 503): message <- Cause: message`. */
export const summarizeErrorDetail = (detail: ErrorDetail): string => {
  const head = [
    [detail.tag ?? detail.name, detail.status === undefined ? "" : `(HTTP ${detail.status})`]
      .filter(Boolean)
      .join(" "),
    detail.message,
  ]
    .filter(Boolean)
    .join(": ");
  const line = detail.cause ? `${head} <- ${summarizeErrorDetail(detail.cause)}` : head;
  return line || detail.kind || "unknown error";
};
