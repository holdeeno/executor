import { afterEach, describe, expect, it } from "@effect/vitest";
// oxlint-disable-next-line executor/no-vitest-import -- boundary: spies come from vitest itself
import { vi } from "vitest";
import { Effect } from "effect";
import { errors } from "jose";

import { orgWriteAccessForPrincipal } from "@executor-js/host-mcp";

import type { CloudflareConfig } from "../config";
import { makeAccessVerifier, principalFromAccessClaims } from "./cloudflare-access";

const config: CloudflareConfig = {
  accessTeamDomain: "team.cloudflareaccess.com",
  accessAud: "aud-tag",
  accessNameClaim: "name",
  accessGroupsClaim: "groups",
  adminEmails: ["admin@example.com"],
  organizationId: "default",
  organizationName: "Default",
  organizationSlug: "default",
  secretKey: "x".repeat(32),
  allowLocalNetwork: false,
  webBaseUrl: "https://localhost",
  enableDevAuth: false,
};

describe("principalFromAccessClaims", () => {
  it("maps a human identity (email + sub + groups)", () => {
    const p = principalFromAccessClaims(
      { sub: "user-123", email: "person@example.com", name: "Person", groups: ["eng"] },
      config,
    );
    expect(p.accountId).toBe("user-123");
    expect(p.email).toBe("person@example.com");
    expect(p.name).toBe("Person");
    expect(p.roles).toEqual(["eng"]);
    expect(p.organizationId).toBe("default");
  });

  it("grants admin when the email is in the allowlist", () => {
    const p = principalFromAccessClaims({ sub: "u", email: "ADMIN@example.com" }, config);
    expect(p.roles).toContain("admin");
    expect(p.orgRoleModel).toBe("organization");
    expect(p.orgRole).toBe("admin");
    expect(orgWriteAccessForPrincipal(p)).toBe("allowed");
  });

  it("gives a SERVICE TOKEN (common_name, no email/sub) a stable identity", () => {
    // Cloudflare Access service-token JWT: common_name set, email/sub absent.
    const p = principalFromAccessClaims({ common_name: "df8a20db.access", type: "app" }, config);
    expect(p.accountId).toBe("df8a20db.access"); // not empty — stable per token
    expect(p.name).toBe("df8a20db.access");
    expect(p.email).toBe("");
    expect(p.roles).toEqual(["member"]); // a token is a member, not an admin
    expect(p.organizationId).toBe("default");
  });

  it("defaults to member when there are no groups and no admin match", () => {
    const p = principalFromAccessClaims({ sub: "u", email: "nobody@other.com" }, config);
    expect(p.roles).toEqual(["member"]);
    expect(p.orgRoleModel).toBe("organization");
    expect(p.orgRole).toBe("member");
    expect(orgWriteAccessForPrincipal(p)).toBe("denied");
  });
});

describe("makeAccessVerifier signing keys fetch", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("logs a timed-out certs fetch instead of passing it off as a bad token", async () => {
    const warn = vi.spyOn(console, "warn");
    const header = btoa(JSON.stringify({ alg: "RS256", kid: "key-1" }));
    const payload = btoa(JSON.stringify({ sub: "u" }));
    const { verify } = makeAccessVerifier(config, () =>
      Effect.runPromise(Effect.fail(new errors.JWKSTimeout())),
    );

    const principal = await Effect.runPromise(
      verify(
        new Request("https://executor.example.com/mcp", {
          headers: { "Cf-Access-Jwt-Assertion": `${header}.${payload}.c2ln` },
        }),
      ),
    );

    expect(principal).toBeNull();
    expect(warn.mock.calls.flat().join("\n")).toContain("ERR_JWKS_TIMEOUT");
  });
});
