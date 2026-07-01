import { beforeEach, describe, expect, it } from "vitest";

import { VERSIONS, versions, versionsAsync } from "../src/versions.js";
import { clearManifestCache } from "../src/changelog.js";

function mockManifestFetch(body: unknown): typeof fetch {
  return (async () =>
    new Response(JSON.stringify(body), {
      status: 200,
      headers: { "content-type": "application/json" },
    })) as unknown as typeof fetch;
}

describe("versions()", () => {
  it("returns all versions when called with no args", () => {
    const all = versions();
    expect(all.length).toBe(VERSIONS.length);
    expect(all.length).toBe(63);
    expect(all[0]).toBe("19751231");
    expect(all.at(-1)).toBe("20260701");
  });

  it("filters by year", () => {
    const y2025 = versions(2025);
    expect(y2025.length).toBeGreaterThan(0);
    for (const k of y2025) {
      expect(k.startsWith("2025")).toBe(true);
    }
  });

  it("returns empty array for years with no data", () => {
    expect(versions(1800)).toEqual([]);
  });

  it("rejects non-integer year", () => {
    // @ts-expect-error testing runtime check
    expect(() => versions("2025")).toThrow(TypeError);
  });

  it("VERSIONS is sorted ascending", () => {
    const sorted = [...VERSIONS].sort();
    expect(VERSIONS).toEqual(sorted);
  });
});

describe("versionsAsync()", () => {
  beforeEach(() => clearManifestCache());

  it("returns manifest.versions when present (can exceed embedded VERSIONS)", async () => {
    const remote = [...VERSIONS, "29991231"];
    const all = await versionsAsync(undefined, {
      fetch: mockManifestFetch({ versions: remote }),
    });
    expect(all).toEqual(remote);
    expect(all.at(-1)).toBe("29991231");
  });

  it("filters manifest.versions by year", async () => {
    const y2025 = await versionsAsync(2025, {
      fetch: mockManifestFetch({ versions: VERSIONS }),
    });
    expect(y2025.length).toBeGreaterThan(0);
    for (const k of y2025) expect(k.startsWith("2025")).toBe(true);
  });

  it("falls back to embedded VERSIONS when manifest lacks versions", async () => {
    const all = await versionsAsync(undefined, {
      fetch: mockManifestFetch({ data_version: "x" }),
    });
    expect(all).toEqual([...VERSIONS]);
  });

  it("falls back to embedded VERSIONS on network failure", async () => {
    const failing = (async () => {
      throw new Error("network down");
    }) as unknown as typeof fetch;
    const all = await versionsAsync(undefined, { fetch: failing });
    expect(all).toEqual([...VERSIONS]);
  });
});
