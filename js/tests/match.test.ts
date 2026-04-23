import { readFile } from "node:fs/promises";
import path from "node:path";

import { beforeEach, describe, expect, it } from "vitest";

import { matchAdm } from "../src/match.js";
import { clearIndexCache } from "../src/find.js";

const repoRoot = path.resolve(__dirname, "..", "..");
const baseUrl = "file:///local-data";

const localFetch: typeof fetch = async (input) => {
  const url = typeof input === "string" ? input : (input as URL).toString();
  if (!url.startsWith(baseUrl)) throw new Error(`local-only fetch; got ${url}`);
  const filename = url.slice(baseUrl.length + 1);
  const buf = await readFile(
    path.join(repoRoot, "lib", "src", "admdongkor", "data", filename),
  );
  return new Response(buf, { status: 200, statusText: "OK" });
};

beforeEach(() => {
  clearIndexCache();
});

describe("matchAdm()", () => {
  it("matches same-version region with weight 1", async () => {
    const r = await matchAdm({
      base: "20260401",
      region: "11", // 서울특별시
      target: "20260401",
      baseUrl,
      fetch: localFetch,
    });
    expect(r.emd.length).toBeGreaterThan(0);
    for (const row of r.emd) {
      expect(row.version_key).toBe("20260401");
      expect(row.sidocd).toBe("11");
      expect(row.weight).toBeCloseTo(1.0, 5);
    }
  });

  it("projects to sgg level", async () => {
    const r = await matchAdm({
      base: "20260401",
      region: "11",
      target: "20260401",
      baseUrl,
      fetch: localFetch,
    });
    const sggs = await r.sgg();
    expect(sggs.length).toBeGreaterThan(10);
    for (const row of sggs) {
      expect(row.sidocd).toBe("11");
      expect(row.weight).toBeLessThanOrEqual(1);
    }
  });

  it("projects to sido level", async () => {
    const r = await matchAdm({
      base: "20260401",
      region: "11",
      target: "20260401",
      baseUrl,
      fetch: localFetch,
    });
    const sidos = await r.sido();
    expect(sidos.length).toBe(1);
    expect(sidos[0]!.sidocd).toBe("11");
    expect(sidos[0]!.weight).toBeCloseTo(1, 5);
  });

  it("cross-version region matching (2015 sgg → 2025)", async () => {
    const r = await matchAdm({
      base: "20151231",
      region: "11110", // 종로구
      target: "20251231",
      baseUrl,
      fetch: localFetch,
    });
    expect(r.emd.length).toBeGreaterThan(0);
    for (const row of r.emd) {
      expect(row.version_key).toBe("20251231");
      expect(row.weight).toBeGreaterThan(0);
      expect(row.weight).toBeLessThanOrEqual(1);
    }
  });

  it("multi-target returns rows for each version", async () => {
    const r = await matchAdm({
      base: "20260401",
      region: "11",
      target: ["20101231", "20201001", "20260401"],
      baseUrl,
      fetch: localFetch,
    });
    const vs = new Set(r.emd.map((e) => e.version_key));
    expect(vs.size).toBe(3);
  });

  it("unknown region returns empty", async () => {
    const r = await matchAdm({
      base: "20260401",
      region: "99", // 존재하지 않는 sidocd
      target: "20260401",
      baseUrl,
      fetch: localFetch,
    });
    expect(r.emd.length).toBe(0);
  });

  it("rejects invalid region length", async () => {
    await expect(
      matchAdm({
        base: "20260401",
        region: "123",
        target: "20260401",
        baseUrl,
        fetch: localFetch,
      }),
    ).rejects.toThrow(/2\/5\/7\/10 digit code/);
  });
});
