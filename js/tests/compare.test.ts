import { readFile } from "node:fs/promises";
import path from "node:path";

import { beforeEach, describe, expect, it } from "vitest";

import { compare } from "../src/compare.js";
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

describe("compare()", () => {
  it("same version → all rows in same, no diff", async () => {
    const r = await compare(["20260401", "20260401"], { baseUrl, fetch: localFetch });
    expect(r.va).toBe("20260401");
    expect(r.vb).toBe("20260401");
    expect(r.diff.length).toBe(0);
    expect(r.same.length).toBeGreaterThan(0);
    // same 은 emdcd 당 2 rows (va + vb 같아도)
    expect(r.same.length % 2).toBe(0);
  });

  it("adjacent versions — mostly same, some diff", async () => {
    const r = await compare(["20260201", "20260401"], { baseUrl, fetch: localFetch });
    expect(r.same.length).toBeGreaterThan(0);
    // diff 는 changed / only_in_* 중 하나
    for (const d of r.diff) {
      expect(["changed", "only_in_a", "only_in_b"]).toContain(d.status);
    }
    const changed = r.diff.filter((d) => d.status === "changed");
    // changed 는 항상 쌍으로 (A + B)
    expect(changed.length % 2).toBe(0);
  });

  it("distant versions — more diff than adjacent", async () => {
    const adj = await compare(["20251001", "20251231"], { baseUrl, fetch: localFetch });
    const far = await compare(["20001231", "20260401"], { baseUrl, fetch: localFetch });
    expect(far.diff.length).toBeGreaterThan(adj.diff.length);
  });

  it("threshold 1.0 promotes nothing — diff count ≥ default", async () => {
    const def = await compare(["20011231", "20260401"], { baseUrl, fetch: localFetch });
    const strict = await compare(["20011231", "20260401"], {
      baseUrl,
      fetch: localFetch,
      threshold: 1.0,
    });
    expect(strict.diff.length).toBeGreaterThanOrEqual(def.diff.length);
  });

  it("rejects unknown version key", async () => {
    await expect(
      compare(["99999999", "20260401"], { baseUrl, fetch: localFetch }),
    ).rejects.toThrow(/unknown version key/);
  });

  it("rejects non-tuple input", async () => {
    await expect(
      // @ts-expect-error
      compare(["20260401"], { baseUrl, fetch: localFetch }),
    ).rejects.toThrow(/tuple of exactly 2/);
  });
});
