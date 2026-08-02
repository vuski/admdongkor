import { readFile } from "node:fs/promises";
import path from "node:path";

import { beforeEach, describe, expect, it } from "vitest";

import {
  find,
  findFirst,
  findLast,
  findVersions,
  clearIndexCache,
} from "../src/find.js";

const repoRoot = path.resolve(__dirname, "..", "..");
const baseUrl = "file:///local-data";

const localFetch: typeof fetch = async (input) => {
  const url = typeof input === "string" ? input : (input as URL).toString();
  if (!url.startsWith(baseUrl)) {
    throw new Error(`local-only fetch; got ${url}`);
  }
  const filename = url.slice(baseUrl.length + 1);
  const buf = await readFile(
    path.join(repoRoot, "lib", "src", "admdongkor", "data", filename),
  );
  return new Response(buf, { status: 200, statusText: "OK" });
};

beforeEach(() => {
  clearIndexCache();
});

describe("find()", () => {
  it("finds substring across levels with a single token", async () => {
    const rows = await find("여주군", { baseUrl, fetch: localFetch });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      const hay = `${r.sidonm ?? ""}${r.sggnm ?? ""}${r.name}`;
      expect(hay.includes("여주군")).toBe(true);
    }
    const versions = findVersions(rows);
    expect(versions.length).toBeGreaterThan(0);
    expect(findFirst(rows)).toBe(versions[0]);
    expect(findLast(rows)).toBe(versions[versions.length - 1]);
  });

  it("two tokens auto-filter to sgg", async () => {
    const rows = await find("서울특별시 종로구", {
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      expect(r.level).toBe("sgg");
    }
  });

  it("three tokens auto-filter to emd", async () => {
    const rows = await find("서울특별시 종로구 사직동", {
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      expect(r.level).toBe("emd");
    }
  });

  it("level option overrides auto-level", async () => {
    const rows = await find("서울", {
      level: "sido",
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      expect(r.level).toBe("sido");
    }
  });

  it("year filter narrows results", async () => {
    const all = await find("강원", { level: "sido", baseUrl, fetch: localFetch });
    const y2025 = await find("강원", {
      level: "sido",
      year: [2025],
      baseUrl,
      fetch: localFetch,
    });
    expect(y2025.length).toBeGreaterThan(0);
    expect(y2025.length).toBeLessThan(all.length);
    for (const r of y2025) {
      expect(r.version_key.startsWith("2025")).toBe(true);
    }
  });

  it("exact match requires single token and matches name only", async () => {
    const rows = await find("종로구", {
      exact: true,
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      expect(r.name).toBe("종로구");
    }
  });

  it("exact + multi-token throws", async () => {
    await expect(
      find("서울 종로", { exact: true, baseUrl, fetch: localFetch }),
    ).rejects.toThrow(/exact=true requires a single-token/);
  });

  it("empty name throws", async () => {
    await expect(find("   ", { baseUrl, fetch: localFetch })).rejects.toThrow(
      /name cannot be empty/,
    );
  });

  it("too many tokens throws", async () => {
    await expect(
      find("a b c d", { baseUrl, fetch: localFetch }),
    ).rejects.toThrow(/1-3 whitespace-separated tokens/);
  });
});

describe("find() — 코드 검색", () => {
  it("digits-only query auto-detects as code search", async () => {
    const rows = await find("1111051500", { baseUrl, fetch: localFetch });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      expect(r.level).toBe("emd");
      expect(r.code).toBe("1111051500");
    }
    expect(new Set(rows.map((r) => r.name))).toContain("청운효자동");
  });

  it("prefix matches the sgg itself and its descendant emds", async () => {
    const rows = await find("11110", { baseUrl, fetch: localFetch });
    const levels = new Set(rows.map((r) => r.level));
    expect(levels.has("sgg")).toBe(true);
    expect(levels.has("emd")).toBe(true);
    for (const r of rows) {
      const hit =
        r.code?.startsWith("11110") ||
        r.code7?.startsWith("11110") ||
        r.code8?.startsWith("11110");
      expect(hit).toBe(true);
    }
  });

  it("exact means full-width match, not prefix", async () => {
    const rows = await find("11110", {
      exact: true,
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      expect(r.level).toBe("sgg");
      expect(r.code).toBe("11110");
    }
  });

  it("matches 통계청 code7", async () => {
    const rows = await find("1101053", {
      exact: true,
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      expect(r.code7).toBe("1101053");
    }
  });

  it("matches 통계청 code8", async () => {
    const rows = await find("11010530", {
      exact: true,
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      expect(r.code8).toBe("11010530");
    }
  });

  it("level narrows a code search", async () => {
    const rows = await find("11110", {
      level: "emd",
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) expect(r.level).toBe("emd");
  });

  it("year narrows a code search", async () => {
    const rows = await find("11110", {
      year: [2025],
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) expect(r.version_key.slice(0, 4)).toBe("2025");
  });

  it('by:"name" forces name search on a digits query', async () => {
    const rows = await find("11110", {
      by: "name",
      baseUrl,
      fetch: localFetch,
    });
    expect(rows.length).toBe(0);
  });

  it('by:"code" rejects a non-digits query', async () => {
    await expect(
      find("종로구", { by: "code", baseUrl, fetch: localFetch }),
    ).rejects.toThrow(/digits-only/);
  });

  it("invalid by throws", async () => {
    await expect(
      // @ts-expect-error 잘못된 by 값 런타임 검증
      find("11110", { by: "codes", baseUrl, fetch: localFetch }),
    ).rejects.toThrow(/by must be/);
  });

  it("name search is unaffected", async () => {
    const rows = await find("종로구", { baseUrl, fetch: localFetch });
    expect(rows.length).toBeGreaterThan(0);
  });

  it("unmatched code returns empty", async () => {
    const rows = await find("99999999", { baseUrl, fetch: localFetch });
    expect(rows.length).toBe(0);
  });
});
