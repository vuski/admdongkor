import { readFile } from "node:fs/promises";
import path from "node:path";

import { beforeEach, describe, expect, it } from "vitest";

import { findOffices } from "../src/offices.js";
import { clearIndexCache } from "../src/find.js";

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

describe("findOffices()", () => {
  it("resolves a full 10-digit code to one office", async () => {
    const rows = await findOffices("2920083000", { baseUrl, fetch: localFetch });
    expect(rows).toHaveLength(1);
    expect(rows[0]!.name).toBe("임곡출장소");
    expect(rows[0]!.sidonm).toBe("광주광역시");
    expect(rows[0]!.abolished).toBe("19981015");
  });

  it("prefix-matches by code", async () => {
    const rows = await findOffices("28", { baseUrl, fetch: localFetch });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) expect(r.code.startsWith("28")).toBe(true);
  });

  it("exact means full-code match, not prefix", async () => {
    expect(
      await findOffices("28", { exact: true, baseUrl, fetch: localFetch }),
    ).toHaveLength(0);
    expect(
      await findOffices("2811400000", {
        exact: true,
        baseUrl,
        fetch: localFetch,
      }),
    ).toHaveLength(1);
  });

  it("searches by name", async () => {
    const rows = await findOffices("영종", { baseUrl, fetch: localFetch });
    expect(rows.length).toBeGreaterThan(0);
    for (const r of rows) {
      const hay = `${r.sidonm ?? ""}${r.sggnm ?? ""}${r.name}`;
      expect(hay.includes("영종")).toBe(true);
    }
  });

  it("distinguishes alive from abolished", async () => {
    const rows = await findOffices("영종", { baseUrl, fetch: localFetch });
    expect(rows.some((r) => r.abolished === null)).toBe(true);
    expect(rows.some((r) => r.abolished !== null)).toBe(true);
  });

  it("repairs the misaligned 시도명 rows", async () => {
    // 원본 xlsx 는 4110500000 의 시도명 칸에 '북부출장소' 를 넣어놨다.
    const rows = await findOffices("4110500000", {
      baseUrl,
      fetch: localFetch,
    });
    expect(rows).toHaveLength(1);
    expect(rows[0]!.sidonm).toBe("경기도");
    expect(rows[0]!.name).toBe("북부출장소");
  });

  it("assigns level by trailing 5 digits", async () => {
    const sgg = await findOffices("2811400000", { baseUrl, fetch: localFetch });
    expect(sgg[0]!.level).toBe("sgg");
    const emd = await findOffices("2920083000", { baseUrl, fetch: localFetch });
    expect(emd[0]!.level).toBe("emd");
  });

  it("returns empty for no match", async () => {
    expect(
      await findOffices("9999999999", { baseUrl, fetch: localFetch }),
    ).toHaveLength(0);
  });

  it("returns empty for a blank query", async () => {
    expect(await findOffices("   ", { baseUrl, fetch: localFetch })).toHaveLength(
      0,
    );
  });

  it("invalid by throws", async () => {
    await expect(
      // @ts-expect-error 잘못된 by 값 런타임 검증
      findOffices("28", { by: "codes", baseUrl, fetch: localFetch }),
    ).rejects.toThrow(/by must be/);
  });
});
