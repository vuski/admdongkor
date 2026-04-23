import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { get, getParquet } from "../src/get.js";

const repoRoot = path.resolve(__dirname, "..", "..");
const localBase = pathToFileURL(path.join(repoRoot, "parquet")).toString();

const localFetch: typeof fetch = async (input) => {
  const url = typeof input === "string" ? input : (input as URL).toString();
  if (!url.startsWith("file://")) {
    throw new Error(`local-only fetch; got ${url}`);
  }
  const filePath = new URL(url);
  const buf = await readFile(filePath);
  return new Response(buf, { status: 200, statusText: "OK" });
};

describe("get()", () => {
  it("reads sido light parquet (2026-04-01)", async () => {
    const fc = await get("20260401", "sido", {
      baseUrl: localBase,
      fetch: localFetch,
    });
    expect(fc.type).toBe("FeatureCollection");
    expect(fc.features.length).toBeGreaterThan(10);
    const first = fc.features[0]!;
    expect(first.type).toBe("Feature");
    expect(first.geometry.type === "Polygon" || first.geometry.type === "MultiPolygon").toBe(true);
    expect(typeof first.properties.sidonm).toBe("string");
  });

  it("reads sgg light parquet", async () => {
    const fc = await get("20260401", "sgg", {
      baseUrl: localBase,
      fetch: localFetch,
    });
    expect(fc.features.length).toBeGreaterThan(100);
  });

  it("reads emd light parquet", async () => {
    const fc = await get("20260401", "emd", {
      baseUrl: localBase,
      fetch: localFetch,
    });
    expect(fc.features.length).toBeGreaterThan(3000);
  });

  it("rejects unknown key", async () => {
    await expect(
      get("99999999", "sido", { baseUrl: localBase, fetch: localFetch }),
    ).rejects.toThrow(/unknown version key/);
  });

  it("getParquet returns raw ArrayBuffer (parquet magic bytes)", async () => {
    const buf = await getParquet("20260401", "sido", {
      baseUrl: localBase,
      fetch: localFetch,
    });
    expect(buf).toBeInstanceOf(ArrayBuffer);
    expect(buf.byteLength).toBeGreaterThan(1000);
    // parquet 파일은 "PAR1" magic 으로 시작·끝
    const head = new Uint8Array(buf, 0, 4);
    const tail = new Uint8Array(buf, buf.byteLength - 4, 4);
    const magic = new TextDecoder().decode(head);
    expect(magic).toBe("PAR1");
    expect(new TextDecoder().decode(tail)).toBe("PAR1");
  });

  it("rejects invalid level", async () => {
    await expect(
      // @ts-expect-error
      get("20260401", "bogus", { baseUrl: localBase, fetch: localFetch }),
    ).rejects.toThrow(/level must be one of/);
  });
});
