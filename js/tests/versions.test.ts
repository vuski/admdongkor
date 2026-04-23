import { describe, expect, it } from "vitest";

import { VERSIONS, versions } from "../src/versions.js";

describe("versions()", () => {
  it("returns all versions when called with no args", () => {
    const all = versions();
    expect(all.length).toBe(VERSIONS.length);
    expect(all.length).toBe(62);
    expect(all[0]).toBe("19751231");
    expect(all.at(-1)).toBe("20260401");
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
