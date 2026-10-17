// 패키지 배포 이후 추가된 시점을 manifest 로 받아들이는지 (네트워크 없음).
//
// 배경: get / matchAdm / compare 가 패키지 안의 정적 VERSIONS 로만 키를 검사해서,
// dist/data/ 에 새 시점을 push 해도 npm 재배포 전까지 `unknown version key` 로
// 거부했다 (2026-10, 20261001 에서 발견). 지금은 manifest 의 versions 도 함께 본다.

import { beforeEach, describe, expect, it } from "vitest";

import { clearManifestCache } from "../src/changelog.js";
import { compare } from "../src/compare.js";
import { get } from "../src/get.js";
import { matchAdm } from "../src/match.js";
import { VERSIONS, versionsAsync } from "../src/versions.js";

const NEW_KEY = "29991231"; // 스냅샷에는 없고 manifest 에만 있는 가상의 새 시점
const INDEX = "https://example.test/dist/data";
const PARQUET = "https://example.test/parquet";

/** manifest.json 만 돌려주고 나머지는 404. 요청 URL 을 기록한다. */
function fakeFetch(manifest: unknown | null) {
  const calls: string[] = [];
  const fn: typeof fetch = async (input) => {
    const url = typeof input === "string" ? input : (input as URL).toString();
    calls.push(url);
    if (manifest !== null && url === `${INDEX}/manifest.json`) {
      return new Response(JSON.stringify(manifest), { status: 200 });
    }
    return new Response("", { status: 404, statusText: "Not Found" });
  };
  return { fn, calls };
}

const withNewKey = { data_version: "2999.12.31", versions: [...VERSIONS, NEW_KEY] };

beforeEach(() => clearManifestCache());

describe("versionsAsync()", () => {
  it("includes a key that only the manifest knows", async () => {
    const { fn } = fakeFetch(withNewKey);
    const all = await versionsAsync(undefined, { baseUrl: INDEX, fetch: fn });
    expect(all.at(-1)).toBe(NEW_KEY);
    expect(await versionsAsync(2999, { baseUrl: INDEX, fetch: fn })).toEqual([NEW_KEY]);
  });

  it("keeps the snapshot when the manifest is stale", async () => {
    const { fn } = fakeFetch({ versions: VERSIONS.slice(0, -1) });
    const all = await versionsAsync(undefined, { baseUrl: INDEX, fetch: fn });
    expect(all).toEqual([...VERSIONS]);
  });
});

describe("get() accepts a key known only to the manifest", () => {
  it("passes validation and goes on to download", async () => {
    const { fn, calls } = fakeFetch(withNewKey);
    // 키 검사를 통과하면 parquet 다운로드 단계(404)에서 실패한다
    await expect(
      get(NEW_KEY, "sido", { baseUrl: PARQUET, indexBaseUrl: INDEX, fetch: fn }),
    ).rejects.toThrow(/failed to fetch .*sido_29991231_light\.parquet/);
    expect(calls).toContain(`${INDEX}/manifest.json`);
  });

  it("does not touch the manifest for keys in the snapshot", async () => {
    const { fn, calls } = fakeFetch(withNewKey);
    await expect(
      get(VERSIONS[0], "sido", { baseUrl: PARQUET, indexBaseUrl: INDEX, fetch: fn }),
    ).rejects.toThrow(/failed to fetch/);
    expect(calls).not.toContain(`${INDEX}/manifest.json`);
  });

  it("still rejects a key nobody knows", async () => {
    const { fn } = fakeFetch(withNewKey);
    await expect(
      get("19000101", "sido", { baseUrl: PARQUET, indexBaseUrl: INDEX, fetch: fn }),
    ).rejects.toThrow(/unknown version key/);
  });

  it("rejects the new key when the manifest is unreachable", async () => {
    const { fn } = fakeFetch(null);
    await expect(
      get(NEW_KEY, "sido", { baseUrl: PARQUET, indexBaseUrl: INDEX, fetch: fn }),
    ).rejects.toThrow(/unknown version key/);
  });
});

describe("matchAdm() / compare() accept a key known only to the manifest", () => {
  it("matchAdm passes base/target validation", async () => {
    const { fn } = fakeFetch(withNewKey);
    // 검증 통과 후 인덱스 parquet 다운로드(404)에서 실패
    await expect(
      matchAdm({ base: NEW_KEY, region: "11", target: NEW_KEY, baseUrl: INDEX, fetch: fn }),
    ).rejects.toThrow(/failed to fetch .*timeline_v3_emd\.parquet/);
  });

  it("matchAdm still rejects an unknown base", async () => {
    const { fn } = fakeFetch(withNewKey);
    await expect(
      matchAdm({ base: "19000101", region: "11", target: NEW_KEY, baseUrl: INDEX, fetch: fn }),
    ).rejects.toThrow(/unknown base version key/);
  });

  it("compare passes validation", async () => {
    const { fn } = fakeFetch(withNewKey);
    await expect(
      compare([VERSIONS[0], NEW_KEY], { baseUrl: INDEX, fetch: fn }),
    ).rejects.toThrow(/failed to fetch .*timeline_v3_emd\.parquet/);
  });
});
