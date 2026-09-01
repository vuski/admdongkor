/**
 * "단순화(많이)" — mapshaper 2.7% 로 시군구를 크게 단순화하고,
 * 시도는 그 결과를 dissolve 해서 만든다.
 *
 * ## 왜 별도 경로인가
 * 기본 제공하는 light parquet 은 **읍면동 기준 18.7%** 로 단순화한 뒤
 * sgg/sido 를 dissolve 한 것이다. 여기서 더 줄이려면 읍면동 경계를 버리고
 * 시군구부터 다시 단순화해야 하므로, **읍면동을 선택하지 않았을 때만** 쓸 수 있다.
 *
 * ## mapshaper 를 브라우저에서
 * ⚠️ npm 패키지의 **기본 진입점은 Node 용**이라 번들러에 넣으면
 * `child_process`/`fs`/geopackage/geotiff 등 29개 모듈을 못 찾아 빌드가 깨진다.
 * 브라우저용은 별도 파일 `mapshaper/www/mapshaper.js` 이고, 이건 ESM 이 아니라
 * `window.mapshaper` 를 세팅하는 **IIFE** 라 import 할 수 없다.
 *
 * → `public/mapshaper.js` 로 복사해 두고 **script 태그로 1회 로드**한다.
 *   2.8MB 라 이 옵션을 실제로 쓸 때만 받는다 (sql.js WASM 과 같은 패턴).
 *
 * 기본 알고리즘은 **weighted Visvalingam (계수 0.7, 3D)** 이다. shapely 의
 * `simplify`(RDP) 와 결과가 다르므로, 파이썬 쪽에서 같은 결과를 원하면
 * 반드시 mapshaper 를 써야 한다.
 *
 * ## 독도는 단순화 후 원본에서 되붙인다
 * 2.7% 에서는 part 가 933 → 273 으로 줄면서 독도 두 폴리곤이 통째로 사라진다.
 * `keep-shapes` 는 **feature(행)** 만 보존할 뿐 MultiPolygon 안의 작은 part 는
 * 지우기 때문이다 (18.7% 에서 겪은 것과 같은 함정, 더 심한 버전).
 *
 * → 단순화 **후**에 원본 폴리곤을 그대로(단순화 없이) **울릉군 feature 안에**
 *   되붙인다. 별도 feature 로 만들면 시군구 목록에 없는 행이 생기므로 안 된다.
 */

import type { Feature, FeatureCollection, Position } from "geojson";

/** www 빌드가 window 에 붙이는 API 중 우리가 쓰는 부분. */
interface MapshaperApi {
  applyCommands(
    commands: string,
    input: Record<string, string | Uint8Array>,
  ): Promise<Record<string, Uint8Array | string>>;
}

declare global {
  interface Window {
    mapshaper?: MapshaperApi;
  }
}

/**
 * mapshaper 브라우저 번들은 **두 파일**이고 순서가 중요하다.
 *
 *   modules.js    → `window.modules` 에 mproj·buffer·iconv-lite 등을 올린다
 *   mapshaper.js  → 로드 시 `window.modules[name]` 으로 그걸 꺼내 쓴다
 *
 * modules.js 를 빼면 mapshaper.js 가 `require$1('mproj')` 에서 undefined 를
 * 받아 조용히 실패한다 (에러 없이 window.mapshaper 가 안 붙거나, 붙어도
 * 좌표계 처리에서 터진다). next basePath 를 쓰면 이 경로도 맞춰야 한다.
 */
const MAPSHAPER_SRCS = ["/modules.js", "/mapshaper.js"];

let loading: Promise<MapshaperApi> | null = null;

/** script 하나를 로드한다. */
function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const el = document.createElement("script");
    el.src = src;
    el.async = false; // 순서 보장 (modules.js 가 먼저여야 한다)
    el.onload = () => resolve();
    el.onerror = () => reject(new Error(`스크립트를 불러오지 못했습니다: ${src}`));
    document.head.appendChild(el);
  });
}

/** 브라우저 mapshaper 를 1회만 로드한다 (중복 호출은 같은 Promise 를 공유). */
function loadMapshaper(): Promise<MapshaperApi> {
  if (window.mapshaper) return Promise.resolve(window.mapshaper);
  if (loading) return loading;
  loading = (async () => {
    for (const src of MAPSHAPER_SRCS) await loadScript(src);
    const api = window.mapshaper;
    if (!api) {
      loading = null; // 재시도 가능하게
      throw new Error(
        "mapshaper 로드 실패: window.mapshaper 가 설정되지 않았습니다.",
      );
    }
    return api;
  })();
  return loading;
}

/** applyCommands 결과는 브라우저에서 문자열/Uint8Array 로 온다. 문자열로 통일. */
function asText(v: Uint8Array | string | undefined): string {
  if (typeof v === "string") return v;
  if (v) return new TextDecoder().decode(v);
  throw new Error("mapshaper 출력이 비어 있습니다.");
}

/** mapshaper 단순화 비율. 사용자가 지정한 값. */
const SIMPLIFY_PERCENT = "2.7%";

/**
 * 단순화에서 사라지면 원본으로 되살릴 미세 섬. EPSG:4326.
 * 독도 하나지만, 다른 섬이 문제되면 여기 추가하면 된다.
 */
const KEEP_PARTS_BBOX: [number, number, number, number][] = [
  [131.855, 37.23, 131.88, 37.252], // 독도 (동도·서도)
];

/** ring 의 모든 점이 bbox 안에 있는가. */
function ringInside(
  ring: Position[],
  [minX, minY, maxX, maxY]: [number, number, number, number],
): boolean {
  for (const p of ring) {
    const x = p[0] as number;
    const y = p[1] as number;
    if (x < minX || x > maxX || y < minY || y > maxY) return false;
  }
  return true;
}

/** feature 의 polygon part 목록 (Polygon/MultiPolygon 공통). */
function partsOf(f: Feature): Position[][][] {
  const g = f.geometry;
  if (!g) return [];
  if (g.type === "Polygon") return [g.coordinates as Position[][]];
  if (g.type === "MultiPolygon") return g.coordinates as Position[][][];
  return [];
}

/** bbox 안에 완전히 들어가는 part 를 소속 feature 키와 함께 뽑는다. */
function collectTinyParts(
  fc: FeatureCollection,
  keyOf: (f: Feature) => string,
): Map<string, Position[][][]> {
  const out = new Map<string, Position[][][]>();
  for (const f of fc.features) {
    for (const poly of partsOf(f)) {
      const outer = poly[0];
      if (!outer || outer.length === 0) continue;
      if (!KEEP_PARTS_BBOX.some((b) => ringInside(outer, b))) continue;
      const k = keyOf(f);
      const list = out.get(k) ?? [];
      list.push(poly);
      out.set(k, list);
    }
  }
  return out;
}

/** 해당 part 가 이미 살아있는지. */
function hasTinyParts(fc: FeatureCollection): boolean {
  for (const f of fc.features) {
    for (const poly of partsOf(f)) {
      const outer = poly[0];
      if (!outer || outer.length === 0) continue;
      if (KEEP_PARTS_BBOX.some((b) => ringInside(outer, b))) return true;
    }
  }
  return false;
}

/** 같은 키의 feature 에 part 들을 되붙인다 (in-place). */
function restoreInto(
  fc: FeatureCollection,
  saved: Map<string, Position[][][]>,
  keyOf: (f: Feature) => string,
): number {
  let n = 0;
  for (const f of fc.features) {
    const polys = saved.get(keyOf(f));
    if (!polys || polys.length === 0) continue;
    const g = f.geometry;
    if (!g) continue;
    if (g.type === "Polygon") {
      f.geometry = {
        type: "MultiPolygon",
        coordinates: [g.coordinates as Position[][], ...polys],
      };
    } else if (g.type === "MultiPolygon") {
      (g.coordinates as Position[][][]).push(...polys);
    } else {
      continue;
    }
    n += polys.length;
  }
  return n;
}

/**
 * 시군구 FeatureCollection 을 2.7% 로 단순화하고 독도를 복원한다.
 * 입력·출력 모두 **EPSG:4326**.
 */
export async function supersimplifySgg(
  fc: FeatureCollection,
): Promise<FeatureCollection> {
  // 소속 판정 키 — sggcd 가 있으면 그것, 없으면 이름 조합 (옛 시점 대비).
  const keyOf = (f: Feature): string => {
    const p = (f.properties ?? {}) as Record<string, unknown>;
    return String(p.sggcd ?? `${p.sidonm ?? ""}|${p.sggnm ?? ""}`);
  };

  // 단순화 전에 미세 섬을 따로 보관해 둔다.
  const saved = collectTinyParts(fc, keyOf);

  const mapshaper = await loadMapshaper();
  const out = await mapshaper.applyCommands(
    `-i in.json -simplify ${SIMPLIFY_PERCENT} keep-shapes -o out.json`,
    { "in.json": JSON.stringify(fc) },
  );
  const simplified = JSON.parse(asText(out["out.json"])) as FeatureCollection;

  // 사라졌을 때만 되붙인다 (멱등 — 살아있으면 중복 추가 안 함).
  if (!hasTinyParts(simplified)) restoreInto(simplified, saved, keyOf);
  return simplified;
}

/**
 * 단순화된 시군구를 dissolve 해서 시도를 만든다.
 *
 * 시도를 따로 단순화하지 않고 sgg 를 병합하는 이유: 각자 단순화하면 시도
 * 경계와 시군구 경계가 어긋나 두 레이어를 겹쳤을 때 틈이 벌어진다.
 */
export async function dissolveToSido(
  sgg: FeatureCollection,
): Promise<FeatureCollection> {
  const mapshaper = await loadMapshaper();
  const out = await mapshaper.applyCommands(
    "-i in.json -dissolve sidocd copy-fields=sidonm -o out.json",
    { "in.json": JSON.stringify(sgg) },
  );
  return JSON.parse(asText(out["out.json"])) as FeatureCollection;
}
