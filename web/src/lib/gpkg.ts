/**
 * GeoJSON → GeoPackage(.gpkg) 변환.
 *
 * GeoPackage 는 규약을 얹은 SQLite 파일이다. 브라우저에는 SQLite 가 없으므로
 * sql.js(WASM) 를 **이 모듈을 실제로 부를 때** dynamic import 한다 — 초기
 * 번들에 1.5MB WASM 이 들어가지 않도록. Next.js 가 별도 청크로 분리한다.
 *
 * 구현 범위: OGC GeoPackage 1.2 중 "vector features" 최소 프로파일.
 * gpkg_spatial_ref_sys / gpkg_contents / gpkg_geometry_columns 세 메타 테이블 +
 * 피처 테이블 1개. 공간 인덱스(R-tree)는 만들지 않는다 — QGIS·ArcGIS·GDAL 모두
 * 없어도 정상적으로 읽고, 있으면 파일이 커지고 쓰기가 느려진다.
 */

import type { Feature, FeatureCollection, Geometry, Position } from "geojson";

/** GeoPackage application_id 'GPKG' (0x47504B47), SQLite 헤더 offset 68. */
const APPLICATION_ID = 0x47504b47;
/** user_version 10200 = GeoPackage 1.2. SQLite 헤더 offset 60. */
const USER_VERSION = 10200;

type SqlValue = string | number | Uint8Array | null;

/** GeoJSON 좌표 → WKB. GPKG 는 WKB 를 그대로 품는다 (little-endian).
 *
 * 좌표가 수십만 개까지 가므로 배열 spread(`push(...buf)`) 를 쓰면 인자 개수
 * 한계로 스택이 터진다. 자라나는 Uint8Array 에 직접 쓴다.
 */
function wkbFromGeometry(g: Geometry): Uint8Array {
  let out = new Uint8Array(1024);
  let len = 0;
  const ensure = (extra: number) => {
    if (len + extra <= out.length) return;
    let cap = out.length * 2;
    while (cap < len + extra) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(out.subarray(0, len));
    out = next;
  };
  const dv = () => new DataView(out.buffer, out.byteOffset, out.byteLength);

  const u8 = (v: number) => {
    ensure(1);
    out[len++] = v & 0xff;
  };
  const u32 = (v: number) => {
    ensure(4);
    dv().setUint32(len, v, true);
    len += 4;
  };
  const f64 = (v: number) => {
    ensure(8);
    dv().setFloat64(len, v, true);
    len += 8;
  };
  const point = (p: Position) => {
    f64(p[0] ?? 0);
    f64(p[1] ?? 0);
  };
  const ring = (r: Position[]) => {
    u32(r.length);
    for (const p of r) point(p);
  };
  const polygon = (poly: Position[][]) => {
    u32(poly.length);
    for (const r of poly) ring(r);
  };

  // geometry type codes: 1 Point, 2 LineString, 3 Polygon,
  //                      4 MultiPoint, 5 MultiLineString, 6 MultiPolygon
  switch (g.type) {
    case "Point":
      u8(1);
      u32(1);
      point(g.coordinates);
      break;
    case "LineString":
      u8(1);
      u32(2);
      ring(g.coordinates);
      break;
    case "Polygon":
      u8(1);
      u32(3);
      polygon(g.coordinates);
      break;
    case "MultiPoint":
      u8(1);
      u32(4);
      u32(g.coordinates.length);
      for (const p of g.coordinates) {
        u8(1);
        u32(1);
        point(p);
      }
      break;
    case "MultiLineString":
      u8(1);
      u32(5);
      u32(g.coordinates.length);
      for (const l of g.coordinates) {
        u8(1);
        u32(2);
        ring(l);
      }
      break;
    case "MultiPolygon":
      u8(1);
      u32(6);
      u32(g.coordinates.length);
      for (const poly of g.coordinates) {
        u8(1);
        u32(3);
        polygon(poly);
      }
      break;
    default:
      throw new Error(`unsupported geometry type: ${(g as Geometry).type}`);
  }
  return out.slice(0, len);
}

interface Bbox {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
}

function extendBbox(b: Bbox, g: Geometry): void {
  const visit = (c: unknown): void => {
    if (!Array.isArray(c)) return;
    if (typeof c[0] === "number" && typeof c[1] === "number") {
      const x = c[0] as number;
      const y = c[1] as number;
      if (x < b.minX) b.minX = x;
      if (x > b.maxX) b.maxX = x;
      if (y < b.minY) b.minY = y;
      if (y > b.maxY) b.maxY = y;
      return;
    }
    for (const sub of c) visit(sub);
  };
  if ("coordinates" in g) visit(g.coordinates);
}

/**
 * GPKG geometry blob = 헤더 + WKB.
 * 헤더: magic 'GP'(2) + version(1) + flags(1) + srs_id(4) + envelope(4×f64).
 * flags bit0 = byteOrder(1=LE), bit1..3 = envelope 종류(1 = XY).
 */
function gpkgGeometryBlob(g: Geometry, srsId: number): Uint8Array {
  const wkb = wkbFromGeometry(g);
  const bb: Bbox = {
    minX: Infinity,
    minY: Infinity,
    maxX: -Infinity,
    maxY: -Infinity,
  };
  extendBbox(bb, g);
  const hasEnv = Number.isFinite(bb.minX);

  const headerLen = 8 + (hasEnv ? 32 : 0);
  const out = new Uint8Array(headerLen + wkb.length);
  const dv = new DataView(out.buffer);

  out[0] = 0x47; // 'G'
  out[1] = 0x50; // 'P'
  out[2] = 0x00; // version 0 (= GPKG 1.x binary)
  out[3] = (hasEnv ? 1 : 0) << 1; // envelope XY, byteOrder big-endian(0) for srs_id
  dv.setInt32(4, srsId, false); // srs_id 는 헤더 byteOrder(bit0=0 → BE) 를 따른다
  if (hasEnv) {
    // envelope 는 byteOrder bit0 을 따르므로 여기서도 big-endian
    dv.setFloat64(8, bb.minX, false);
    dv.setFloat64(16, bb.maxX, false);
    dv.setFloat64(24, bb.minY, false);
    dv.setFloat64(32, bb.maxY, false);
  }
  out.set(wkb, headerLen);
  return out;
}

function sqlIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** 속성값 → SQLite 컬럼 타입. 전 피처를 훑어 결정한다. */
function inferColumns(features: Feature[]): Map<string, "TEXT" | "REAL"> {
  const cols = new Map<string, "TEXT" | "REAL">();
  for (const f of features) {
    const props = f.properties ?? {};
    for (const [k, v] of Object.entries(props)) {
      if (v === null || v === undefined) {
        if (!cols.has(k)) cols.set(k, "TEXT");
        continue;
      }
      const t = typeof v === "number" ? "REAL" : "TEXT";
      const prev = cols.get(k);
      // 한 번이라도 문자열이 나오면 TEXT 로 고정 (숫자 문자열 코드 보존)
      cols.set(k, prev === "TEXT" || t === "TEXT" ? "TEXT" : "REAL");
    }
  }
  return cols;
}

export interface GpkgOptions {
  /** 피처 테이블 이름. 보통 'emd_20260701' 같은 형태. */
  tableName: string;
  /** EPSG 코드. admdongkor 의 GeoJSON 은 4326. */
  srsId?: number;
  /**
   * sql.js WASM 을 찾을 **디렉토리** (끝에 `/`). 기본은 사이트 루트.
   *
   * 파일명을 직접 넘기면 안 된다 — 번들러가 `browser` export 조건을 타면
   * sql.js 가 `sql-wasm-browser.wasm` 을, Node 에서는 `sql-wasm.wasm` 을
   * 요청한다. 어느 쪽이 올지 알 수 없으므로 sql.js 가 주는 파일명을 그대로
   * 쓰고 우리는 디렉토리만 지정한다. (public/ 에 두 파일 모두 둔다.)
   */
  wasmDir?: string;
  /**
   * 같은 파일에 함께 넣을 **추가 레이어**. GeoPackage 는 한 파일에 여러
   * 피처 테이블을 담을 수 있어, 경계 폴리곤과 지시선을 분리해 각각 다른
   * 스타일을 줄 수 있다 (QGIS 에서 일점쇄선 지정 등).
   */
  extraLayers?: { tableName: string; fc: FeatureCollection }[];
}

/**
 * FeatureCollection 을 GeoPackage 바이트로.
 *
 * sql.js 는 이 함수가 호출될 때 처음 로드된다 (dynamic import).
 */
export async function featureCollectionToGpkg(
  fc: FeatureCollection,
  options: GpkgOptions,
): Promise<Uint8Array> {
  const { tableName, srsId = 4326, wasmDir = "/" } = options;

  const initSqlJs = (await import("sql.js")).default;
  // sql.js 가 요청하는 파일명(`sql-wasm.wasm` 또는 `sql-wasm-browser.wasm`)을
  // 그대로 쓰고 디렉토리만 바꾼다.
  const SQL = await initSqlJs({ locateFile: (f: string) => `${wasmDir}${f}` });
  const db = new SQL.Database();

  try {
    db.run("PRAGMA application_id = " + APPLICATION_ID);
    db.run("PRAGMA user_version = " + USER_VERSION);

    // ── 필수 메타 테이블 ──
    db.run(`
      CREATE TABLE gpkg_spatial_ref_sys (
        srs_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL PRIMARY KEY,
        organization TEXT NOT NULL,
        organization_coordsys_id INTEGER NOT NULL,
        definition TEXT NOT NULL,
        description TEXT
      );
      CREATE TABLE gpkg_contents (
        table_name TEXT NOT NULL PRIMARY KEY,
        data_type TEXT NOT NULL,
        identifier TEXT UNIQUE,
        description TEXT DEFAULT '',
        last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE,
        srs_id INTEGER,
        CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
      );
      CREATE TABLE gpkg_geometry_columns (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        geometry_type_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL,
        z TINYINT NOT NULL,
        m TINYINT NOT NULL,
        CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
        CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
      );
    `);

    // 스펙이 요구하는 기본 3행 (-1 undefined cartesian, 0 undefined geographic)
    const wgs84 =
      'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,' +
      'AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],' +
      'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],' +
      'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],' +
      'AUTHORITY["EPSG","4326"]]';
    const srsRows: SqlValue[][] = [
      ["Undefined cartesian SRS", -1, "NONE", -1, "undefined", null],
      ["Undefined geographic SRS", 0, "NONE", 0, "undefined", null],
      ["WGS 84 geodetic", 4326, "EPSG", 4326, wgs84, null],
    ];
    if (srsId !== 4326 && srsId !== -1 && srsId !== 0) {
      srsRows.push([`EPSG:${srsId}`, srsId, "EPSG", srsId, "undefined", null]);
    }
    const srsStmt = db.prepare(
      "INSERT INTO gpkg_spatial_ref_sys VALUES (?,?,?,?,?,?)",
    );
    for (const r of srsRows) srsStmt.run(r);
    srsStmt.free();

    const writeLayer = (layerTable: string, layerFc: FeatureCollection): void => {
      // ── 피처 테이블 ──
      const features = layerFc.features ?? [];
      const cols = inferColumns(features);
      // GPKG 예약 컬럼과 충돌 방지
      cols.delete("fid");
      cols.delete("geom");

      const colDefs = [...cols.entries()]
        .map(([name, type]) => `${sqlIdent(name)} ${type}`)
        .join(", ");
      db.run(
        `CREATE TABLE ${sqlIdent(layerTable)} (` +
          "fid INTEGER PRIMARY KEY AUTOINCREMENT, geom BLOB" +
          (colDefs ? ", " + colDefs : "") +
          ")",
      );

      const colNames = [...cols.keys()];
      const placeholders = ["?", ...colNames.map(() => "?")].join(",");
      const insert = db.prepare(
        `INSERT INTO ${sqlIdent(layerTable)} (geom${
          colNames.length ? ", " + colNames.map(sqlIdent).join(", ") : ""
        }) VALUES (${placeholders})`,
      );

      const total: Bbox = {
        minX: Infinity,
        minY: Infinity,
        maxX: -Infinity,
        maxY: -Infinity,
      };
      let geomType = "GEOMETRY";
      const seenTypes = new Set<string>();

      db.run("BEGIN");
      for (const f of features) {
        if (!f.geometry) continue;
        seenTypes.add(f.geometry.type);
        extendBbox(total, f.geometry);
        const row: SqlValue[] = [gpkgGeometryBlob(f.geometry, srsId)];
        const props = f.properties ?? {};
        for (const name of colNames) {
          const v = (props as Record<string, unknown>)[name];
          if (v === null || v === undefined) row.push(null);
          else if (typeof v === "number") row.push(v);
          else if (typeof v === "string") row.push(v);
          else row.push(JSON.stringify(v));
        }
        insert.run(row);
      }
      db.run("COMMIT");
      insert.free();

      if (seenTypes.size === 1) geomType = [...seenTypes][0]!.toUpperCase();
      else if (
        seenTypes.size === 2 &&
        seenTypes.has("Polygon") &&
        seenTypes.has("MultiPolygon")
      ) {
        // 혼재 시 상위 타입으로. QGIS 가 둘 다 읽는다.
        geomType = "MULTIPOLYGON";
      }

      const hasExtent = Number.isFinite(total.minX);
      const contents = db.prepare(
        "INSERT INTO gpkg_contents " +
          "(table_name, data_type, identifier, description, min_x, min_y, max_x, max_y, srs_id) " +
          "VALUES (?,'features',?,'',?,?,?,?,?)",
      );
      contents.run([
        layerTable,
        layerTable,
        hasExtent ? total.minX : null,
        hasExtent ? total.minY : null,
        hasExtent ? total.maxX : null,
        hasExtent ? total.maxY : null,
        srsId,
      ]);
      contents.free();

      const gcols = db.prepare(
        "INSERT INTO gpkg_geometry_columns VALUES (?,'geom',?,?,0,0)",
      );
      gcols.run([layerTable, geomType, srsId]);
      gcols.free();
    };

    writeLayer(tableName, fc);
    for (const ex of options.extraLayers ?? []) writeLayer(ex.tableName, ex.fc);

    const bytes = db.export();
    // sql.js 의 PRAGMA application_id 가 헤더에 반영되지 않는 빌드가 있어
    // 바이트를 직접 확인·보정한다 (GDAL 이 이 값으로 GPKG 를 판별).
    const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    if (dv.getUint32(68, false) !== APPLICATION_ID) {
      dv.setUint32(68, APPLICATION_ID, false);
    }
    if (dv.getUint32(60, false) !== USER_VERSION) {
      dv.setUint32(60, USER_VERSION, false);
    }
    return bytes;
  } finally {
    db.close();
  }
}
