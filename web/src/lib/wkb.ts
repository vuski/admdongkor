/** 최소 WKB 디코더 — Polygon / MultiPolygon 만 지원.
 *  이 프로젝트 timeline/*.bin 은 WKB concat 이며 이 두 타입만 등장.
 *  shapely 가 내보내는 WKB 는 little-endian 이지만 혹시 모르니 byteOrder 존중. */

export type Ring = [number, number][];
export type PolygonCoords = Ring[];          // [exterior, ...interiors]
export type MultiPolygonCoords = PolygonCoords[];

export interface DecodedPolygon {
  type: "Polygon";
  coordinates: PolygonCoords;
}
export interface DecodedMultiPolygon {
  type: "MultiPolygon";
  coordinates: MultiPolygonCoords;
}
export type DecodedGeometry = DecodedPolygon | DecodedMultiPolygon;

class Cursor {
  view: DataView;
  pos: number;
  littleEndian: boolean = true;

  constructor(buf: ArrayBuffer, pos = 0) {
    this.view = new DataView(buf);
    this.pos = pos;
  }

  readByte(): number {
    const v = this.view.getUint8(this.pos);
    this.pos += 1;
    return v;
  }
  readUint32(): number {
    const v = this.view.getUint32(this.pos, this.littleEndian);
    this.pos += 4;
    return v;
  }
  readFloat64(): number {
    const v = this.view.getFloat64(this.pos, this.littleEndian);
    this.pos += 8;
    return v;
  }
  readRing(): Ring {
    const n = this.readUint32();
    const ring: Ring = new Array(n);
    for (let i = 0; i < n; i++) {
      const x = this.readFloat64();
      const y = this.readFloat64();
      ring[i] = [x, y];
    }
    return ring;
  }
  readPolygonBody(): PolygonCoords {
    const nRings = this.readUint32();
    const rings: PolygonCoords = new Array(nRings);
    for (let i = 0; i < nRings; i++) rings[i] = this.readRing();
    return rings;
  }
}

/** buf 전체가 하나의 geometry 를 담고 있다고 가정. */
export function decodeWKB(buf: ArrayBuffer, offset = 0): DecodedGeometry {
  const cur = new Cursor(buf, offset);
  const byteOrder = cur.readByte();
  cur.littleEndian = byteOrder === 1;
  const type = cur.readUint32();
  // bit 31 = SRID 플래그. shapely 기본 wkb 는 SRID 없음. 있으면 건너뜀.
  if ((type & 0x20000000) !== 0) cur.readUint32();
  const baseType = type & 0xff;

  if (baseType === 3) {
    return { type: "Polygon", coordinates: cur.readPolygonBody() };
  }
  if (baseType === 6) {
    const n = cur.readUint32();
    const coords: MultiPolygonCoords = new Array(n);
    for (let i = 0; i < n; i++) {
      // 각 멤버도 자기 header 를 가짐: byteOrder + type(=3)
      const subOrder = cur.readByte();
      cur.littleEndian = subOrder === 1;
      const subType = cur.readUint32();
      if ((subType & 0xff) !== 3) {
        throw new Error(`MultiPolygon member type=${subType & 0xff}, expected 3`);
      }
      if ((subType & 0x20000000) !== 0) cur.readUint32();
      coords[i] = cur.readPolygonBody();
    }
    return { type: "MultiPolygon", coordinates: coords };
  }
  throw new Error(`unsupported WKB type ${baseType}`);
}

/** blob 바이트 안의 여러 WKB 를 순서대로 디코드.
 *  각 feature 의 (offset, length) 배열을 받아 동일 길이 배열 반환. */
export function decodeWKBBatch(
  blob: ArrayBuffer,
  items: Array<{ offset: number; length: number }>,
  /** blob 이 전체 파일 절대 offset 이 아니라 partial range 라면 base 를 빼서 상대 offset 사용 */
  baseOffset = 0,
): DecodedGeometry[] {
  const out: DecodedGeometry[] = new Array(items.length);
  for (let i = 0; i < items.length; i++) {
    out[i] = decodeWKB(blob, items[i].offset - baseOffset);
  }
  return out;
}

/** bbox 계산 (lon/lat). min/max lon, min/max lat. */
export function geomBBox(g: DecodedGeometry): [number, number, number, number] {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const polys: PolygonCoords[] = g.type === "Polygon" ? [g.coordinates] : g.coordinates;
  for (const poly of polys) {
    const ring = poly[0];
    if (!ring) continue;
    for (const [x, y] of ring) {
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    }
  }
  return [minX, minY, maxX, maxY];
}

/** 여러 geometry 의 합친 bbox. */
export function unionBBox(
  geoms: DecodedGeometry[],
): [number, number, number, number] {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const g of geoms) {
    const [a, b, c, d] = geomBBox(g);
    if (a < minX) minX = a;
    if (b < minY) minY = b;
    if (c > maxX) maxX = c;
    if (d > maxY) maxY = d;
  }
  return [minX, minY, maxX, maxY];
}
