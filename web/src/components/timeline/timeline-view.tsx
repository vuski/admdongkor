"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2, X } from "lucide-react";
import { matchAdm } from "admdongkor";
import polylabel from "@mapbox/polylabel";
import { useAppStore } from "@/stores/app-store";
import {
  useTimelineStore,
  type TimelineViewport,
} from "@/stores/timeline-store";
import {
  fetchBinRange,
  fetchMeta,
  fetchTimelineSlice,
  type TimelineSlice,
} from "@/lib/timeline-client";
import { loadVersionNames, type NameRow } from "@/lib/name-index";
import { shortSido } from "@/lib/sido-short";
import { decodeWKB } from "@/lib/wkb";
import { fmtVersionLabel } from "@/lib/timeline-years";
import type { DecodedGeometry } from "@/lib/wkb";

/** 한 버전의 이름 맵 (sido/sgg/emd). meta 에서 이름을 뺐으므로 라벨·색 할당용으로 로드. */
interface NameMaps {
  sido: Map<string, NameRow>;
  sgg: Map<string, NameRow>;
  emd: Map<string, NameRow>;
}

const CELL_HEIGHT = 360;

/** 파스텔 색 10종 + 그에 대응하는 hue 값 (HSL). 10 을 넘으면 hue hash 기반으로
 *  추가 생성하되 이미 쓴 hue 와 겹치지 않게 재뽑기 한다. */
const PASTEL_PALETTE = [
  "#c6e4f5", // 하늘
  "#f5d6c6", // 복숭아
  "#d6f0c6", // 연두
  "#f0c6e4", // 연분홍
  "#e4d6f0", // 연보라
  "#f5e6c6", // 연노랑
  "#c6f0e4", // 민트
  "#f5c6c6", // 연빨강
  "#c6c6f0", // 라일락
  "#d6e6f0", // 연청
];
/** 위 색들의 hue (0~360). 충돌 회피 판정 전용. */
const PASTEL_HUES = [205, 20, 95, 320, 270, 45, 160, 0, 240, 210];
/** 새 hue 와 기존 hue 의 최소 각도 거리 (도). 이보다 가까우면 충돌로 본다. */
const HUE_MIN_GAP = 25;

function hashKey(s: string): number {
  // djb2
  let h = 5381;
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) + h + s.charCodeAt(i)) | 0;
  }
  return h >>> 0;
}

function hueDist(a: number, b: number): number {
  const d = Math.abs(a - b) % 360;
  return d > 180 ? 360 - d : d;
}

function hslToHex(h: number, s: number, l: number): string {
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const hp = h / 60;
  const x = c * (1 - Math.abs((hp % 2) - 1));
  let r = 0,
    g = 0,
    b = 0;
  if (hp < 1) {
    r = c;
    g = x;
  } else if (hp < 2) {
    r = x;
    g = c;
  } else if (hp < 3) {
    g = c;
    b = x;
  } else if (hp < 4) {
    g = x;
    b = c;
  } else if (hp < 5) {
    r = x;
    b = c;
  } else {
    r = c;
    b = x;
  }
  const m = l - c / 2;
  const toHex = (v: number) =>
    Math.round((v + m) * 255)
      .toString(16)
      .padStart(2, "0");
  return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
}

/** key hash 로 시작 hue 를 정하고, 이미 쓴 hue 와 HUE_MIN_GAP 이상 떨어지도록
 *  golden-angle 보폭으로 회전시킨다. 결정적(같은 key + 같은 usedHues → 같은 색). */
function pastelFromKeyUnique(key: string, usedHues: number[]): string {
  const h0 = hashKey(key) % 360;
  const step = 137.5; // golden angle
  for (let i = 0; i < 360; i++) {
    const hue = (h0 + step * i) % 360;
    let ok = true;
    for (const u of usedHues) {
      if (hueDist(hue, u) < HUE_MIN_GAP) {
        ok = false;
        break;
      }
    }
    if (ok) {
      usedHues.push(hue);
      // 파스텔: saturation 40%, lightness 85%
      return hslToHex(hue, 0.4, 0.85);
    }
  }
  // 360 색상 다 써서 빈 hue 가 없으면 gap 조건 무시하고 hash hue 그대로.
  usedHues.push(h0);
  return hslToHex(h0, 0.4, 0.85);
}

export function TimelineView() {
  const query = useTimelineStore((s) => s.query);
  const versions = useTimelineStore((s) => s.selectedVersions);
  const viewport = useTimelineStore((s) => s.viewport);
  const setViewport = useTimelineStore((s) => s.setViewport);
  const updateViewport = useTimelineStore((s) => s.updateViewport);
  const showLabels = useAppStore((s) => s.showLabels);
  const setTimelineViewActive = useAppStore((s) => s.setTimelineViewActive);
  const versionList = useAppStore((s) => s.versionList);
  const baseGeometries = useTimelineStore((s) => s.baseGeometries);
  const setBaseGeometries = useTimelineStore((s) => s.setBaseGeometries);

  // version -> slice.
  const [slices, setSlices] = useState<Map<string, TimelineSlice>>(new Map());
  const [loading, setLoading] = useState(false);
  const [errors, setErrors] = useState<Map<string, string>>(new Map());
  // version -> NameMaps (라벨/색 할당용).
  const [namesByVersion, setNamesByVersion] = useState<Map<string, NameMaps>>(
    new Map(),
  );

  // 쿼리/버전 변경 시 로드: 1) matchAdm 으로 각 target 의 매칭 코드 구함, 2) 코드들로 slice fetch.
  useEffect(() => {
    if (!query || versions.length === 0) return;
    let cancelled = false;
    setLoading(true);
    setSlices(new Map());
    setErrors(new Map());
    setNamesByVersion(new Map());

    // versions 의 이름 맵 로드 (병렬). _index_v3.parquet 는 한 번만 다운로드되고 캐시됨.
    (async () => {
      const entries = await Promise.all(
        versions.map(async (v) => [v, await loadVersionNames(v)] as const),
      );
      if (cancelled) return;
      setNamesByVersion(new Map(entries));
    })().catch(() => {
      // 이름 로드 실패해도 geometry 는 그릴 수 있음 — 조용히 무시.
    });

    const validVersions = new Set(versionList);
    // matchAdm 이 지원하는 target 만 (1990+). 그 외는 별도로 "미지원" 마킹.
    const supportedTargets = versions.filter((v) => validVersions.has(v));
    const unsupportedTargets = versions.filter((v) => !validVersions.has(v));

    const results = new Map<string, TimelineSlice>();
    const errs = new Map<string, string>();

    // 미지원 버전은 즉시 exists=false 로 마킹.
    for (const v of unsupportedTargets) {
      results.set(v, {
        version: v,
        targetLevel: "sgg",
        exists: false,
        groups: [],
      });
    }
    if (unsupportedTargets.length > 0) setSlices(new Map(results));

    // base 자신도 matchAdm 의 target 으로 넣으면 weight=1 의 자기 자신 매칭이 나와 동일 로직으로 처리.
    const runMatch = async () => {
      if (
        supportedTargets.length === 0 ||
        !validVersions.has(query.baseVersion)
      ) {
        if (!validVersions.has(query.baseVersion)) {
          errs.set(query.baseVersion, "base version not in match index");
          setErrors(new Map(errs));
        }
        return;
      }
      try {
        // 복수 selection: 각 region 별 matchAdm 을 병렬 호출하고 결과를 버전·code 키로 병합.
        //   같은 (version, sggcd) 가 여러 selection 에서 매칭되면 weight 는 최댓값 사용.
        const matchResults = await Promise.all(
          query.selections.map((sel) =>
            matchAdm({
              base: query.baseVersion,
              region: sel.code,
              target: supportedTargets,
            }),
          ),
        );

        // sgg 병합 — version -> code -> weight (max).
        const sggWeightByVersion = new Map<string, Map<string, number>>();
        for (const mr of matchResults) {
          const rows = await mr.sgg();
          for (const r of rows) {
            let m = sggWeightByVersion.get(r.version_key);
            if (!m) {
              m = new Map();
              sggWeightByVersion.set(r.version_key, m);
            }
            const prev = m.get(r.sggcd);
            if (prev === undefined || r.weight > prev) m.set(r.sggcd, r.weight);
          }
        }

        // emd 병합 — version -> emdcd -> weight (max).
        const emdWeightByVersion = new Map<string, Map<string, number>>();
        for (const mr of matchResults) {
          for (const r of mr.emd) {
            let m = emdWeightByVersion.get(r.version_key);
            if (!m) {
              m = new Map();
              emdWeightByVersion.set(r.version_key, m);
            }
            const prev = m.get(r.emdcd);
            if (prev === undefined || r.weight > prev) m.set(r.emdcd, r.weight);
          }
        }

        // version 별 row 배열 (weight 내림차순).
        const rowsByVersion = new Map<
          string,
          Array<{ code: string; weight: number }>
        >();
        for (const [v, m] of sggWeightByVersion) {
          const arr = [...m.entries()].map(([code, weight]) => ({
            code,
            weight,
          }));
          arr.sort((a, b) => b.weight - a.weight);
          rowsByVersion.set(v, arr);
        }

        // 각 target 병렬 fetch.
        await Promise.all(
          supportedTargets.map(async (v) => {
            const rows = rowsByVersion.get(v) ?? [];
            try {
              // 렌더 레벨은 항상 sgg.
              const slice = await fetchTimelineSlice(
                v,
                "sgg",
                rows.map((r) => r.code),
                rows.map((r) => r.weight),
                emdWeightByVersion.get(v),
              );
              if (cancelled) return;
              results.set(v, slice);
              setSlices(new Map(results));
            } catch (e) {
              if (cancelled) return;
              errs.set(v, String(e));
              setErrors(new Map(errs));
            }
          }),
        );
      } catch (e) {
        if (cancelled) return;
        // matchAdm 전체 실패 — 모든 target 에 에러 표시.
        for (const v of supportedTargets) errs.set(v, String(e));
        setErrors(new Map(errs));
      }
    };

    runMatch().finally(() => {
      if (!cancelled) setLoading(false);
    });
    return () => {
      cancelled = true;
    };
  }, [query, versions, versionList]);

  // 초기 viewport 는 셀 내부에서 계산 — bbox + 셀의 실제 픽셀 width/height 둘 다
  // 있어야 잘리지 않고 fit 가능하기 때문.

  // 쿼리 변경 시 viewport + baseGeometries 초기화, 선택 대상 각각의 geometry 로드.
  const lastQueryKey = useRef<string | null>(null);
  useEffect(() => {
    const k = query
      ? `${query.baseVersion}|${query.level}|${query.selections
          .map((s) => s.code)
          .sort()
          .join(",")}`
      : null;
    if (k !== lastQueryKey.current) {
      lastQueryKey.current = k;
      setViewport(null);
      setBaseGeometries([]);
    }
    if (!query) return;
    let cancelled = false;
    (async () => {
      const meta = await fetchMeta(query.baseVersion);
      const geoms = await Promise.all(
        query.selections.map(async (sel) => {
          const entry =
            query.level === "sido"
              ? meta.sido.get(sel.code)
              : meta.sgg.get(sel.code);
          if (!entry) return null;
          const buf = await fetchBinRange(query.baseVersion, entry);
          return decodeWKB(buf, 0);
        }),
      );
      if (cancelled) return;
      setBaseGeometries(geoms.filter((g): g is DecodedGeometry => g !== null));
    })();
    return () => {
      cancelled = true;
    };
  }, [query, setViewport, setBaseGeometries]);

  if (!query) {
    return (
      <div className="h-full flex items-center justify-center text-muted-foreground text-sm">
        시계열추적 탭에서 지역과 연도를 고른 뒤 시작하세요.
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col bg-muted/30">
      <Header
        query={query}
        count={versions.length}
        loaded={slices.size}
        loading={loading}
        onClose={() => setTimelineViewActive(false)}
      />
      <div className="flex-1 overflow-y-auto">
        <div className="p-3 space-y-3">
          {versions.map((v) => (
            <TimelineCell
              key={v}
              version={v}
              slice={slices.get(v) ?? null}
              error={errors.get(v) ?? null}
              query={query}
              viewport={viewport}
              showLabels={showLabels}
              baseGeometries={baseGeometries}
              isBaseVersion={v === query.baseVersion}
              names={namesByVersion.get(v) ?? null}
              onViewportChange={(patch) => updateViewport(patch)}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function Header({
  query,
  count,
  loaded,
  loading,
  onClose,
}: {
  query: NonNullable<ReturnType<typeof useTimelineStore.getState>["query"]>;
  count: number;
  loaded: number;
  loading: boolean;
  onClose: () => void;
}) {
  const title = query.selections
    .map((s) => (s.parentName ? `${s.parentName} ${s.name}` : s.name))
    .join(", ");
  return (
    <div className="shrink-0 border-b border-border bg-background px-4 py-2.5 flex items-center gap-3">
      <div className="min-w-0 flex-1">
        <div className="text-sm font-semibold truncate">{title}</div>
        <div className="text-[10px] text-muted-foreground">
          기준 {fmtVersionLabel(query.baseVersion)} ·{" "}
          {query.level === "sido" ? "시도" : "시군구"}
          {query.selections.length > 1 && ` ${query.selections.length}개`} ·
          연도 {count}개 ({loaded}/{count} 로드)
          {loading && <span className="inline-block ml-1">· 로딩중</span>}
        </div>
      </div>
      <button
        onClick={onClose}
        className="p-1.5 rounded-md hover:bg-muted"
        aria-label="닫기"
      >
        <X className="h-4 w-4" />
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 개별 셀
// ---------------------------------------------------------------------------
interface CellProps {
  version: string;
  slice: TimelineSlice | null;
  error: string | null;
  query: NonNullable<ReturnType<typeof useTimelineStore.getState>["query"]>;
  viewport: TimelineViewport | null;
  showLabels: boolean;
  baseGeometries: DecodedGeometry[];
  isBaseVersion: boolean;
  names: NameMaps | null;
  onViewportChange: (patch: Partial<TimelineViewport>) => void;
}

function TimelineCell({
  version,
  slice,
  error,
  query,
  viewport,
  showLabels,
  baseGeometries,
  isBaseVersion,
  names,
  onViewportChange,
}: CellProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [width, setWidth] = useState(0);
  // Noto Sans KR 웹폰트 로드 완료 여부. canvas 는 폰트 로드 전 그리면 fallback 으로
  // 렌더되고 자동 재draw 가 안 되므로, 로드되면 fontReady 를 flip 해 draw effect 재실행.
  const [fontReady, setFontReady] = useState(false);
  useEffect(() => {
    let cancelled = false;
    const done = () => !cancelled && setFontReady(true);
    // 라벨용 700 굵기 Noto Sans KR 이 실제 로드됐는지 확인 후 flip.
    document.fonts
      .load('700 16px "Noto Sans KR"')
      .then(() => document.fonts.ready)
      .then(done)
      .catch(done); // 실패해도 fallback 폰트로라도 그리게 flip.
    return () => {
      cancelled = true;
    };
  }, []);

  // 셀 width 측정. 커지고 작아지는 양방향 모두 반응.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        // contentRect 가 있으면 그걸 쓰되, 없으면 clientWidth 로 fallback.
        const w = entry.contentRect?.width ?? el.clientWidth;
        setWidth(Math.round(w));
      }
    });
    ro.observe(el);
    setWidth(el.clientWidth);
    return () => ro.disconnect();
  }, []);

  // 초기 viewport 세팅: 기준 연도 셀이 bbox+width 를 알게 된 순간에 계산.
  //   width/height 두 축 중 더 제약이 큰 쪽에 맞춰 scale 결정 → 어느 방향으로도 잘리지 않음.
  //   기준 연도가 없거나 exists=false 면 (드문 케이스) 로드된 가장 최근 slice 중 첫 셀이 담당.
  const setViewportStore = useTimelineStore((s) => s.setViewport);
  useEffect(() => {
    if (viewport) return;
    if (!slice || !slice.exists || !slice.bbox) return;
    if (width === 0) return;
    if (!isBaseVersion) return; // 기준 연도 셀만 viewport 초기화 담당
    const [minX, minY, maxX, maxY] = slice.bbox;
    const cx = (minX + maxX) / 2;
    const cy = (minY + maxY) / 2;
    const lonSpan = Math.max(0.0001, maxX - minX);
    const latSpan = Math.max(0.0001, maxY - minY);
    // equirectangular: 한 픽셀에 1도 = scale (lat), scale*cos(lat) (lon)
    const cosLat = Math.cos((cy * Math.PI) / 180);
    const scaleByHeight = (CELL_HEIGHT * 0.9) / latSpan;
    const scaleByWidth = (width * 0.9) / (lonSpan * cosLat);
    const scale = Math.min(scaleByHeight, scaleByWidth);
    setViewportStore({
      center: [cx, cy],
      scale,
      initialBBox: slice.bbox,
    });
  }, [slice, width, viewport, isBaseVersion, setViewportStore]);

  // 그리기.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !slice || !viewport || width === 0) return;
    drawCell(
      canvas,
      slice,
      query,
      viewport,
      width,
      CELL_HEIGHT,
      showLabels,
      baseGeometries,
      isBaseVersion,
      names,
    );
  }, [
    slice,
    viewport,
    query,
    width,
    showLabels,
    baseGeometries,
    isBaseVersion,
    names,
    fontReady, // 웹폰트 로드 완료 시 재draw (라벨을 Noto Sans KR 로 다시 그림)
  ]);

  // wheel: 줌. pointer drag: 팬.
  // 주의: viewport 를 closure 에 캡처하지 않는다 — store 에서 매번 최신값을 읽어야
  // 연속 pan 시 delta 가 누적된다 (stale closure 방지).
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const getVP = () => useTimelineStore.getState().viewport;

    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const vp = getVP();
      if (!vp) return;
      const rect = canvas.getBoundingClientRect();
      const px = e.clientX - rect.left;
      const py = e.clientY - rect.top;
      const world = screenToWorld(px, py, vp, width, CELL_HEIGHT);
      const zoomFactor = Math.exp(-e.deltaY * 0.0015);
      const newScale = clampScale(vp.scale * zoomFactor);
      const newCenter = recenterAtPointer(
        world,
        px,
        py,
        newScale,
        width,
        CELL_HEIGHT,
      );
      useTimelineStore.getState().updateViewport({
        scale: newScale,
        center: newCenter,
      });
    };

    let panning = false;
    let lastX = 0;
    let lastY = 0;
    const onPointerDown = (e: PointerEvent) => {
      panning = true;
      lastX = e.clientX;
      lastY = e.clientY;
      canvas.setPointerCapture(e.pointerId);
      canvas.style.cursor = "grabbing";
    };
    const onPointerMove = (e: PointerEvent) => {
      if (!panning) return;
      const vp = getVP();
      if (!vp) return;
      const dx = e.clientX - lastX;
      const dy = e.clientY - lastY;
      lastX = e.clientX;
      lastY = e.clientY;
      const latRad = (vp.center[1] * Math.PI) / 180;
      const scaleX = vp.scale * Math.cos(latRad);
      const newCenter: [number, number] = [
        vp.center[0] - dx / scaleX,
        vp.center[1] + dy / vp.scale,
      ];
      useTimelineStore.getState().updateViewport({ center: newCenter });
    };
    const onPointerUp = (e: PointerEvent) => {
      panning = false;
      canvas.releasePointerCapture(e.pointerId);
      canvas.style.cursor = "grab";
    };

    canvas.addEventListener("wheel", onWheel, { passive: false });
    canvas.addEventListener("pointerdown", onPointerDown);
    canvas.addEventListener("pointermove", onPointerMove);
    canvas.addEventListener("pointerup", onPointerUp);
    canvas.addEventListener("pointercancel", onPointerUp);
    return () => {
      canvas.removeEventListener("wheel", onWheel);
      canvas.removeEventListener("pointerdown", onPointerDown);
      canvas.removeEventListener("pointermove", onPointerMove);
      canvas.removeEventListener("pointerup", onPointerUp);
      canvas.removeEventListener("pointercancel", onPointerUp);
    };
  }, [width]);

  return (
    <div
      ref={containerRef}
      className="relative bg-background rounded-md overflow-hidden min-w-0"
      style={{
        height: CELL_HEIGHT,
        border: isBaseVersion ? "3px solid #dc2626" : "1px solid var(--border)",
      }}
    >
      <div className="absolute top-1.5 left-2 text-[16px] font-mono text-white z-10 pointer-events-none bg-black/85 px-1.5 py-0.5 rounded">
        {fmtVersionLabel(version)}
      </div>
      {!slice && !error && (
        <div className="absolute inset-0 flex items-center justify-center text-muted-foreground text-xs">
          <Loader2 className="h-4 w-4 animate-spin mr-1" /> 로딩...
        </div>
      )}
      {error && (
        <div className="absolute inset-0 flex items-center justify-center text-red-500 text-xs px-4 text-center">
          {error}
        </div>
      )}
      {slice && !slice.exists && (
        <div className="absolute inset-0 flex items-center justify-center text-muted-foreground text-xs">
          이 버전에 데이터 없음
        </div>
      )}
      <canvas
        ref={canvasRef}
        width={Math.max(1, width) * devicePixelRatioSafe()}
        height={CELL_HEIGHT * devicePixelRatioSafe()}
        style={{
          width: "100%",
          height: CELL_HEIGHT,
          display: "block",
          cursor: "grab",
        }}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// draw
// ---------------------------------------------------------------------------
function drawCell(
  canvas: HTMLCanvasElement,
  slice: TimelineSlice,
  query: NonNullable<ReturnType<typeof useTimelineStore.getState>["query"]>,
  viewport: TimelineViewport,
  cssWidth: number,
  cssHeight: number,
  showLabels: boolean,
  baseGeometries: DecodedGeometry[],
  isBaseVersion: boolean,
  names: NameMaps | null,
) {
  const dpr = devicePixelRatioSafe();
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.save();
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, cssWidth, cssHeight);

  if (!slice.exists) {
    ctx.restore();
    return;
  }

  ctx.lineJoin = "round";
  ctx.lineCap = "round";

  // 규칙:
  //   parent (sgg) weight >= 0.9 → 굵은 경계
  //   parent (sgg) weight <  0.9 → 얇은 경계
  //   emd (child) weight >= 0.9 → 시도별 파스텔 fill, 얇은 경계
  //   emd (child) weight <  0.9 → fill 없음, 얇은 경계
  const parentLineWidth = 1.8;
  const childLineWidth = 0.6;
  const parentColor = "#1e293b"; // slate-800
  const childColor = "#94a3b8"; // slate-400
  const weakFill = "rgba(148, 163, 184, 0.30)";

  // 색 할당 기준:
  //   level=sido → sidocd 기준. base sido 는 palette[0].
  //   level=sgg  → sggcd 기준. base selection 의 각 sggcd 가 palette[0..N-1],
  //                그 외 매칭된 sgg 는 palette[N..] 로 이어짐.
  //   즉 "기준과 같은 행정구역이면 같은 색, 합병/분할/편입된 다른 구역은 다른 색".
  const colorKey: "sido" | "sgg" = query.level === "sido" ? "sido" : "sgg";
  const baseKeys: string[] =
    colorKey === "sido"
      ? query.selections.map((s) => s.code)
      : query.selections.map((s) => s.code);
  const colorMap = assignColors(slice, colorKey, baseKeys, names);

  // children 먼저. weight 기준으로 fill 결정.
  for (const g of slice.groups) {
    const cws = g.childWeights;
    for (let i = 0; i < g.children.length; i++) {
      const c = g.children[i]!;
      const w = cws?.[i] ?? 0;
      let fillColor: string | null;
      if (w >= 0.9) {
        const key =
          colorKey === "sido"
            ? sidocdOf(c.code, "emd", names)
            : sggcdOf(c.code, "emd", names);
        fillColor = colorMap.get(key) ?? PASTEL_PALETTE[0]!;
      } else {
        fillColor = weakFill;
      }
      drawGeometry(ctx, c.geometry, viewport, cssWidth, cssHeight, {
        fillColor,
        strokeColor: childColor,
        lineWidth: childLineWidth,
      });
    }
  }
  // parent — weight 에 따라 두 규격.
  for (const g of slice.groups) {
    const isStrong = (g.weight ?? 1) >= 0.9;
    drawGeometry(ctx, g.parent.geometry, viewport, cssWidth, cssHeight, {
      fillColor: null,
      strokeColor: isStrong ? parentColor : childColor,
      lineWidth: isStrong ? parentLineWidth : childLineWidth,
    });
  }

  // base 연도 region 의 빨간 실선 오버레이 (다른 연도 cell 에만).
  if (!isBaseVersion) {
    for (const g of baseGeometries) {
      drawBaseOverlay(ctx, g, viewport, cssWidth, cssHeight);
    }
  }

  // 레이블.
  if (showLabels) {
    drawLabels(ctx, slice, query, viewport, cssWidth, cssHeight, names);
  }

  ctx.restore();
}

/** 기준 연도 region 의 outline 을 빨간 점선으로.
 *  성능: 점 솎기(stride) + 외곽 ring 만 그림 + 한 번의 stroke. */
function drawBaseOverlay(
  ctx: CanvasRenderingContext2D,
  geom: DecodedGeometry,
  vp: TimelineViewport,
  cssWidth: number,
  cssHeight: number,
) {
  ctx.save();
  ctx.lineJoin = "round";
  ctx.lineCap = "round";
  ctx.beginPath();
  const polys = geom.type === "Polygon" ? [geom.coordinates] : geom.coordinates;
  // 화면 전체 폭 대비 ring 점 개수가 많으면 stride 로 솎음. 점선이라 디테일 손실 무시 가능.
  // 솎음 기준: 대략 1px 당 1점이면 충분. 위 수치는 runtime 에서 계산하지 않고 고정 샘플링.
  for (const poly of polys) {
    // outer ring 만 사용 (hole 은 점선 오버레이엔 불필요).
    const ring = poly[0];
    if (!ring || ring.length < 2) continue;
    const stride = ring.length > 400 ? Math.ceil(ring.length / 400) : 1;
    for (let i = 0; i < ring.length; i += stride) {
      const [lon, lat] = ring[i]!;
      const [x, y] = worldToScreen(lon, lat, vp, cssWidth, cssHeight);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    // 마지막 점이 끝점으로 안 들어왔으면 보정.
    const last = ring[ring.length - 1]!;
    const [lx, ly] = worldToScreen(last[0]!, last[1]!, vp, cssWidth, cssHeight);
    ctx.lineTo(lx, ly);
    ctx.closePath();
  }
  ctx.strokeStyle = "rgba(220, 38, 38, 0.55)";
  ctx.lineWidth = 2.5;
  ctx.stroke();
  ctx.restore();
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

/** polylabel: polygon 내부의 가장 안쪽 점. MultiPolygon 은 가장 큰 조각. */
function featureLabelPos(geom: DecodedGeometry): [number, number] {
  const precision = 0.001;
  if (geom.type === "Polygon") {
    const p = polylabel(geom.coordinates as number[][][], precision);
    return [p[0], p[1]];
  }
  let best: number[][][] | null = null;
  let bestArea = -1;
  for (const poly of geom.coordinates) {
    const outer = poly[0];
    if (!outer) continue;
    let minX = Infinity,
      minY = Infinity,
      maxX = -Infinity,
      maxY = -Infinity;
    for (const [x, y] of outer) {
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    }
    const area = (maxX - minX) * (maxY - minY);
    if (area > bestArea) {
      bestArea = area;
      best = poly as number[][][];
    }
  }
  if (!best) return [0, 0];
  const p = polylabel(best, precision);
  return [p[0], p[1]];
}

interface LabelItem {
  pos: [number, number];
  lines: string[];
  size: number;
  priority: number; // 큰 면이 우선
}

/** scale (px/deg) 기반 줌 판정.
 *  - scale >= EMD: emd 라벨 ("sgg\nemd")
 *  - SIDO_ONLY <= scale < EMD: sgg 라벨 ("sido_short\nsgg")
 *  - scale < SIDO_ONLY: sido 풀네임만 */
const EMD_LABEL_MIN_SCALE = 3400;
const SIDO_ONLY_MAX_SCALE = 400; // 이 값보다 작으면 sido 풀네임만.

function drawLabels(
  ctx: CanvasRenderingContext2D,
  slice: TimelineSlice,
  query: NonNullable<ReturnType<typeof useTimelineStore.getState>["query"]>,
  viewport: TimelineViewport,
  cssWidth: number,
  cssHeight: number,
  names: NameMaps | null,
) {
  const items: LabelItem[] = [];
  // 3 단계:
  //   scale < SIDO_ONLY_MAX_SCALE → sido 풀네임만 (예: "충청남도")
  //   SIDO_ONLY <= scale < EMD → sgg 라벨 ("시도short\nsgg")
  //   scale >= EMD → emd 라벨 ("sgg\nemd")
  const tier: "sido" | "sgg" | "emd" =
    viewport.scale < SIDO_ONLY_MAX_SCALE
      ? "sido"
      : viewport.scale < EMD_LABEL_MIN_SCALE
        ? "sgg"
        : "emd";

  // 라벨 크기: 검색·조회 탭의 MapLibre symbol 과 통일 (sido 18 / sgg 15 / emd 12).
  const SIZE_SIDO = 18;
  const SIZE_SGG = 15;
  const SIZE_EMD = 12;

  if (tier === "sido") {
    // sido 풀네임 — sidocd 별로 그 sido 에 속한 모든 sgg 의 polygon 을 하나의
    // MultiPolygon 으로 합쳐 polylabel. polylabel 이 가장 큰 내접원을 알아서 고르므로
    // 바다를 포함한 bbox 기준이 아닌 '실제 가장 큰 땅덩이' 에 라벨이 찍힘.
    const bySidoPolys = new Map<
      string,
      { name: string; polys: number[][][][] }
    >();
    for (const g of slice.groups) {
      const sidocd = sidocdOf(g.parent.code, g.parent.level, names);
      const sidonm = sidonmOf(sidocd, names) ?? sidocd;
      const entry = bySidoPolys.get(sidocd) ?? { name: sidonm, polys: [] };
      const geom = g.parent.geometry;
      if (geom.type === "Polygon") {
        entry.polys.push(geom.coordinates as number[][][]);
      } else {
        for (const p of geom.coordinates) entry.polys.push(p as number[][][]);
      }
      bySidoPolys.set(sidocd, entry);
    }
    for (const v of bySidoPolys.values()) {
      // 한 sido 에 속한 여러 polygon 중 '가장 큰 땅덩이' 하나의 polylabel.
      // polylabel 은 한 polygon 만 받으므로, 각 polygon 의 shoelace 면적을 구해
      // 최대 면적 polygon 을 골라 거기에 polylabel 을 돌린다.
      let bestPoly: number[][][] | null = null;
      let bestArea = -Infinity;
      for (const poly of v.polys) {
        const ring = poly[0];
        if (!ring) continue;
        const a = Math.abs(shoelace(ring));
        if (a > bestArea) {
          bestArea = a;
          bestPoly = poly;
        }
      }
      if (!bestPoly) continue;
      const p = polylabel(bestPoly, 0.001);
      items.push({
        pos: [p[0], p[1]],
        lines: [v.name],
        size: SIZE_SIDO,
        priority: bestArea,
      });
    }
  } else if (tier === "sgg") {
    // sgg 라벨
    for (const g of slice.groups) {
      const pos = featureLabelPos(g.parent.geometry);
      const sidocd = sidocdOf(g.parent.code, g.parent.level, names);
      const sidoShort = shortSido(sidonmOf(sidocd, names));
      const sggnm = names?.sgg.get(g.parent.code)?.name ?? g.parent.code;
      const lines = sidoShort ? [sidoShort, sggnm] : [sggnm];
      items.push({
        pos,
        lines,
        size: SIZE_SGG,
        priority: approxArea(g.parent.geometry),
      });
    }
  } else {
    // emd 라벨
    for (const g of slice.groups) {
      for (const c of g.children) {
        const pos = featureLabelPos(c.geometry);
        const emdRow = names?.emd.get(c.code);
        const emdName = emdRow?.name ?? c.code;
        const sggnm =
          emdRow?.sggnm ?? names?.sgg.get(g.parent.code)?.name ?? g.parent.code;
        items.push({
          pos,
          lines: [sggnm, emdName],
          size: SIZE_EMD,
          priority: approxArea(c.geometry),
        });
      }
    }
  }

  // 겹침 회피: priority(면적) 큰 라벨 우선 배치. 이미 놓인 라벨의 화면 bbox 와
  // 겹치면 skip → MapLibre symbol 의 collision(text-allow-overlap:false) + sort-key 와 동일 원리.
  items.sort((a, b) => b.priority - a.priority);

  ctx.textAlign = "center";
  ctx.textBaseline = "middle";

  // 이미 배치된 라벨들의 화면 bbox (충돌 검사용).
  const placed: Array<{ x0: number; y0: number; x1: number; y1: number }> = [];
  // 라벨 사이 최소 간격(px) — 붙어도 답답하지 않게 약간의 padding.
  const PAD = 2;

  for (const it of items) {
    const [x, y] = worldToScreen(
      it.pos[0],
      it.pos[1],
      viewport,
      cssWidth,
      cssHeight,
    );
    if (x < 0 || x > cssWidth || y < 0 || y > cssHeight) continue;

    // 검색·조회 탭 symbol 과 통일한 폰트/halo/색. 한글은 symbol 의
    // localIdeographFontFamily 와 같은 Noto Sans KR 로 그려 두 화면이 일치.
    ctx.font = `700 ${it.size}px "Noto Sans KR", "Malgun Gothic", "Apple SD Gothic Neo", sans-serif`;
    const lineH = it.size * 1.1;

    // 이 라벨의 화면 bbox 계산 (가장 넓은 줄 폭 × 전체 줄 높이).
    let maxW = 0;
    for (const line of it.lines) {
      const w = ctx.measureText(line).width;
      if (w > maxW) maxW = w;
    }
    const totalH = it.lines.length * lineH;
    const bx0 = x - maxW / 2 - PAD;
    const bx1 = x + maxW / 2 + PAD;
    const by0 = y - totalH / 2 - PAD;
    const by1 = y + totalH / 2 + PAD;

    // 이미 놓인 라벨과 겹치면 skip (AABB 교차 검사).
    let collides = false;
    for (const p of placed) {
      if (bx0 < p.x1 && bx1 > p.x0 && by0 < p.y1 && by1 > p.y0) {
        collides = true;
        break;
      }
    }
    if (collides) continue;
    placed.push({ x0: bx0, y0: by0, x1: bx1, y1: by1 });

    // halo(흰 외곽선) → 채움. symbol 의 text-halo(#fff, width 2.5) + text-color(#111) 통일.
    ctx.lineWidth = 3;
    ctx.lineJoin = "round";
    ctx.miterLimit = 2;
    ctx.strokeStyle = "#ffffff";
    ctx.fillStyle = "#111111";
    const startY = y - ((it.lines.length - 1) / 2) * lineH;
    for (let i = 0; i < it.lines.length; i++) {
      const ly = startY + i * lineH;
      ctx.strokeText(it.lines[i]!, x, ly);
      ctx.fillText(it.lines[i]!, x, ly);
    }
  }
}

/** Polygon outer ring 의 signed area (shoelace). 부호는 방향에 따라 변함. */
function shoelace(ring: number[][]): number {
  let sum = 0;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const a = ring[i]!;
    const b = ring[j]!;
    sum += (b[0]! - a[0]!) * (b[1]! + a[1]!);
  }
  return sum / 2;
}

function approxArea(geom: DecodedGeometry): number {
  let minX = Infinity,
    minY = Infinity,
    maxX = -Infinity,
    maxY = -Infinity;
  const polys = geom.type === "Polygon" ? [geom.coordinates] : geom.coordinates;
  for (const poly of polys) {
    const outer = poly[0];
    if (!outer) continue;
    for (const [x, y] of outer) {
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    }
  }
  if (!isFinite(minX)) return 0;
  return (maxX - minX) * (maxY - minY);
}

/** slice 의 parent/children 에 등장하는 (sido 또는 sgg) 코드에 파스텔 색 할당.
 *  baseKeys 순서대로 palette[0..N-1] 에 앵커링하고, 그 외 코드는 등장 순서로 이어짐.
 *  level=sido 조회: baseKeys = [sidocd]. level=sgg 조회: baseKeys = [sggcd, ...]. */
function assignColors(
  slice: TimelineSlice,
  colorKey: "sido" | "sgg",
  baseKeys: string[],
  names: NameMaps | null,
): Map<string, string> {
  const out = new Map<string, string>();
  const usedHues: number[] = []; // 이미 쓴 hue 들 (충돌 회피용)
  // 1) 기준 선택은 여러 개든 모두 palette[0] (같은 조회 대상이므로 같은 색).
  for (const k of baseKeys) {
    if (!out.has(k)) out.set(k, PASTEL_PALETTE[0]!);
  }
  if (baseKeys.length > 0) usedHues.push(PASTEL_HUES[0]!);

  let idx = 1; // base 는 palette[0] 을 통째로 가져갔으므로 다음은 [1] 부터.
  const assign = (key: string) => {
    if (out.has(key)) return;
    let color: string;
    if (idx < PASTEL_PALETTE.length) {
      color = PASTEL_PALETTE[idx]!;
      usedHues.push(PASTEL_HUES[idx]!);
    } else {
      color = pastelFromKeyUnique(key, usedHues);
    }
    out.set(key, color);
    idx += 1;
  };

  const keyOf = (code: string, level: "sido" | "sgg" | "emd"): string =>
    colorKey === "sido"
      ? sidocdOf(code, level, names)
      : sggcdOf(code, level, names);

  // 2) parent 에서 등장한 순 (매칭 weight 내림차순 정렬되어 있음).
  for (const g of slice.groups) {
    assign(keyOf(g.parent.code, g.parent.level));
  }
  // 3) children (parent 와 다른 key 일 수 있음).
  for (const g of slice.groups) {
    for (const c of g.children) {
      assign(keyOf(c.code, c.level));
    }
  }
  return out;
}

/** feature 의 code + level 에서 sidocd 를 유도.
 *  - names 에 등록된 경우: row.sidocd 우선
 *  - 아니면 code 앞 2자리 (surrogate "name:..." 이면 전체를 sidocd 로 취급) */
function sidocdOf(
  code: string,
  level: "sido" | "sgg" | "emd",
  names: NameMaps | null,
): string {
  if (names) {
    const row =
      level === "sido"
        ? names.sido.get(code)
        : level === "sgg"
          ? names.sgg.get(code)
          : names.emd.get(code);
    if (row?.sidocd) return row.sidocd;
  }
  if (code.startsWith("name:")) {
    // surrogate. "name:sidonm|sggnm|..." 의 sidonm 부분이 sido 식별자.
    const rest = code.slice(5);
    const sido = rest.split("|")[0] ?? rest;
    return `name:${sido}`;
  }
  return code.length >= 2 ? code.slice(0, 2) : code;
}

/** feature 의 code + level 에서 sggcd 를 유도.
 *  - names 에 등록된 경우: row.sggcd 우선 (emd row 에 있음)
 *  - 아니면 code 앞 5자리 (sgg 면 자기 자신, emd 면 emd[:5])
 *  - surrogate "name:sidonm|sggnm|..." 이면 sggnm 까지 잘라낸 "name:sidonm|sggnm" */
function sggcdOf(
  code: string,
  level: "sido" | "sgg" | "emd",
  names: NameMaps | null,
): string {
  if (level === "sido") return code; // sido level 에선 sgg 분해 불가 — sido code 그대로 반환.
  if (level === "sgg") return code;
  // level === "emd"
  if (names) {
    const row = names.emd.get(code);
    if (row?.sggcd) return row.sggcd;
  }
  if (code.startsWith("name:")) {
    // "name:sido|sgg|emd" → "name:sido|sgg"
    const rest = code.slice(5);
    const parts = rest.split("|");
    const joined = parts.slice(0, 2).join("|");
    return `name:${joined}`;
  }
  return code.length >= 5 ? code.slice(0, 5) : code;
}

/** sidocd -> sidonm. names 에서 lookup. 없으면 null. */
function sidonmOf(sidocd: string, names: NameMaps | null): string | null {
  if (!names) return null;
  const sidoRow = names.sido.get(sidocd);
  if (sidoRow?.sidonm) return sidoRow.sidonm;
  if (sidoRow?.name) return sidoRow.name;
  // sidocd 로 sido 레벨 row 를 못 찾으면 sgg row 에서라도 lookup.
  for (const row of names.sgg.values()) {
    if (row.sidocd === sidocd && row.sidonm) return row.sidonm;
  }
  return null;
}

function drawGeometry(
  ctx: CanvasRenderingContext2D,
  geom: DecodedGeometry,
  viewport: TimelineViewport,
  cssWidth: number,
  cssHeight: number,
  style: {
    fillColor: string | null;
    strokeColor: string;
    lineWidth: number;
  },
) {
  ctx.beginPath();
  const polys = geom.type === "Polygon" ? [geom.coordinates] : geom.coordinates;
  for (const poly of polys) {
    for (const ring of poly) {
      if (ring.length < 2) continue;
      for (let i = 0; i < ring.length; i++) {
        const [lon, lat] = ring[i]!;
        const [x, y] = worldToScreen(lon, lat, viewport, cssWidth, cssHeight);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.closePath();
    }
  }
  if (style.fillColor) {
    ctx.fillStyle = style.fillColor;
    ctx.fill("evenodd");
  }
  ctx.strokeStyle = style.strokeColor;
  ctx.lineWidth = style.lineWidth;
  ctx.stroke();
}

// ---------------------------------------------------------------------------
// 투영 유틸 (간이 equirectangular; lat 중심에서 cos 보정으로 종횡비 맞춤)
// ---------------------------------------------------------------------------
function worldToScreen(
  lon: number,
  lat: number,
  vp: TimelineViewport,
  w: number,
  h: number,
): [number, number] {
  const [cLon, cLat] = vp.center;
  const latRad = (cLat * Math.PI) / 180;
  const scaleX = vp.scale * Math.cos(latRad);
  const scaleY = vp.scale;
  const x = w / 2 + (lon - cLon) * scaleX;
  const y = h / 2 - (lat - cLat) * scaleY;
  return [x, y];
}

function screenToWorld(
  px: number,
  py: number,
  vp: TimelineViewport,
  w: number,
  h: number,
): [number, number] {
  const [cLon, cLat] = vp.center;
  const latRad = (cLat * Math.PI) / 180;
  const scaleX = vp.scale * Math.cos(latRad);
  const scaleY = vp.scale;
  const lon = cLon + (px - w / 2) / scaleX;
  const lat = cLat - (py - h / 2) / scaleY;
  return [lon, lat];
}

function recenterAtPointer(
  world: [number, number],
  px: number,
  py: number,
  newScale: number,
  w: number,
  h: number,
): [number, number] {
  // (px, py) 에 world 가 놓이는 center 를 역산.
  // px - w/2 = (world_lon - cLon) * scaleX
  // cLon = world_lon - (px - w/2) / scaleX
  const latRad = (world[1] * Math.PI) / 180; // 근사치; center latitude 대신 world latitude 사용
  const scaleX = newScale * Math.cos(latRad);
  const scaleY = newScale;
  const cLon = world[0] - (px - w / 2) / scaleX;
  const cLat = world[1] + (py - h / 2) / scaleY;
  return [cLon, cLat];
}

function clampScale(s: number): number {
  return Math.max(1, Math.min(200000, s));
}

function devicePixelRatioSafe(): number {
  if (typeof window === "undefined") return 1;
  return Math.min(window.devicePixelRatio || 1, 2);
}
