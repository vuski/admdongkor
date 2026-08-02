"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Download, Loader2, AlertCircle, X } from "lucide-react";
import type { Level } from "admdongkor";
import { useAppStore } from "@/stores/app-store";
import { cn } from "@/lib/cn";
import {
  buildDownload,
  saveBlob,
  estimateMb,
  parquetNeedsSourceCrs,
  nativeParquetCrs,
  FORMAT_LABEL,
  FORMAT_NOTE,
  SOURCE_CRS,
  CUSTOM_CRS,
  type DownloadFormat,
  type DownloadProgress,
} from "@/lib/download";
import { CRS_DEFS, CRS_GROUPS } from "@/lib/crs";

const FORMATS: DownloadFormat[] = ["parquet", "geojson", "gpkg"];

const LEVELS: { id: Level; label: string }[] = [
  { id: "emd", label: "읍면동" },
  { id: "sgg", label: "시군구" },
  { id: "sido", label: "시도" },
];

function fmtVersion(k: string) {
  return `${k.slice(0, 4)}-${k.slice(4, 6)}-${k.slice(6, 8)}`;
}

export function DownloadPanel() {
  const versionKey = useAppStore((s) => s.versionKey);
  const versionList = useAppStore((s) => s.versionList);

  // 지도에서 보는 시점이 기본값. 사용자가 바꾸기 전까지 지도를 따라간다.
  const [picked, setPicked] = useState<string | null>(null);
  const selectedVersion = picked ?? versionKey;

  const [format, setFormat] = useState<DownloadFormat>("parquet");
  const [detail, setDetail] = useState(false);
  const [levels, setLevels] = useState<Level[]>(["emd"]);
  const [crs, setCrs] = useState<string>(SOURCE_CRS);
  const [customProj4, setCustomProj4] = useState("");
  const [progress, setProgress] = useState<DownloadProgress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => () => abortRef.current?.abort(), []);

  const estimate = useMemo(
    () => estimateMb(format, detail, levels),
    [format, detail, levels],
  );

  const busy = progress !== null;
  const crsBlocked = parquetNeedsSourceCrs(format, crs, detail);
  const customEmpty = crs === CUSTOM_CRS && !customProj4.trim();
  const canDownload = levels.length > 0 && !crsBlocked && !customEmpty;

  function toggleLevel(id: Level) {
    setLevels((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id],
    );
  }

  async function handleDownload() {
    if (!canDownload || busy) return;
    setError(null);
    const controller = new AbortController();
    abortRef.current = controller;
    setProgress({ ratio: 0, message: "준비 중…" });
    try {
      const { blob, filename } = await buildDownload({
        versionKey: selectedVersion,
        levels,
        format,
        detail,
        crs,
        customProj4,
        onProgress: setProgress,
        signal: controller.signal,
      });
      saveBlob(blob, filename);
      setProgress(null);
    } catch (e) {
      if ((e as Error)?.name === "AbortError") {
        setProgress(null);
        return;
      }
      setError(e instanceof Error ? e.message : String(e));
      setProgress(null);
    } finally {
      abortRef.current = null;
    }
  }

  return (
    <div className="rounded-lg border border-border bg-muted/30 p-3 space-y-3">
      <div className="flex items-center gap-1.5">
        <Download className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
        <span className="text-xs font-semibold">다운로드</span>
      </div>

      {/* 시점 */}
      <div>
        <label htmlFor="dl-version" className="text-[11px] font-medium block mb-1">
          시점
        </label>
        <select
          id="dl-version"
          value={selectedVersion}
          onChange={(e) => setPicked(e.target.value)}
          disabled={busy}
          className="w-full px-2 py-1.5 text-xs rounded-md border border-border bg-background focus:outline-none focus:border-accent disabled:opacity-50"
        >
          {versionList.map((v) => (
            <option key={v} value={v}>
              {fmtVersion(v)}
            </option>
          ))}
        </select>
        {picked !== null && picked !== versionKey && (
          <button
            onClick={() => setPicked(null)}
            className="text-[10px] text-muted-foreground hover:text-accent mt-1"
          >
            지도 시점({fmtVersion(versionKey)})으로 되돌리기
          </button>
        )}
      </div>

      {/* 형식 */}
      <div>
        <span className="text-[11px] font-medium block mb-1">형식</span>
        <div className="flex gap-1">
          {FORMATS.map((f) => (
            <button
              key={f}
              onClick={() => setFormat(f)}
              disabled={busy}
              className={cn(
                "flex-1 px-1.5 py-1.5 rounded-md border text-[11px] transition disabled:opacity-50",
                format === f
                  ? "border-accent bg-accent/10 font-medium"
                  : "border-border bg-background hover:border-accent/50",
              )}
            >
              {FORMAT_LABEL[f]}
            </button>
          ))}
        </div>
        <p className="text-[10px] text-muted-foreground mt-1 leading-relaxed">
          {FORMAT_NOTE[format]}
        </p>
      </div>

      {/* 해상도 — 형식과 독립 */}
      <div>
        <span className="text-[11px] font-medium block mb-1">해상도</span>
        <div className="flex gap-1">
          <button
            onClick={() => setDetail(false)}
            disabled={busy}
            className={cn(
              "flex-1 px-1.5 py-1.5 rounded-md border text-[11px] transition disabled:opacity-50",
              !detail
                ? "border-accent bg-accent/10 font-medium"
                : "border-border bg-background hover:border-accent/50",
            )}
          >
            단순화
          </button>
          <button
            onClick={() => setDetail(true)}
            disabled={busy}
            className={cn(
              "flex-1 px-1.5 py-1.5 rounded-md border text-[11px] transition disabled:opacity-50",
              detail
                ? "border-accent bg-accent/10 font-medium"
                : "border-border bg-background hover:border-accent/50",
            )}
          >
            원본
          </button>
        </div>
        <p className="text-[10px] text-muted-foreground mt-1 leading-relaxed">
          {detail
            ? "원본 해상도. 정밀 분석용이며 용량이 크다."
            : "mapshaper 단순화. 웹 지도·개괄 분석용."}
        </p>
      </div>

      {/* 좌표계 */}
      <div>
        <label htmlFor="dl-crs" className="text-[11px] font-medium block mb-1">
          좌표계
        </label>
        <select
          id="dl-crs"
          value={crs}
          onChange={(e) => setCrs(e.target.value)}
          disabled={busy}
          className="w-full px-2 py-1.5 text-xs rounded-md border border-border bg-background focus:outline-none focus:border-accent disabled:opacity-50"
        >
          {CRS_GROUPS.map((group) => (
            <optgroup key={group} label={group}>
              {Object.entries(CRS_DEFS)
                .filter(([, d]) => d.group === group)
                .map(([key, d]) => (
                  <option key={key} value={key}>
                    {d.epsg ? `${d.epsg} · ` : ""}
                    {d.label}
                  </option>
                ))}
            </optgroup>
          ))}
          <optgroup label="직접 입력">
            <option value={CUSTOM_CRS}>proj4 문자열 직접 입력…</option>
          </optgroup>
        </select>

        {crs === CUSTOM_CRS && (
          <textarea
            value={customProj4}
            onChange={(e) => setCustomProj4(e.target.value)}
            disabled={busy}
            rows={3}
            spellCheck={false}
            placeholder="+proj=tmerc +lat_0=38 +lon_0=127 +k=1 +x_0=200000 +y_0=600000 +ellps=GRS80 +units=m +no_defs"
            className="w-full mt-1 px-2 py-1.5 text-[10px] font-mono rounded-md border border-border bg-background focus:outline-none focus:border-accent disabled:opacity-50 resize-y"
          />
        )}

        {crsBlocked && (
          <p className="text-[10px] text-amber-700 dark:text-amber-500 mt-1 leading-relaxed">
            Parquet {detail ? "원본" : "단순화"} 은 {nativeParquetCrs(detail)} 로
            저장돼 있어 그대로만 받을 수 있습니다. 다른 좌표계는 GeoJSON 또는
            GeoPackage 를 선택하세요.
          </p>
        )}
      </div>

      {/* 경계 단위 */}
      <div>
        <span className="text-[11px] font-medium block mb-1">경계 단위</span>
        <div className="flex gap-1">
          {LEVELS.map(({ id, label }) => {
            const on = levels.includes(id);
            return (
              <label
                key={id}
                className={cn(
                  "flex-1 flex items-center justify-center gap-1 px-1.5 py-1.5 rounded-md border text-[11px] cursor-pointer transition",
                  on
                    ? "border-accent bg-accent/10 font-medium"
                    : "border-border bg-background hover:border-accent/50",
                  busy && "opacity-50 cursor-not-allowed",
                )}
              >
                <input
                  type="checkbox"
                  checked={on}
                  onChange={() => toggleLevel(id)}
                  disabled={busy}
                  className="accent-current"
                />
                {label}
              </label>
            );
          })}
        </div>
      </div>

      {/* 요약 + 실행 */}
      <div className="space-y-1.5">
        <div className="text-[10px] text-muted-foreground">
          {levels.length === 0
            ? "경계 단위를 하나 이상 선택하세요."
            : `${levels.length}개 파일 · 약 ${estimate.toFixed(1)} MB · zip`}
        </div>

        {busy ? (
          <div className="space-y-1.5">
            <div className="h-1 rounded-full bg-muted overflow-hidden">
              <div
                className="h-full bg-accent transition-[width] duration-200"
                style={{ width: `${Math.round((progress?.ratio ?? 0) * 100)}%` }}
              />
            </div>
            <div className="flex items-center justify-between gap-2">
              <span className="text-[10px] text-muted-foreground flex items-center gap-1 min-w-0">
                <Loader2 className="h-3 w-3 animate-spin shrink-0" />
                <span className="truncate">{progress?.message}</span>
              </span>
              <button
                onClick={() => abortRef.current?.abort()}
                className="text-[10px] text-muted-foreground hover:text-accent flex items-center gap-0.5 shrink-0"
              >
                <X className="h-3 w-3" />
                취소
              </button>
            </div>
          </div>
        ) : (
          <button
            onClick={handleDownload}
            disabled={!canDownload}
            className={cn(
              "w-full px-3 py-2 rounded-md text-xs border transition flex items-center justify-center gap-1.5",
              !canDownload
                ? "bg-muted border-border text-muted-foreground cursor-not-allowed"
                : "bg-accent text-accent-foreground border-accent hover:opacity-90",
            )}
          >
            <Download className="h-3.5 w-3.5" />
            zip 으로 받기
          </button>
        )}

        {error && (
          <div className="flex items-start gap-1.5 text-[10px] text-red-600 px-1.5 py-1 rounded bg-red-50 dark:bg-red-950/30">
            <AlertCircle className="h-3 w-3 mt-0.5 shrink-0" />
            <span className="min-w-0">{error}</span>
          </div>
        )}
      </div>
    </div>
  );
}
