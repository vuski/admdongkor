import { TextLayer } from "@deck.gl/layers";
import type { Level } from "admdongkor";
import type { LabelDatum } from "./label-data";

export interface LabelData {
  sido: LabelDatum[];
  sgg: LabelDatum[];
  emd: LabelDatum[];
}

export interface LabelLayerOptions {
  side: "A" | "B";
  /** 정수 zoom (소수점 변화는 무시). */
  zoomInt: number;
  level: Level;
  /** buildLabels 결과를 useMemo 로 캐싱해서 전달. */
  labelData: LabelData;
}

// 이 이상 줌에서만 emd 레이블 표시 (그 전엔 sgg 유지)
const EMD_MIN_ZOOM = 10;

const FONT_FAMILY =
  'Pretendard, "Apple SD Gothic Neo", "Malgun Gothic", sans-serif';

// 사용자 레퍼런스 (이미 검증된 설정) 그대로 사용.
function baseProps(id: string, data: LabelDatum[], sizePx: number) {
  return {
    id,
    data,
    getPosition: (d: LabelDatum) => d.position,
    getText: (d: LabelDatum) => d.text,
    getSize: sizePx,
    sizeUnits: "pixels" as const,
    getColor: [0, 0, 0, 255] as [number, number, number, number],
    getTextAnchor: "middle" as const,
    getAlignmentBaseline: "center" as const,
    fontFamily: FONT_FAMILY,
    fontWeight: 600,
    //outlineWidth: 7,
    //outlineColor: [255, 255, 255, 255] as [number, number, number, number],
    characterSet: "auto" as const,
    // fontSettings: {
    //   sdf: true,
    //   size: 48,
    //   radius: 20,
    //   cutoff: 0.25,
    // },
    // 디버그: extension 완전 제거. 보이면 extension 이 원인 확정.
    extensions: [],
    //getCollisionPriority: (d: LabelDatum) => d.priority,
  };
}

/** 현재 설정에 맞는 label 레이어들을 반환.
 *  labelData 는 useMemo 로 캐싱해서 전달해야 polylabel 재계산이 없음. */
export function buildLabelLayers(opts: LabelLayerOptions) {
  const { side, level, zoomInt, labelData } = opts;
  const layers: TextLayer<LabelDatum>[] = [];

  if (level === "sido") {
    if (labelData.sido.length > 0)
      layers.push(new TextLayer<LabelDatum>(baseProps(`labels-${side}-sido`, labelData.sido, 20) as never));
  } else if (level === "sgg") {
    if (labelData.sgg.length > 0)
      layers.push(new TextLayer<LabelDatum>(baseProps(`labels-${side}-sgg`, labelData.sgg, 16) as never));
  } else if (level === "emd") {
    if (zoomInt < EMD_MIN_ZOOM) {
      if (labelData.sgg.length > 0)
        layers.push(new TextLayer<LabelDatum>(baseProps(`labels-${side}-sgg-in-emd`, labelData.sgg, 16) as never));
    } else {
      if (labelData.emd.length > 0)
        layers.push(new TextLayer<LabelDatum>(baseProps(`labels-${side}-emd`, labelData.emd, 13) as never));
    }
  }

  return layers;
}
