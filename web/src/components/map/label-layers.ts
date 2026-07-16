import type { LabelDatum } from "./label-data";

/** 레벨별 라벨 포인트 묶음. buildLabels 결과를 useMemo 로 캐싱해서 전달.
 *  실제 렌더는 label-symbol.ts 의 MapLibre native symbol 레이어가 담당한다
 *  (collision 자동 회피 + SDF halo). */
export interface LabelData {
  sido: LabelDatum[];
  sgg: LabelDatum[];
  emd: LabelDatum[];
}
