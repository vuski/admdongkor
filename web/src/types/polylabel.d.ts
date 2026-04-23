declare module "@mapbox/polylabel" {
  /**
   * Polygon 내부의 가장 "안쪽 깊숙한 점" 을 찾아 반환 (레이블 배치용).
   *
   * @param polygon GeoJSON-style polygon: [ ring, hole1, hole2, ... ]
   *                각 ring 은 [[x, y], ...]
   * @param precision 탐색 해상도 (경위도 단위면 0.001 정도)
   * @param debug
   * @returns [x, y] — polygon 내부의 점
   */
  function polylabel(
    polygon: number[][][],
    precision?: number,
    debug?: boolean,
  ): [number, number] & { distance: number };
  export default polylabel;
}
