import type { Level } from "admdongkor";

export type SideColor = [number, number, number, number];

export const SIDE_A_FILL: SideColor = [37, 99, 235, 40];
export const SIDE_A_LINE: SideColor = [37, 99, 235, 200];

export const SIDE_B_FILL: SideColor = [249, 115, 22, 40];
export const SIDE_B_LINE: SideColor = [249, 115, 22, 200];

export function lineWidthForLevel(level: Level): number {
  switch (level) {
    case "sido":
      return 2;
    case "sgg":
      return 1.3;
    case "emd":
      return 0.7;
  }
}
