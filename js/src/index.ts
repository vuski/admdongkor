export { versions, versionsAsync, VERSIONS } from "./versions.js";
export type { VersionKey } from "./versions.js";
export { get, getParquet } from "./get.js";
export {
  find,
  findVersions,
  findFirst,
  findLast,
  clearIndexCache,
} from "./find.js";
export type { FindRow, FindOptions } from "./find.js";
export { findOffices } from "./offices.js";
export type { OfficeRow, FindOfficesOptions } from "./offices.js";
export { compare } from "./compare.js";
export type { CompareRow, CompareResult, CompareOptions } from "./compare.js";
export { matchAdm } from "./match.js";
export type {
  MatchEmdRow,
  MatchSggRow,
  MatchSidoRow,
  MatchOptions,
  MatchResult,
} from "./match.js";
export type {
  AdmFeature,
  AdmFeatureCollection,
  AdmProperties,
  EmdProperties,
  SggProperties,
  SidoProperties,
  GetOptions,
  Level,
} from "./types.js";
export {
  changelog,
  dataVersion,
  fetchManifest,
  clearManifestCache,
} from "./changelog.js";
export type { ChangelogEntry, Manifest } from "./changelog.js";
