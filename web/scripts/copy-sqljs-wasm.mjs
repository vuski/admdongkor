/**
 * 설치된 sql.js 의 WASM 을 public/ 으로 복사한다 (predev / prebuild).
 *
 * GeoPackage 다운로드(src/lib/gpkg.ts)는 sql.js 의 JS 를 번들에 넣고, WASM 은
 * public/ 에서 받는다. 둘은 **반드시 같은 버전**이어야 한다 — 버전이 다르면
 * WASM export 이름 배치가 달라 초기화 중 `a.O is not a function` 같은 오류로
 * 죽고, 그 오류는 Promise 밖에서 터져 잡히지도 않는다.
 *
 * 예전에는 WASM 을 public/ 에 손으로 복사해 두었다(1.14.1). 그런데 배포
 * 워크플로가 lockfile 을 지우고 `npm install` 하므로 JS 는 그때그때 최신
 * (^1.14.1 → 1.15.x) 로 올라가고 WASM 은 1.14.1 에 남아, 2026-10-17 배포부터
 * GeoPackage 다운로드가 전부 실패했다. 빌드마다 설치된 패키지에서 복사하면
 * 어떤 버전이 설치되든 둘이 어긋나지 않는다.
 */

import { copyFileSync, existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
// workspace 라 sql.js 가 루트 node_modules 로 hoist 될 수 있다 → resolve 로 찾는다.
// exports 맵이 "./package.json" 을 막고 있어 진입 파일(dist/sql-wasm.js)로 위치를 구한다.
const dist = path.dirname(require.resolve("sql.js"));
const pkgJson = path.join(dist, "..", "package.json");
const publicDir = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "public");
const { version } = JSON.parse(readFileSync(pkgJson, "utf8"));

// 브라우저 번들은 sql-wasm-browser.wasm, Node 는 sql-wasm.wasm 을 요청한다.
const files = ["sql-wasm-browser.wasm", "sql-wasm.wasm"];
for (const f of files) {
  const src = path.join(dist, f);
  if (!existsSync(src)) {
    throw new Error(`sql.js ${version} 에 ${f} 가 없다 (${src})`);
  }
  copyFileSync(src, path.join(publicDir, f));
}
console.log(`[copy-sqljs-wasm] sql.js ${version} → public/{${files.join(",")}}`);
