# mapshaper (vendored, 2개 파일)

npm `mapshaper` 패키지의 **브라우저 빌드** 두 개를 그대로 복사한 것이다.

| 파일 | 원본 | 역할 |
|---|---|---|
| `modules.js` | `node_modules/mapshaper/www/modules.js` | `window.modules` 에 mproj·buffer·iconv-lite 등을 올림 |
| `mapshaper.js` | `node_modules/mapshaper/www/mapshaper.js` | 본체. 로드 시 `window.modules[name]` 에서 의존성을 꺼내 씀 |

⚠️ **순서가 중요하다** — `modules.js` 를 먼저 로드해야 한다. 빠뜨리면
`require$1('mproj')` 가 undefined 를 받아 좌표계 처리에서 터진다.
(실제로 `mapshaper.js` 만 복사했다가 "window.mapshaper 없음" 으로 헤맸다.)

## 왜 복사해서 두나

npm 패키지를 그냥 `import` 하면 **Node 용 진입점**(`mapshaper/mapshaper.js`)이
번들에 들어가 `child_process`·`fs`·geopackage·geotiff 등 29개 모듈을 못 찾아
빌드가 깨진다.

브라우저용은 `www/mapshaper.js` 인데, 이건 ESM 이 아니라 `window.mapshaper` 를
세팅하는 **IIFE** 라 import 대상이 아니다. 그래서 정적 파일로 두고
`src/lib/supersimplify.ts` 가 script 태그로 1회 로드한다
(sql.js WASM 을 `public/` 에 두는 것과 같은 방식).

2.8MB 라 "단순화(많이)" 옵션을 실제로 쓸 때만 내려받는다.

## 갱신 방법

```powershell
cd z:\Github\adk-master\admdongkor
npm install mapshaper@<버전> --no-save
Copy-Item node_modules\mapshaper\www\modules.js web\public\modules.js
Copy-Item node_modules\mapshaper\www\mapshaper.js web\public\mapshaper.js
npm uninstall mapshaper   # 번들에 Node 진입점이 들어가면 빌드가 깨진다
```

갱신 후에는 "단순화(많이)" 로 시군구·시도를 실제로 받아 독도가 살아있는지
확인할 것 (`-simplify keep-shapes` 는 feature 만 지키고 작은 part 는 지운다).
