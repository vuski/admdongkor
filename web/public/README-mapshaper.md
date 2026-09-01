# mapshaper.js (vendored)

`mapshaper.js` 는 npm `mapshaper` 패키지의 **브라우저 빌드**
(`node_modules/mapshaper/www/mapshaper.js`) 를 그대로 복사한 것이다.

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
Copy-Item node_modules\mapshaper\www\mapshaper.js web\public\mapshaper.js
```

갱신 후에는 "단순화(많이)" 로 시군구·시도를 실제로 받아 독도가 살아있는지
확인할 것 (`-simplify keep-shapes` 는 feature 만 지키고 작은 part 는 지운다).
