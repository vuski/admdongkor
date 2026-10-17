"""패키지 배포 이후 추가된 시점을 manifest 로 받아들이는지 (네트워크 없음).

배경: get / match_adm / compare 가 패키지 안의 정적 VERSIONS 로만 키를 검사해서,
dist/data/ 에 새 시점을 push 해도 PyPI 재배포 전까지는 `unknown version key` 로
거부했다 (2026-10, 20261001 에서 발견). 지금은 import 시 자동 갱신되는 manifest 의
`versions` 도 함께 본다.
"""

from __future__ import annotations

import json

import pytest

import admdongkor as adk
from admdongkor import _cache, api
from admdongkor._versions import VERSIONS

NEW_KEY = "29991231"  # 패키지 스냅샷에는 없고 manifest 에만 있는 가상의 새 시점


def _write_manifest(tmp_path, versions):
    (tmp_path / "manifest.json").write_text(
        json.dumps({"data_version": "2999.12.31", "versions": versions}),
        encoding="utf-8",
    )


@pytest.fixture
def manifest_with_new_key(tmp_path, monkeypatch):
    """ADMDONGKOR_DATA_DIR 를 임시 폴더로 고정하고, 새 시점이 든 manifest 를 둔다."""
    monkeypatch.setenv("ADMDONGKOR_DATA_DIR", str(tmp_path))
    _write_manifest(tmp_path, [*VERSIONS, NEW_KEY])
    return tmp_path


class TestManifestVersions:
    def test_reads_versions(self, manifest_with_new_key):
        assert _cache.manifest_versions() == sorted([*VERSIONS, NEW_KEY])

    def test_no_manifest(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ADMDONGKOR_DATA_DIR", str(tmp_path))
        assert _cache.manifest_versions() is None

    def test_old_manifest_without_versions(self, tmp_path, monkeypatch):
        # 2026.07 이전 manifest 에는 versions 필드가 없다
        monkeypatch.setenv("ADMDONGKOR_DATA_DIR", str(tmp_path))
        (tmp_path / "manifest.json").write_text('{"data_version": "2026.04.25"}')
        assert _cache.manifest_versions() is None

    @pytest.mark.parametrize("bad", [[], ["2026"], [20261001], "20261001"])
    def test_malformed_versions(self, tmp_path, monkeypatch, bad):
        monkeypatch.setenv("ADMDONGKOR_DATA_DIR", str(tmp_path))
        _write_manifest(tmp_path, bad)
        assert _cache.manifest_versions() is None


class TestListsIncludeNewKey:
    def test_get_list(self, manifest_with_new_key):
        assert adk.get_list()[-1] == NEW_KEY
        assert adk.get_list(year=2999) == [NEW_KEY]

    def test_versions(self, manifest_with_new_key):
        assert adk.versions().tail(1) == [NEW_KEY]

    def test_stale_manifest_does_not_hide_snapshot(self, tmp_path, monkeypatch):
        # 오프라인 등으로 manifest 캐시가 패키지보다 오래된 경우: 스냅샷의 최신 시점이 살아 있어야 한다
        monkeypatch.setenv("ADMDONGKOR_DATA_DIR", str(tmp_path))
        _write_manifest(tmp_path, VERSIONS[:-1])
        assert adk.get_list() == VERSIONS


class TestValidationAcceptsNewKey:
    def test_check_version(self, manifest_with_new_key):
        api._check_version(NEW_KEY)  # raise 하지 않아야 함

    def test_get_passes_key_check(self, manifest_with_new_key):
        # 키 검사를 통과하면 그 다음의 level 검사에서 걸린다 → 다운로드 없이 확인
        with pytest.raises(ValueError, match="level must be"):
            adk.get(NEW_KEY, "bogus")

    def test_match_adm_passes_base_check(self, manifest_with_new_key):
        with pytest.raises(TypeError, match="target must be"):
            adk.match_adm(base=NEW_KEY, region="11", target=123)

    def test_still_rejects_truly_unknown(self, manifest_with_new_key):
        with pytest.raises(ValueError, match="unknown version key"):
            adk.get("19000101", "emd")
        with pytest.raises(ValueError, match="unknown base version key"):
            adk.match_adm(base="19000101", region="11", target=VERSIONS[-1])

    def test_rejects_new_key_without_manifest(self, tmp_path, monkeypatch):
        # manifest 가 없으면 예전과 같이 거부 (스냅샷만으로 판단)
        monkeypatch.setenv("ADMDONGKOR_DATA_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="unknown version key"):
            adk.get(NEW_KEY, "emd")
