"""오류 신고용 GitHub 이슈 폼 prefill.

`report_issue()` 는 사용자가 호출하면 환경 정보가 자동 첨부된 GitHub 이슈 폼 URL 을
만들어 브라우저로 연다. title/body 본문은 비워둠 — 사용자가 GitHub 에디터에서
직접 작성하는 게 마크다운/이미지 첨부 등 UX 가 더 낫다.
라이브러리는 GitHub 토큰을 다루지 않으므로 자동 POST 는 하지 않는다.
"""

from __future__ import annotations

import platform
import urllib.parse
import webbrowser

ISSUE_BASE_URL = "https://github.com/vuski/admdongkor/issues/new"


def _collect_env() -> str:
    from . import __version__
    from ._cache import data_version

    return "\n".join([
        f"admdongkor: {__version__}",
        f"data_version: {data_version() or 'unknown'}",
        f"python: {platform.python_version()}",
        f"os: {platform.system()} {platform.release()}",
    ])


def _build_url() -> str:
    env = _collect_env()
    body_template = (
        "<!-- 무엇이 잘못되었는지 적어주세요. "
        "재현 코드와 기대값/실제값이 있으면 좋습니다. -->\n"
        "\n\n"
        "---\n"
        "```\n"
        f"{env}\n"
        "```\n"
    )
    qs = urllib.parse.urlencode({
        "body": body_template,
        "labels": "user-report,data",
    })
    return f"{ISSUE_BASE_URL}?{qs}"


def report_issue(*, open_browser: bool = True) -> str:
    """오류 신고용 GitHub 이슈 폼을 환경 정보가 채워진 채로 연다.

    GitHub 계정이 필요하다 (없으면 가입 후 사용).
    함수는 환경 정보(버전, OS, data_version)만 prefill 한다 — 제목/본문은
    브라우저 GitHub 에디터에서 직접 작성한다.

    Args:
        open_browser: True (기본) 면 기본 브라우저로 자동 오픈. False 면 URL 만 출력.
            CI/SSH 등 GUI 가 없는 환경에서는 False 로.

    Returns:
        생성된 GitHub 이슈 폼 URL.

    Examples:
        >>> import admdongkor as adk
        >>> adk.report_issue()
        # 브라우저에서 GitHub 이슈 폼이 prefill 된 채로 열림.
        # 사용자는 제목/본문을 작성하고 'Submit' 클릭.

        >>> adk.report_issue(open_browser=False)
        # URL 만 출력 (헤드리스 환경용).
    """
    url = _build_url()
    if open_browser:
        try:
            webbrowser.open(url)
            print(
                "브라우저에서 GitHub 이슈 폼을 열었습니다. "
                "내용을 작성하고 'Submit' 을 눌러주세요."
            )
        except Exception:
            print(f"브라우저 자동 오픈 실패. 반환된 URL 을 직접 열어주세요.")
    return url
