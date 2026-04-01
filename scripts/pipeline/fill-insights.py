"""
주간 리포트 인사이트 자동 작성 (Claude API)

프로세스:
  1. weekly-insights-context.md 읽기
  2. insight-prompt-weekly.md 프롬프트 템플릿 읽기
  3. Claude API 호출 → {{EXECUTIVE_SUMMARY}} + {{INSIGHTS_AND_ACTIONS}} 생성
  4. 원본 리포트 파일 업데이트

사용법:
  python scripts/pipeline/fill-insights.py --report <리포트경로>
  python scripts/pipeline/fill-insights.py --latest  # 최신 리포트 자동 선택
"""

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv

# === 경로 설정 ===
BASE_DIR = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = BASE_DIR / "reports" / "weekly"
PIPELINE_DIR = BASE_DIR / "data" / "processed" / "pipeline"
TEMPLATES_DIR = BASE_DIR / "docs" / "templates"
API_DIR = BASE_DIR / "scripts" / "api"

# .env 로드
load_dotenv(API_DIR / ".env")


def log(msg, level="INFO"):
    """로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")


def get_latest_report():
    """최신 주간 리포트 파일 찾기"""
    files = list(REPORTS_DIR.glob("weekly-performance-*.md"))
    if not files:
        log("리포트 파일을 찾을 수 없습니다.", level="ERROR")
        sys.exit(1)
    return max(files, key=lambda x: x.stat().st_mtime)


def read_context(context_file):
    """insights-context.md 읽기"""
    if not context_file.exists():
        log(f"Context 파일을 찾을 수 없습니다: {context_file}", level="ERROR")
        sys.exit(1)

    with open(context_file, "r", encoding="utf-8") as f:
        return f.read()


def read_prompt_template(template_file):
    """프롬프트 템플릿 읽기"""
    if not template_file.exists():
        log(f"템플릿을 찾을 수 없습니다: {template_file}", level="ERROR")
        sys.exit(1)

    with open(template_file, "r", encoding="utf-8") as f:
        return f.read()


def call_claude_api(context, prompt_template):
    """Claude API 호출하여 인사이트 생성"""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log("ANTHROPIC_API_KEY를 찾을 수 없습니다.", level="ERROR")
        sys.exit(1)

    client = Anthropic(api_key=api_key)

    # 사용자 프롬프트 구성
    user_message = f"""{prompt_template}

---

## 입력 데이터 (분석 대상)

{context}

---

**작업**: 위 입력 데이터를 바탕으로:

1. **Executive Summary** (8줄 이내) 작성
   - {{EXECUTIVE_SUMMARY}} 영역에 들어갈 내용
   - 형식은 프롬프트의 "출력 1" 섹션 참고

2. **섹션 6: 인사이트 & 액션** (6-1 ~ 6-6) 작성
   - {{INSIGHTS_AND_ACTIONS}} 영역에 들어갈 내용
   - 형식은 프롬프트의 "출력 2" 섹션 참고

**출력 형식:**

```
# {{EXECUTIVE_SUMMARY}} 내용 (여기부터 # 로 시작)

## 6. 인사이트 & 액션

### 6-1. 일반 기획전 추이 요약 (핵심)
...
```

{{}}를 포함하지 마세요. 위 형식 그대로 마크다운으로 작성하세요.
"""

    log("Claude API 호출 중...")

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",  # Haiku: 단순 데이터 기반 작성용
        max_tokens=3000,
        messages=[
            {
                "role": "user",
                "content": user_message,
            }
        ],
    )

    response_text = message.content[0].text
    log("✅ Claude 응답 수신")

    return response_text


def extract_sections(claude_response):
    """Claude 응답에서 Executive Summary와 Insights & Actions 추출"""
    # Executive Summary 추출 (# 로 시작하는 첫 번째 헤딩 ~ 다음 헤딩 전까지)
    exec_match = re.search(
        r"^(## Executive Summary\n.*?)(?=\n## |\n# |\Z)",
        claude_response,
        re.MULTILINE | re.DOTALL,
    )

    # Insights & Actions 추출 (## 6. 인사이트 & 액션 이하)
    insights_match = re.search(
        r"^(## 6\. 인사이트 & 액션.*)",
        claude_response,
        re.MULTILINE | re.DOTALL,
    )

    exec_summary = exec_match.group(1) if exec_match else ""
    insights = insights_match.group(1) if insights_match else ""

    # 만약 "# Executive Summary" 형식이면 "## "로 정규화
    if exec_summary.startswith("# Executive Summary"):
        exec_summary = exec_summary.replace("# Executive Summary", "## Executive Summary", 1)

    return exec_summary, insights


def update_report(report_path, exec_summary, insights):
    """원본 리포트 파일 업데이트"""
    with open(report_path, "r", encoding="utf-8") as f:
        content = f.read()

    # {{EXECUTIVE_SUMMARY}} 교체
    if "{{EXECUTIVE_SUMMARY}}" in content:
        content = content.replace("{{EXECUTIVE_SUMMARY}}", exec_summary)
        log("✅ Executive Summary 교체 완료")
    else:
        log("⚠️  {{EXECUTIVE_SUMMARY}} 플레이스홀더를 찾을 수 없습니다.", level="WARN")

    # {{INSIGHTS_AND_ACTIONS}} 교체
    if "{{INSIGHTS_AND_ACTIONS}}" in content:
        content = content.replace("{{INSIGHTS_AND_ACTIONS}}", insights)
        log("✅ Insights & Actions 교체 완료")
    else:
        log("⚠️  {{INSIGHTS_AND_ACTIONS}} 플레이스홀더를 찾을 수 없습니다.", level="WARN")

    # 파일 저장
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)

    log(f"✅ 리포트 업데이트 완료: {report_path.name}")


def main():
    parser = argparse.ArgumentParser(description="리포트 인사이트 자동 작성 (주간/소재)")
    parser.add_argument(
        "--report-type",
        choices=["weekly", "creative"],
        default="weekly",
        help="리포트 유형 (기본값: weekly)",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="리포트 파일 경로 (미지정 시 최신 파일 자동 선택)",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="최신 리포트 자동 선택",
    )
    args = parser.parse_args()

    # 리포트 유형별 경로 및 템플릿 설정
    report_dir = REPORTS_DIR if args.report_type == "weekly" else (BASE_DIR / "reports" / "creative")

    if args.report_type == "weekly":
        context_file = PIPELINE_DIR / "weekly-insights-context.md"
        template_file = TEMPLATES_DIR / "insight-prompt-weekly.md"
    else:
        context_file = PIPELINE_DIR / "creative-insights-context.md"
        template_file = TEMPLATES_DIR / "insight-prompt-creative.md"

    # 리포트 파일 결정
    if args.report:
        report_path = args.report
        if not report_path.exists():
            log(f"파일을 찾을 수 없습니다: {report_path}", level="ERROR")
            sys.exit(1)
    else:
        files = list(report_dir.glob("*.md"))
        files = [f for f in files if f.name.startswith(
            ("weekly-performance", "creative-performance")
        )]
        if not files:
            log(f"{args.report_type} 리포트 파일을 찾을 수 없습니다.", level="ERROR")
            sys.exit(1)
        report_path = max(files, key=lambda x: x.stat().st_mtime)

    log(f"대상 리포트: {report_path.name}")
    log(f"Context 파일: {context_file.name}")
    log(f"Template: {template_file.name}")

    # 1. Context와 Template 읽기
    log("데이터 준비 중...")
    context = read_context(context_file)
    prompt_template = read_prompt_template(template_file)

    # 2. Claude API 호출
    claude_response = call_claude_api(context, prompt_template)

    # 3. 응답에서 섹션 추출
    log("응답 파싱 중...")
    exec_summary, insights = extract_sections(claude_response)

    if not exec_summary or not insights:
        log("응답에서 필요한 섹션을 찾을 수 없습니다.", level="ERROR")
        log(f"Claude 응답:\n{claude_response}")
        sys.exit(1)

    # 4. 원본 리포트 업데이트
    update_report(report_path, exec_summary, insights)

    log("=" * 60)
    log("✅ 인사이트 자동 작성 완료")
    log("=" * 60)


if __name__ == "__main__":
    main()
