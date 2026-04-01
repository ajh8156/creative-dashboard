"""
데일리 자동화 파이프라인 (월~금 오전 9시 자동 실행)

실행 순서:
  1. Airbridge API → CSV 최신 데이터 다운로드 → data/raw/
  2. 전처리 (preprocess-airbridge.py) → data/processed/pipeline/*.csv 4개
  3. 주간 리포트 생성 (generate-weekly-report.py)
  4. 소재 리포트 생성 (generate-creative-report.py)
  5. 대시보드 데이터 업데이트 (update_dashboard_data.py)
  6. 노션 자동 업로드 (주간 + 소재 리포트)

사용법:
  python scripts/pipeline/run-daily.py
  python scripts/pipeline/run-daily.py --skip-notion  # 노션 업로드 제외
"""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

# === 경로 설정 ===
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # joo-mkt/
SCRIPTS_DIR = BASE_DIR / "scripts"
REPORTS_DIR = BASE_DIR / "reports"
PIPELINE_DIR = SCRIPTS_DIR / "pipeline"
API_DIR = SCRIPTS_DIR / "api"

# === 로깅 ===
TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg, level="INFO"):
    """로그 출력"""
    print(f"[{TIMESTAMP}] [{level}] {msg}")


def run_command(cmd, description):
    """명령어 실행 및 에러 처리"""
    log(f"시작: {description}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        log(f"✅ 완료: {description}")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ 실패: {description}", level="ERROR")
        log(f"Error: {e.stderr}", level="ERROR")
        return False


def fetch_airbridge_data():
    """Step 1: Airbridge API에서 최신 데이터 다운로드"""
    # 어제 데이터 기준 (API는 1일 지연)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    cmd = [
        sys.executable,
        str(API_DIR / "fetch_airbridge.py"),
        "--start", yesterday,
        "--end", yesterday
    ]

    return run_command(cmd, f"Airbridge API 데이터 다운로드 ({yesterday})")


def preprocess_data():
    """Step 2: 데이터 전처리"""
    cmd = [
        sys.executable,
        str(PIPELINE_DIR / "preprocess-airbridge.py"),
        "--type", "weekly"
    ]

    return run_command(cmd, "데이터 전처리 (preprocess-airbridge.py)")


def generate_weekly_report():
    """Step 3: 주간 리포트 생성"""
    today = datetime.now().strftime("%Y-%m-%d")

    cmd = [
        sys.executable,
        str(PIPELINE_DIR / "generate-weekly-report.py"),
        "--date", today
    ]

    return run_command(cmd, "주간 리포트 생성 (generate-weekly-report.py)")


def generate_creative_report():
    """Step 4: 소재 리포트 생성"""
    today = datetime.now().strftime("%Y-%m-%d")

    cmd = [
        sys.executable,
        str(PIPELINE_DIR / "generate-creative-report.py"),
        "--date", today
    ]

    return run_command(cmd, "소재 리포트 생성 (generate-creative-report.py)")


def update_dashboard():
    """Step 5: 대시보드 데이터 업데이트"""
    cmd = [
        sys.executable,
        str(PIPELINE_DIR / "update_dashboard_data.py")
    ]

    return run_command(cmd, "대시보드 업데이트 (update_dashboard_data.py)")


def upload_to_notion():
    """Step 6: 노션에 자동 업로드"""
    log("노션 업로드 시작...")

    try:
        # 생성된 리포트 파일 찾기
        weekly_files = list(REPORTS_DIR.glob("weekly/weekly-performance-*.md"))
        creative_files = list(REPORTS_DIR.glob("creative/creative-performance-*.md"))

        if not weekly_files or not creative_files:
            log("⚠️ 리포트 파일을 찾을 수 없습니다.", level="WARN")
            return False

        # 최신 파일만 사용
        weekly_file = max(weekly_files, key=lambda x: x.stat().st_mtime)
        creative_file = max(creative_files, key=lambda x: x.stat().st_mtime)

        log(f"업로드 대상: {weekly_file.name}, {creative_file.name}")

        # 노션 업로드는 Claude 에이전트에서 처리
        # (이 스크립트에서는 파일 경로만 확인)
        log(f"✅ 노션 업로드 준비 완료")
        log(f"  - 주간 리포트: {weekly_file}")
        log(f"  - 소재 리포트: {creative_file}")

        return True

    except Exception as e:
        log(f"❌ 노션 업로드 실패: {str(e)}", level="ERROR")
        return False


def main():
    parser = argparse.ArgumentParser(description="데일리 자동화 파이프라인")
    parser.add_argument("--skip-notion", action="store_true", help="노션 업로드 제외")
    args = parser.parse_args()

    log("=" * 60)
    log("데일리 파이프라인 시작")
    log("=" * 60)

    # Step 1: Airbridge 데이터 다운로드
    if not fetch_airbridge_data():
        log("Airbridge 데이터 다운로드 실패 → 파이프라인 중단", level="ERROR")
        sys.exit(1)

    # Step 2: 전처리
    if not preprocess_data():
        log("전처리 실패 → 파이프라인 중단", level="ERROR")
        sys.exit(1)

    # Step 3: 주간 리포트
    if not generate_weekly_report():
        log("주간 리포트 생성 실패 → 계속 진행", level="WARN")

    # Step 4: 소재 리포트
    if not generate_creative_report():
        log("소재 리포트 생성 실패 → 계속 진행", level="WARN")

    # Step 5: 대시보드 업데이트
    if not update_dashboard():
        log("대시보드 업데이트 실패 → 계속 진행", level="WARN")

    # Step 6: 노션 업로드
    if not args.skip_notion:
        upload_to_notion()

    log("=" * 60)
    log("✅ 데일리 파이프라인 완료")
    log("=" * 60)


if __name__ == "__main__":
    main()
