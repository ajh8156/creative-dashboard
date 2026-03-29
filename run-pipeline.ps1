# ============================================================
# 통합 파이프라인 자동화 스크립트
# 사용법:
#   .\run-pipeline.ps1              → 오늘 날짜 기준
#   .\run-pipeline.ps1 -date 2026-03-28  → 날짜 지정
# ============================================================

param (
    [string]$date = (Get-Date).ToString("yyyy-MM-dd")
)

$ErrorActionPreference = "Stop"  # 오류 발생 시 즉시 중단

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host " 애슬러 마케팅 파이프라인 시작 ($date)" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan

# ----------------------------------------------------------
# Step 1. kakao_cleaned.csv 생성 (대시보드 원본)
# ----------------------------------------------------------
Write-Host ""
Write-Host "[1/6] 카카오 raw 데이터 전처리 (kakao_cleaned.csv)..." -ForegroundColor Yellow
python scripts/adhoc/kakao_preprocess.py
if ($LASTEXITCODE -ne 0) { Write-Host "오류: Step 1 실패" -ForegroundColor Red; exit 1 }

# ----------------------------------------------------------
# Step 2. 주간 집계 CSV 생성
# ----------------------------------------------------------
Write-Host ""
Write-Host "[2/6] 주간 전처리 (weekly CSV 4종)..." -ForegroundColor Yellow
python scripts/pipeline/preprocess-airbridge.py --type weekly --date $date
if ($LASTEXITCODE -ne 0) { Write-Host "오류: Step 2 실패" -ForegroundColor Red; exit 1 }

# ----------------------------------------------------------
# Step 3. 소재 집계 CSV 생성
# ----------------------------------------------------------
Write-Host ""
Write-Host "[3/6] 소재 전처리 (creative CSV)..." -ForegroundColor Yellow
python scripts/pipeline/preprocess-airbridge.py --type creative --date $date
if ($LASTEXITCODE -ne 0) { Write-Host "오류: Step 3 실패" -ForegroundColor Red; exit 1 }

# ----------------------------------------------------------
# Step 4. 주간 리포트 + 소재 리포트 생성
# ----------------------------------------------------------
Write-Host ""
Write-Host "[4/6] 리포트 생성 (주간 + 소재)..." -ForegroundColor Yellow
python scripts/pipeline/generate-weekly-report.py --date $date
if ($LASTEXITCODE -ne 0) { Write-Host "오류: 주간 리포트 생성 실패" -ForegroundColor Red; exit 1 }
python scripts/pipeline/generate-creative-report.py --date $date
if ($LASTEXITCODE -ne 0) { Write-Host "오류: 소재 리포트 생성 실패" -ForegroundColor Red; exit 1 }

# ----------------------------------------------------------
# Step 5. 대시보드용 Parquet 최신화
# ----------------------------------------------------------
Write-Host ""
Write-Host "[5/6] 대시보드 Parquet 최신화..." -ForegroundColor Yellow
python scripts/pipeline/update_dashboard_data.py
if ($LASTEXITCODE -ne 0) { Write-Host "오류: Step 5 실패" -ForegroundColor Red; exit 1 }

# ----------------------------------------------------------
# Step 6. GitHub Push → Streamlit Cloud 자동 배포
# ----------------------------------------------------------
Write-Host ""
Write-Host "[6/6] GitHub Push (Streamlit Cloud 자동 배포)..." -ForegroundColor Yellow

# 대시보드 데이터만 커밋 (raw 데이터, 개인 파일 제외)
git add data/processed/dashboard/kakao_dashboard_final_v4.parquet

# 변경사항 있을 때만 커밋
$status = git status --porcelain
if ($status) {
    git commit -m "data: 대시보드 데이터 최신화 ($date)"
    git push origin main
    Write-Host ""
    Write-Host "======================================================" -ForegroundColor Green
    Write-Host " 완료! Streamlit Cloud 재배포 시작됨 (약 1분 소요)" -ForegroundColor Green
    Write-Host "======================================================" -ForegroundColor Green
} else {
    Write-Host "변경사항 없음 — Push 생략" -ForegroundColor Gray
    Write-Host ""
    Write-Host "======================================================" -ForegroundColor Green
    Write-Host " 완료! (데이터 변경 없어 Push 생략)" -ForegroundColor Green
    Write-Host "======================================================" -ForegroundColor Green
}

Write-Host ""
Write-Host "산출물 확인:"
Write-Host "  - 주간 리포트 : reports/weekly/"
Write-Host "  - 소재 리포트 : reports/creative/creative-performance-$date.md"
Write-Host "  - 대시보드    : https://athler-creative-dashboard.streamlit.app"
Write-Host ""
