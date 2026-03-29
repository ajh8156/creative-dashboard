# 소재 리포트 및 대시보드 통합 실행 스크립트
# 사용법: .\run-creative-pipeline.ps1 -date 2026-03-18

param (
    [string]$date = (Get-Date).ToString("yyyy-MM-dd")
)

Write-Host "--- 1. [Step 1] 소재 데이터 전처리 시작 ($date) ---" -ForegroundColor Cyan
python scripts/pipeline/preprocess-airbridge.py --type creative --date $date

Write-Host "--- 2. [Step 2] 소재 리포트 및 컨텍스트 생성 시작 ---" -ForegroundColor Cyan
python scripts/pipeline/generate-creative-report.py --date $date

Write-Host "--- 3. [Step 3] 대시보드 데이터 가공 시작 (Parquet) ---" -ForegroundColor Cyan
python scripts/pipeline/update_dashboard_data.py

Write-Host "--- 4. [Step 4] GitHub 자동 배포 (Git Push) ---" -ForegroundColor Cyan
git add .
git commit -m "Auto-update: creative data & reports ($date)"
git push

Write-Host "--- 작업 완료 및 배포 성공! ---" -ForegroundColor Green
Write-Host "리포트: reports/creative/creative-performance-$date.md"
Write-Host "대시보드: https://share.streamlit.io/ (배포 URL 확인)"
