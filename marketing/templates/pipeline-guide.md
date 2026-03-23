# 파이프라인 실행 가이드

> 마지막 업데이트: 2026-03-23

---

## 폴더 구조

```
joo-mkt/
├── marketing/
│   ├── raw-data/                # 원본 CSV (최신만 루트에, 이전은 archive/)
│   │   └── archive/             # 이전 CSV 보관
│   ├── scripts/
│   │   ├── pipeline/            # 매일 반복 스크립트
│   │   │   ├── preprocess-airbridge.py    # Step 1: 전처리
│   │   │   ├── generate-weekly-report.py  # Step 2a: 주간 리포트 생성
│   │   │   ├── generate-creative-report.py # Step 2b: 소재 리포트 생성
│   │   │   ├── creative_naming.py          # 소재명 정규화 유틸 (import용)
│   │   │   └── update_dashboard_data.py   # 대시보드 parquet 갱신
│   │   └── adhoc/               # 일회성 분석 스크립트
│   │       └── kakao_preprocess.py        # 대시보드용 CSV 전처리
│   ├── reports/
│   │   ├── weekly/              # 주간 성과 리포트 (W11, W12...)
│   │   └── creative/            # 소재 성과 리포트 (날짜별)
│   ├── dashboards/              # Streamlit 대시보드
│   │   └── creative_view.py     # localhost:8501
│   └── templates/               # 템플릿 및 이 가이드
│
└── outputs/processed/
    ├── pipeline/                # 파이프라인 산출물
    │   ├── weekly-total.csv
    │   ├── weekly-by-type.csv
    │   ├── weekly-general-detail.csv
    │   ├── weekly-adgroup.csv
    │   ├── creative-performance.csv
    │   ├── creative-elements.csv
    │   ├── weekly-insights-context.md    # LLM 인사이트용 요약
    │   └── creative-insights-context.md  # LLM 인사이트용 요약
    ├── dashboard/               # 대시보드 전용
    │   ├── kakao_cleaned.csv
    │   ├── kakao_excluded.csv
    │   └── kakao_dashboard_final_v4.parquet
    └── adhoc/                   # 일회성 분석 결과
```

---

## 상황별 실행 가이드

### 1. 주간 성과 리포트 업데이트

**언제:** 매주 or 매일 새 Airbridge CSV를 받았을 때

**사전 준비:**
- 새 CSV를 `marketing/raw-data/`에 넣기
- 이전 CSV는 `marketing/raw-data/archive/`로 이동

**Claude에게:**
```
"03-20 데이터로 주간 리포트 업데이트해줘"
```

**자동 실행 흐름:**

| Step | 무엇을 하나 | 산출물 |
|------|------------|--------|
| 1. 전처리 | `preprocess-airbridge.py --type weekly --date {날짜}` | `pipeline/weekly-*.csv` 4개 |
| 2. 리포트 생성 | `generate-weekly-report.py --date {날짜}` | `reports/weekly/weekly-performance-2026-W{주차}.md` |
| 3. 인사이트 | LLM이 `weekly-insights-context.md` 읽고 작성 | 리포트 내 Executive Summary + 섹션 6 완성 |

**확인:** `marketing/reports/weekly/` 에서 최신 리포트 열기

---

### 2. 소재 성과 리포트 업데이트

**언제:** 소재별 ROAS/CTR/피로도 분석이 필요할 때

**Claude에게:**
```
"03-20 데이터로 소재 리포트 업데이트해줘"
```

**자동 실행 흐름:**

| Step | 무엇을 하나 | 산출물 |
|------|------------|--------|
| 1. 전처리 | `preprocess-airbridge.py --type creative --date {날짜}` | `pipeline/creative-*.csv` 2개 |
| 2. 리포트 생성 | `generate-creative-report.py --date {날짜}` | `reports/creative/creative-performance-{날짜}.md` |
| 3. 인사이트 | LLM이 `creative-insights-context.md` 읽고 작성 | 리포트 내 Executive Summary + 섹션 5 완성 |

**확인:** `marketing/reports/creative/` 에서 최신 리포트 열기

---

### 3. Streamlit 대시보드 데이터 갱신

**언제:** localhost:8501 대시보드에 최신 날짜가 반영 안 될 때

**Claude에게:**
```
"대시보드 데이터 업데이트해줘"
```

**자동 실행 흐름:**

| Step | 무엇을 하나 | 산출물 |
|------|------------|--------|
| 1. CSV 전처리 | `adhoc/kakao_preprocess.py` | `dashboard/kakao_cleaned.csv` |
| 2. parquet 생성 | `pipeline/update_dashboard_data.py` | `dashboard/kakao_dashboard_final_v4.parquet` |
| 3. 반영 | 대시보드에서 **Rerun** 또는 **C키** | 즉시 반영 |

> **참고:** `kakao_preprocess.py`의 RAW_PATH가 하드코딩되어 있음.
> 새 CSV 파일명이 다르면 Claude에게 경로 업데이트를 요청하세요.

---

### 4. 전체 갱신 (주간 + 소재 + 대시보드)

**언제:** 새 CSV 받은 날, 한번에 다 돌리고 싶을 때

**Claude에게:**
```
"03-20 전체 업데이트해줘 (주간 + 소재 + 대시보드)"
```

→ 위 1~3을 순서대로 모두 실행

---

### 5. 일회성 분석

**언제:** 특정 기간/브랜드/캠페인에 대한 단발 분석이 필요할 때

**Claude에게 (자유 형식):**
```
"2월 PBTD 캠페인만 따로 분석해줘"
"nike 브랜드 3주간 추이 분석"
"UA Fail 그룹 상세 원인 분석"
```

→ 결과물은 `outputs/processed/adhoc/`에 저장

---

## 빠른 참조 치트시트

| 하고 싶은 것 | Claude에게 한 줄 |
|---|---|
| 주간 리포트 | `"{날짜} 데이터로 주간 리포트 업데이트"` |
| 소재 리포트 | `"{날짜} 데이터로 소재 리포트 업데이트"` |
| 대시보드 갱신 | `"대시보드 데이터 업데이트"` |
| 전체 갱신 | `"{날짜} 전체 업데이트 (주간+소재+대시보드)"` |
| CSV 교체 | 새 파일 → `raw-data/`, 이전 파일 → `raw-data/archive/` |
| 대시보드 캐시 초기화 | 대시보드 우측 상단 메뉴 → Clear cache, 또는 `C`키 |
| 소재 파이프라인 일괄 실행 (PS1) | `.\run-creative-pipeline.ps1 -date 2026-03-23` |

---

## 주의사항

- **CSV 파일명**: preprocess 스크립트는 `raw-data/`에서 가장 최신 CSV를 자동 탐색함. 단, `kakao_preprocess.py`(adhoc)는 하드코딩이므로 수동 확인 필요
- **비용 보정**: 2월은 /1.5 (무상캐시 50%), 3월~ /1.763 (무상캐시 50% + 리베이트 14.9%)
- **리포트 플레이스홀더**: `{{EXECUTIVE_SUMMARY}}`와 `{{INSIGHTS_AND_ACTIONS}}`가 남아있으면 LLM 인사이트가 아직 안 채워진 상태

---

## 변경 이력

| 날짜 | 변경 내용 |
|------|-----------|
| 2026-03-23 | 대시보드 포트 8503→8501 수정, `creative_naming.py` 추가, `run-creative-pipeline.ps1` 문서화, Changelog 섹션 신설 |
| 2026-03-20 | 최초 작성 |
