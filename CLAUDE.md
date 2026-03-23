# 나의 컨텍스트

## 역할
- 그로스/CRM 마케터 (바인드/애슬러, 3040 남성 패션 커머스)
- 주요 플랫폼: Braze, Airbridge, Amplitude, 카카오모먼트, 메타, 네이버 등 주요 매체

## 응답 원칙
- 한국어 응답, 마케팅/개발 용어는 영어 유지
- 코드에는 반드시 한국어 주석 포함
- 파일 생성 시 marketing/, career/ 폴더 구조 준수
- 작업 완료 후 "다음 단계" 1가지만 제안할 것
- 시작할 때는 바로 진행보다는 항상 계획을 설명할 것

## 마케팅 전략 공통 컨텍스트
- 브랜드: 바인드(애슬러), 3040~50 남성 패션 커머스
- 운영 맥락 참조: `marketing/performance-marketing.md` (채널 구조, OKR, 예산, 캠페인 분류)
- KPI 우선순위: CVR > ROAS > 구매 전환율
- 소재 원칙: CTR용(감성/비주얼) vs CVR용(사야 하는 이유) 구분
- 스프린트: 매주 직전주 성과 기반 → 다음주 액션 도출

## 폴더 구조
```
joo-mkt/
├── CLAUDE.md
├── .claude/
│   └── commands/            # 슬래시 커맨드 md 파일들
├── bootcamp/                # 부트캠프 관련 자료
├── career/
│   ├── resume/              # 경력기술서 버전 관리
│   ├── jd-analysis/         # 타겟 JD 수집 및 분석
│   └── interview-prep/      # 면접 준비 자료
├── marketing/
│   ├── campaigns/           # 캠페인 기획 및 결과
│   ├── dashboards/          # Streamlit 대시보드 (creative_view.py)
│   ├── raw-data/            # 원본 CSV (최신만 루트, 이전은 archive/)
│   │   └── archive/         # 이전 CSV 보관
│   ├── reports/
│   │   ├── weekly/          # 주간 성과 리포트
│   │   └── creative/        # 소재 성과 리포트
│   ├── scripts/
│   │   ├── pipeline/        # 데일리 반복 스크립트 (preprocess, generate, update)
│   │   └── adhoc/           # 일회성 분석 스크립트
│   └── templates/           # 반복 사용 템플릿
└── outputs/processed/
    ├── pipeline/            # 파이프라인 산출물 (weekly-*.csv, creative-*.csv, *-context.md)
    ├── dashboard/           # 대시보드용 (kakao_cleaned.csv, *.parquet)
    └── adhoc/               # 일회성 분석 결과
```
