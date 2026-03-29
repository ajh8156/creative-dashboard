# 나의 컨텍스트

## 역할
- 그로스/CRM 마케터 (바인드/애슬러, 3040 남성 패션 커머스)
- 주요 플랫폼: Braze, Airbridge, Amplitude, 카카오모먼트, 메타, 네이버 등 주요 매체

## 응답 원칙
- 한국어 응답, 마케팅/개발 용어는 영어 유지
- 코드에는 반드시 한국어 주석 포함
- 파일 생성 시 아래 폴더 구조 준수 (마케팅: data/, scripts/, reports/, docs/ / 커리어: career/)
- 작업 완료 후 "다음 단계" 1가지만 제안할 것
- 시작할 때는 바로 진행보다는 항상 계획을 설명할 것
- 말투는 항상 존댓말 사용

## 마케팅 전략 공통 컨텍스트
- 브랜드: 바인드(애슬러), 3040~50 남성 패션 커머스
- 운영 맥락 참조: `docs/SOP/performance-marketing.md` (채널 구조, OKR, 예산, 캠페인 분류)
- KPI 우선순위: ROAS > 구매 CVR > 구매 CPA
- 소재 원칙: CTR용(감성/비주얼) vs CVR용(사야 하는 이유) 구분
- 스프린트 리듬:
  - **주간**: 월~일 데이터 기준, 화요일에 전주 성과 리뷰 및 다음주 액션 도출
  - **데일리**: 매일 성과 파일 업로드 → 트렌드 이상 감지 및 즉시 대응용

## 이벤트 데이터 & 심화 전략 참조

전략 수립 / 분석 쿼리 작성이 필요할 때 아래 문서를 참조할 것.
파일을 통째로 읽지 말고, 목적에 맞는 파일만 선택해 읽을 것.

| 상황 | 참조 파일 |
|------|----------|
| 퍼널 분석, 이벤트 명/프로퍼티 확인 | `docs/taxonomy/event-taxonomy-core.md` |
| Athena 쿼리 작성, 채널 어트리뷰션, CRM 효과 측정 | `docs/taxonomy/analysis-cookbook.md` |
| 전체 218개 이벤트 검색 (개발/QA 수준 확인) | `docs/taxonomy/event-taxonomy.md` |
| 문서 신뢰도 확인, 미구현 이벤트 파악 | `docs/taxonomy/event-taxonomy-guide.md` |

### 심화 전략 작업 시 필수 확인 사항
- 구매 분석: `complete_order`에 product_id 없음 → `complete_order_items` 필수
- 채널 분류: `COALESCE(utm_source, utm_channel)` 사용 (utm_source만 쓰면 누락)
- 금액: `payment_amount`(실결제) vs `origin_price`(정가) — 약 70% 차이
- 첫 구매: `first_purchase`는 클라이언트 추정 → MySQL `orders_order`로 검증
- Athena 테이블: `"bind-event-logs".bind_event_log_compacted`
  - 필수 필터: `"group" = 'default' AND env = 'production'`

## 폴더 구조
```
joo-mkt/
├── CLAUDE.md
├── data/
│   ├── raw/                 # 원본 CSV (최신만 루트, 이전은 archive/)
│   │   └── archive/         # 이전 CSV 보관
│   └── processed/
│       ├── pipeline/        # 파이프라인 산출물
│       ├── dashboard/       # 대시보드용 (kakao_cleaned.csv, *.parquet)
│       └── adhoc/           # 일회성 분석 결과물
├── scripts/
│   ├── pipeline/            # 데일리/주간 반복 스크립트
│   ├── adhoc/               # 일회성 분석 스크립트
│   ├── api/                 # 데이터 수집 (fetch_airbridge)
│   └── apps/                # 대시보드 앱 (creative_view.py)
├── reports/
│   ├── weekly/              # 주간 성과 리포트
│   ├── creative/            # 소재 성과 리포트
│   └── adhoc/               # 이슈 분석 리포트
├── docs/
│   ├── taxonomy/            # 이벤트 정의 및 분석 가이드
│   ├── templates/           # 반복 사용 템플릿
│   └── SOP/                 # 운영 및 전략 가이드
└── career/                  # 커리어 관련 (마케팅 컨텍스트와 독립)
    ├── resume/              # 이력서
    ├── jd-analysis/         # JD 분석
    ├── interview-prep/      # 면접 준비
    └── direction/           # 커리어 방향성 메모
```
