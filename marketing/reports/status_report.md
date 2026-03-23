# Project Status Report: Creative Growth Dashboard v4

이 보고서는 소재 분석 대시보드 고도화 작업의 현재 상태를 공유하기 위해 작성되었습니다. 클로드(또는 다른 AI 어시스턴트)에게 이 내용을 공유하여 협업을 이어가실 수 있습니다.

## 1. 프로젝트 개요 (Context)
- **목표**: 100MB 이상의 대용량 카카오 광고 데이터를 그로스 마케터 관점에서 정교하게 분석할 수 있는 **전문가용 소재 대시보드 (v4)** 구축.
- **핵심 기술**: Python, Streamlit, Pandas, NumPy, Plotly Express.
- **데이터 흐름**: [kakao_cleaned.csv](file:///c:/Users/ajh81/OneDrive/Desktop/joo-mkt/outputs/processed/kakao_cleaned.csv) (로데이터) → 전처리 및 분석 → `kakao_dashboard_final_v4.parquet` (초고속 캐시) → Streamlit UI.

## 2. 주요 구현 기능
- **5개 전문 분석 페이지**:
    1. **요약(Summary)**: 소재 인덱스(naming convention)별 승리 요소 도출 및 KPI 카드.
    2. **상세(Detailed)**: Monthly/Weekly/Daily 단위의 정밀 지표 테이블.
    3. **A/B 테스트**: 캠페인/그룹 단위 소재 선별 및 시계열(Daily Trend) 성과 비교.
    4. **데이터 사이언스(Lab)**: ROAS 하락 예측 모델(Linear Regression) 및 D-Day 예측, 소진-효율 버블 매트릭스.
    5. **메모장(History)**: 운영 이력(타겟 변경 등) 저장을 위한 Markdown 기반 기록 시스템.

- **데이터 엔진 고도화**:
    - **Smart Cache**: 원본 CSV가 업데이트되면 자동으로 감지하여 모든 전처리가 완료된 Parquet 파일을 생성, 로딩 속도를 초 단위로 단축.
    - **7대 지표 표준화**: ROAS, CTR, CPC, CPA, CVR, AOV, Funnel CVR(장바구니/옵션) 구현.

## 3. 핵심 파일 구조
- [marketing/dashboards/creative_view.py](file:///c:/Users/ajh81/OneDrive/Desktop/joo-mkt/marketing/dashboards/creative_view.py): 메인 대시보드 소스 코드.
- [outputs/processed/kakao_cleaned.csv](file:///c:/Users/ajh81/OneDrive/Desktop/joo-mkt/outputs/processed/kakao_cleaned.csv): 주간 리포트와 공용으로 사용하는 원본 데이터.
- `outputs/processed/kakao_dashboard_final_v4.parquet`: 대시보드 전용 초고속 캐시 파일.
- `marketing/dashboards/memo.md`: 대시보드 내 기록되는 운영 히스토리 파일.

## 4. 현재 상황 및 이슈 (Current Issues)
- **성능 최적화**: 100MB+ CSV 로딩 지연을 해결하기 위해 '전처리 완료된 Parquet 캐싱' 시스템을 막 도입한 상태입니다.
- **모듈/환경 이슈**: 사용자의 로컬 환경에서 `pyarrow`, `pandas`, `numpy` 등의 모듈 임포트 또는 Streamlit 구동 시 지연 혹은 에러가 발생하는 것으로 의심됩니다.
- **예측 모델 정교화**: `numpy.polyfit`을 이용한 단순 선형 회귀가 적용되어 있으며, 데이터가 충분하지 않을 경우(5일 미만) 예측이 제한됩니다.

## 5. 클로드에게 요청할 사항 (Suggested Prompt)
> "현재 Streamlit 기반 소재 대시보드 v4를 개발 중입니다. 첨부된 [creative_view.py](file:///c:/Users/ajh81/OneDrive/Desktop/joo-mkt/marketing/dashboards/creative_view.py)와 `status_report.md`를 참고하여, 로컬 환경에서 발생하는 모듈 에러를 디버깅하고 데이터 로딩 속도를 더 안정화할 수 있는 방안을 제안해 주세요. 특히 Parquet 캐시 시스템의 안정성과 예측 알고리즘의 예외 처리를 중점적으로 봐주길 바랍니다."
