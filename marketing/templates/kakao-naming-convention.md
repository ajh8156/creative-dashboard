# 카카오 네이밍 컨벤션 정의서

> **최종 업데이트**: 2026-03-22
> **용도**: CSV 전처리, 캠페인 파싱, 소재 분류 자동화의 기준 문서
> **참조**: `marketing/performance-marketing.md`

---

## 1. 캠페인명 컨벤션

### 구조

```
{지면}_{소재유형}_{랜딩}_{구분}-{타겟유형}-{목표}
```

### 각 항목 정의

| 순서 | 항목 | 구분자 | 값 | 설명 |
|---|---|---|---|---|
| 1 | 지면 | `_` | `bizboard` / `display` | 비즈보드(채팅탭 상단) / 디스플레이 |
| 2 | 소재유형 | `_` | `da` | DA (Display Ad) |
| 3 | 랜딩 | `_` | `pr` / `pdp` | 프로모션 페이지 / 상품상세페이지 |
| 4 | 구분 | `_` → `-` | `(없음)` / `sel` / `pbtd` / `ad_pbtd` | 일반 / 셀렉션 / PB직매입 / AD-PBTD(브랜드 외부 광고) |
| 5 | 타겟유형 | `-` | `retarget` / `ua` | 리타겟팅 / 신규유저획득 |
| 6 | 목표 | `-` | `purchase` / `traffic` | 구매 / 트래픽 |

### 현재 운영 캠페인 목록 (21개)

| 캠페인명 | 지면 | 랜딩 | 구분 | 타겟 | 목표 |
|---|---|---|---|---|---|
| `bizboard_da_pr-retarget-purchase` | 비즈보드 | PR | 일반 | RT | 구매 |
| `bizboard_da_pr-ua-purchase` | 비즈보드 | PR | 일반 | UA | 구매 |
| `bizboard_da_pr_pbtd-retarget-purchase` | 비즈보드 | PR | PBTD | RT | 구매 |
| `bizboard_da_pr_pbtd-ua-purchase` | 비즈보드 | PR | PBTD | UA | 구매 |
| `bizboard_da_pr_sel-retarget-purchase` | 비즈보드 | PR | 셀렉션 | RT | 구매 |
| `bizboard_da_pr_sel-ua-purchase` | 비즈보드 | PR | 셀렉션 | UA | 구매 |
| `bizboard_da_pr_ad_pbtd-retarget-purchase` | 비즈보드 | PR | AD-PBTD | RT | 구매 |
| `bizboard_da_pdp-retarget-purchase` | 비즈보드 | PDP | 일반 | RT | 구매 |
| `bizboard_da_pdp-ua-purchase` | 비즈보드 | PDP | 일반 | UA | 구매 |
| `bizboard_da_pr-ua-traffic` | 비즈보드 | PR | 일반 | UA | 트래픽 |
| `display_da_pr-retarget-purchase` | 디스플레이 | PR | 일반 | RT | 구매 |
| `display_da_pr-ua-purchase` | 디스플레이 | PR | 일반 | UA | 구매 |
| `display_da_pr_pbtd-retarget-purchase` | 디스플레이 | PR | PBTD | RT | 구매 |
| `display_da_pr_sel-retarget-purchase` | 디스플레이 | PR | 셀렉션 | RT | 구매 |
| `display_da_pr_sel-ua-purchase` | 디스플레이 | PR | 셀렉션 | UA | 구매 |
| `display_da_pdp-retarget-purchase` | 디스플레이 | PDP | 일반 | RT | 구매 |
| `display_da_pdp-ua-purchase` | 디스플레이 | PDP | 일반 | UA | 구매 |
| `display_da_pr-retarget-traffic` | 디스플레이 | PR | 일반 | RT | 트래픽 |
| `display_da_pr-ua-traffic` | 디스플레이 | PR | 일반 | UA | 트래픽 |
| `kakaoDisplay_conversion_catalog` | 카카오디스플레이 | - | 카탈로그 | RT | 구매 |

> **참고**: `kakaoDisplay_conversion_catalog`는 컨벤션 예외이나, 구매자 77.8%가 기존유저(90일+)이므로 RT로 분류. 캠페인명으로 자연 분리 가능.

### 캠페인 유형 분류 (최종)

| 유형 | 캠페인 구분 | 목표 | 포함 캠페인 |
|---|---|---|---|
| **일반-UA** | 일반 (기획전) | 구매 | `*_da_pr-ua-purchase`, `*_da_pdp-ua-purchase` |
| **일반-RT** | 일반 (기획전) | 구매 | `*_da_pr-retarget-purchase`, `*_da_pdp-retarget-purchase` |
| **일반-UA-Traffic** | 일반 (기획전) | 트래픽 | `*_da_pr-ua-traffic` |
| **일반-RT-Traffic** | 일반 (기획전) | 트래픽 | `*_da_pr-retarget-traffic` |
| **SEL-UA** | 셀렉션 | 구매 | `*_da_pr_sel-ua-purchase` |
| **SEL-RT** | 셀렉션 | 구매 | `*_da_pr_sel-retarget-purchase` |
| **PBTD-UA** | PB/직매입 | 구매 | `*_da_pr_pbtd-ua-purchase` |
| **PBTD-RT** | PB/직매입 | 구매 | `*_da_pr_pbtd-retarget-purchase` |
| **AD-PBTD** | 브랜드 외부 광고 | 구매 | `*_da_pr_ad_pbtd-retarget-purchase` |
| **카탈로그-RT** | 카탈로그 | 구매 | `kakaoDisplay_conversion_catalog` |

> **PDP 캠페인** (`*_da_pdp-*`)은 랜딩만 다를 뿐 일반과 동일 유형으로 분류.
> 트래픽 캠페인은 일반 내에서 목표로 구분 (셀렉션/PBTD에는 트래픽 목표 없음).

### 예외 캠페인 (제외 대상)

| 캠페인명 | 사유 | 처리 |
|---|---|---|
| `bizboard_conversion_discount` | 미운영 (타임특가 → PDP 이관) | 제외 |
| `bizboard_conversion_appinstall` | 과거 앱설치 | 제외 |
| `bizboard_conversion_promotion` | 과거 컨벤션 | 제외 |
| `Conversion_Catalog_wBrands` | 과거 카탈로그 | 제외 |
| `conversion_display` | 과거 컨벤션 | 제외 |
| `display_conversion` | 과거 컨벤션 | 제외 |
| `Display_conversion_promotion` | 과거 컨벤션 | 제외 |
| `male4059-bigluck_br_fahrenheit-promotion` | 과거 컨벤션 (지면 누락) | 제외 |
| `message-retention` | CRM (비광고) | 제외 |
| `*,*` (콤마 중복) | Airbridge 데이터 오류 | 정상 캠페인명으로 정제 |
| `*-retarget-purchase-retarget-purchase` | 네이밍 오류 (중복) | 정상 캠페인명으로 정제 |

---

## 2. 광고그룹명 컨벤션

### 구조

```
{성별}{연령대}-{YYMM}_{타겟구분}_{브랜드/카테고리명}-{프로모션유형}
```

### 각 항목 정의

| 순서 | 항목 | 값 예시 | 설명 |
|---|---|---|---|
| 1 | 성별+연령대 | `male3564`, `female2564` | 타겟 성별 + 연령 범위 |
| 2 | YYMM | `2601`, `2602` | 소재/광고그룹 생성 시기 |
| 3 | 타겟구분 | `br` / `ct` / `focus` / `custom` | 브랜드 / 카테고리 / 포커스 / 커스텀 |
| 4 | 브랜드명 | `stoneisland`, `calvinklein` | 해당 브랜드/카테고리명 |
| 5 | 프로모션유형 | `promotion`, `outlet` | 프로모션 형태 |

---

## 3. 소재명 컨벤션 (확정본)

### 구조

```
{yymmdd}-{creative_type}-{creative_version}-{option_main}_{option_detailed}_{numbering}_{size/배열}
```

### 각 항목 정의

| 순서 | 항목 | 값 | 설명 |
|---|---|---|---|
| 1 | **yymmdd** | `260211` 등 | 소재 Live 일정 기준 |
| 2 | **creative_type** | `img` / `vid` / `msg` / `carouselfeed` / `carouselcommerce` / `widelist` / `wideimage` | 소재 형태 |
| 3 | **creative_version** | `single` / `dynamic` / `carousel` / `catalog` / `seeding` | 소재 버전 |
| 4 | **option_main** | `log` / `sku` / `inf` / `txt` / `prm` | 메인 오브젝트 유형 |
| 5 | **option_detailed** | 아래 표 참조 | 상세 구분 |
| 6 | **numbering** | `1`, `2`, `1a`, `1b` 등 | 소재 단위 구분 |
| 7 | **size/배열** | `11`(1080²) / `21`(1200x600) / `34`(960x1200) / `169`(1080x1920) / `l` / `c` / `r` | 카카오 디스플레이 사이즈 또는 오브제 배열 |

### option_detailed 상세

| 값 | 의미 | 통합 대상 |
|---|---|---|
| `n` | none (정보 불필요) | - |
| `one` | 상품 1개 강조 | one / single / sku 통합 |
| `multi` | 복수 상품 강조 | two / three / triple / 3p / four / multi2~7 통합 |
| `cat` | 카테고리 특화 | jacket / coat / knit / shirts / shirt / pants / dressshoes / shoes / wallabee 통합 |
| `set` | 세트 구성 강조 | 1plus1 / set / mix / oneplusone 통합 |
| `brand` | 브랜드 로고/화보 | disney / wolsey / logo 통합 |
| `model` | 모델 강조 | 사람 모델 포함 시 (main 무관) |
| `pho` | 화보형 강조 | 화보 형태 중점 시 (photo 통합) |
| `sale` | 할인가 강조 | discount / disc / dis / price 통합 |
| `cp` | 쿠폰 강조 | 쿠폰 메리트 강조 시 |

### size/배열 상세 (카카오 한정)

| 값 | 의미 | 용도 |
|---|---|---|
| `11` | 1080x1080 | 카카오 디스플레이 사이즈 구분 |
| `21` | 1200x600 | 카카오 디스플레이 사이즈 구분 |
| `34` | 960x1200 | 카카오 디스플레이 사이즈 구분 |
| `169` | 1080x1920 | 카카오 디스플레이 사이즈 구분 |
| `l` | left | 비즈보드 오브제 배열 |
| `c` | center | 비즈보드 오브제 배열 |
| `r` | right | 비즈보드 오브제 배열 |

---

### saletap (타임특가) 전환 가이드

> saletap은 비즈보드 PDP RT 전용 기획전 소재. 기존명은 자동 파싱(`prm`/`sale`, `idx_type=img`)으로 호환.

| 기존 패턴 | 신규 컨벤션 | 비고 |
|---|---|---|
| `{YYMMDD}_saletap` | `{YYMMDD}-img-single-prm_sale_{n}` | 이미지/단일/프로모션/할인 |
| `{YYMMDD}_saletap-{n}` | `{YYMMDD}-img-single-prm_sale_{n}` | 넘버링 유지 |
| `{YYMMDD}_saletap_{size}` | `{YYMMDD}-img-single-prm_sale_{n}` | size 생략 (비즈보드 전용) |

---

## 변경 이력

| 날짜 | 변경 내용 |
|---|---|
| 2026-03-17 | 초안 작성. 캠페인/광고그룹/소재 컨벤션 정의. 예외 캠페인 목록 정리 |
| 2026-03-17 | 카탈로그 → RT 분류 확정. AD-PBTD 별도 유형 분리. 캠페인 유형 분류표 추가 |
| 2026-03-22 | option_detailed 통합 대상 정비: one(+single/wallabee/sku), cat(+shirt/dressshoes/shoes), brand(-wallabee, +logo), sale(+dis), pho(+photo). 엑셀 260320 기준 동기화 |
| 2026-03-23 | saletap 전환 가이드 추가. 자동 파싱: saletap → prm/sale/img 매핑 |
