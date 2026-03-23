"""
카카오 Airbridge 로데이터 전처리 스크립트
- 예외 캠페인 제거, 캠페인명 정제, 유형 분류, 비용 보정
- 참조: marketing/templates/kakao-naming-convention.md
"""

import pandas as pd
import os

# === 경로 설정 ===
# marketing/scripts/adhoc/ → 4단계 상위 = joo-mkt/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
RAW_PATH = os.path.join(BASE_DIR, "marketing", "raw-data",
    "athler_(Kai)_Report_Raw_new_2026-02-01_2026-03-22_Asia_Seoul_KST.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "processed", "dashboard")

# === 1. 예외 캠페인 목록 ===
EXCLUDED_CAMPAIGNS = [
    "bizboard_conversion_discount",
    "bizboard_conversion_appinstall",
    "bizboard_conversion_promotion",
    "Conversion_Catalog_wBrands",
    "conversion_display",
    "display_conversion",
    "Display_conversion_promotion",
    "male4059-bigluck_br_fahrenheit-promotion",
    "message-retention",
]

# === 2. 비용 보정 계수 (카카오) ===
# 2월: ÷1.5 (무상캐시 50%)
# 3월~: ÷1.763 (무상캐시 50% + 현금 리베이트 14.9%)
COST_FACTOR_FEB = 1 / 1.5       # 0.667
COST_FACTOR_MAR = 1 / 1.763     # 0.567

# === 3. 캠페인 유형 분류 함수 ===
def classify_campaign_type(campaign) -> str:
    """캠페인명 → 유형 분류 (10개 유형)"""
    if not isinstance(campaign, str):
        return "미분류"
    # 카탈로그 (컨벤션 예외, RT로 분류)
    if "catalog" in campaign.lower():
        return "카탈로그-RT"
    # AD-PBTD (브랜드 외부 광고) — pbtd보다 먼저 체크
    if "ad_pbtd" in campaign:
        return "AD-PBTD"
    # PBTD
    if "_pbtd-" in campaign:
        if "-ua-" in campaign:
            return "PBTD-UA"
        return "PBTD-RT"
    # 셀렉션
    if "_sel-" in campaign:
        if "-ua-" in campaign:
            return "SEL-UA"
        return "SEL-RT"
    # 일반 (기획전) — 트래픽/구매 구분
    if "-traffic" in campaign:
        if "-ua-" in campaign:
            return "일반-UA-Traffic"
        return "일반-RT-Traffic"
    if "-ua-" in campaign:
        return "일반-UA"
    if "-retarget-" in campaign:
        return "일반-RT"
    return "미분류"


def parse_placement(campaign) -> str:
    """캠페인명 → 지면 추출"""
    if not isinstance(campaign, str):
        return "기타"
    if campaign.startswith("bizboard"):
        return "비즈보드"
    elif campaign.startswith("display"):
        return "디스플레이"
    elif campaign.startswith("kakaoDisplay"):
        return "카카오디스플레이"
    return "기타"


def parse_landing(campaign) -> str:
    """캠페인명 → 랜딩 추출"""
    if not isinstance(campaign, str):
        return "-"
    if "_pdp-" in campaign or "_pdp_" in campaign:
        return "PDP"
    elif "_pr-" in campaign or "_pr_" in campaign:
        return "PR"
    return "-"


def main():
    print("=" * 60)
    print("카카오 Airbridge 로데이터 전처리")
    print("=" * 60)

    # === 데이터 로드 ===
    df = pd.read_csv(RAW_PATH, encoding="utf-8-sig")
    print(f"\n[로드] 전체 행: {len(df):,}")

    # 컬럼명 정리
    df.columns = df.columns.str.strip().str.strip('"')

    # === STEP 1: 캠페인명 정제 ===
    # 콤마 중복 제거 (예: "name,name" → "name")
    df["Campaign"] = df["Campaign"].apply(
        lambda x: x.split(",")[0] if isinstance(x, str) and "," in x else x
    )
    # retarget 중복 제거 (예: "-retarget-purchase-retarget-purchase")
    df["Campaign"] = df["Campaign"].str.replace(
        r"-retarget-purchase-retarget-purchase$", "-retarget-purchase", regex=True
    )
    # 헤더 행 제거
    df = df[df["Campaign"] != "Campaign"].copy()
    print(f"[정제] 캠페인명 정제 후: {len(df):,}")

    # === STEP 2: 예외 캠페인 분리 ===
    mask_excluded = df["Campaign"].isin(EXCLUDED_CAMPAIGNS)
    df_excluded = df[mask_excluded].copy()
    df = df[~mask_excluded].copy()
    print(f"[분리] 예외 캠페인 제거: {len(df_excluded):,}행 제외 → 남은 행: {len(df):,}")

    # === STEP 3: 파싱 컬럼 추가 ===
    df["캠페인유형"] = df["Campaign"].apply(classify_campaign_type)
    df["지면"] = df["Campaign"].apply(parse_placement)
    df["랜딩"] = df["Campaign"].apply(parse_landing)

    # 미분류 확인
    unclassified = df[df["캠페인유형"] == "미분류"]["Campaign"].unique()
    if len(unclassified) > 0:
        print(f"\n[경고] 미분류 캠페인 {len(unclassified)}개:")
        for c in unclassified:
            print(f"  - {c}")

    # === STEP 4: 데이터 타입 변환 ===
    numeric_cols = [
        "Impressions (Channel)", "Clicks (Channel)", "Cost (Channel)",
        "구매 완료 (App+Web)", "Installs (App)", "회원가입 (App+Web)",
        "구매액 (App+Web)", "구매 완료 유저 수 (Web)", "구매 완료 유저 수 (App)",
        "장바구니 담기 (App+Web)", "첫 구매 완료 (App)", "첫 구매액 (App)",
        "completeProductOption (App+Web)", "상품상세페이지 조회 (App+Web)"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Event Date"] = pd.to_datetime(df["Event Date"], errors="coerce")

    # === STEP 5: 비용 보정 ===
    df["보정비용"] = df.apply(
        lambda row: row["Cost (Channel)"] * COST_FACTOR_FEB
            if row["Event Date"].month == 2
            else row["Cost (Channel)"] * COST_FACTOR_MAR,
        axis=1
    )

    # === STEP 6: 요약 출력 ===
    print(f"\n{'=' * 60}")
    print("캠페인 유형별 요약")
    print(f"{'=' * 60}")

    summary = df.groupby("캠페인유형").agg(
        행수=("Campaign", "count"),
        비용_raw=("Cost (Channel)", "sum"),
        보정비용=("보정비용", "sum"),
        구매완료=("구매 완료 (App+Web)", "sum"),
        구매액=("구매액 (App+Web)", "sum"),
    ).sort_values("구매액", ascending=False)

    # ROAS 계산 (보정비용 기준)
    summary["ROAS"] = (summary["구매액"] / summary["보정비용"].replace(0, float("nan")) * 100).round(0)
    summary["CPA"] = (summary["보정비용"] / summary["구매완료"].replace(0, float("nan"))).round(0)

    # 포맷팅
    for col in ["비용_raw", "보정비용", "구매액"]:
        summary[col] = summary[col].apply(lambda x: f"{x/10000:,.0f}만")
    summary["ROAS"] = summary["ROAS"].apply(lambda x: f"{x:,.0f}%")
    summary["CPA"] = summary["CPA"].apply(lambda x: f"{x:,.0f}원")
    summary["구매완료"] = summary["구매완료"].apply(lambda x: f"{x:,.0f}")

    print(summary.to_string())

    # === 지면별 요약 ===
    print(f"\n{'=' * 60}")
    print("지면별 요약")
    print(f"{'=' * 60}")

    placement_summary = df.groupby("지면").agg(
        보정비용=("보정비용", "sum"),
        구매완료=("구매 완료 (App+Web)", "sum"),
        구매액=("구매액 (App+Web)", "sum"),
    )
    placement_summary["ROAS"] = (placement_summary["구매액"] / placement_summary["보정비용"].replace(0, float("nan")) * 100).round(0)
    for col in ["보정비용", "구매액"]:
        placement_summary[col] = placement_summary[col].apply(lambda x: f"{x/10000:,.0f}만")
    placement_summary["ROAS"] = placement_summary["ROAS"].apply(lambda x: f"{x:,.0f}%")
    placement_summary["구매완료"] = placement_summary["구매완료"].apply(lambda x: f"{x:,.0f}")
    print(placement_summary.to_string())

    # === 주간별 추이 ===
    print(f"\n{'=' * 60}")
    print("주간별 추이")
    print(f"{'=' * 60}")

    df["주차"] = df["Event Date"].dt.isocalendar().week.astype(int)
    df["연월"] = df["Event Date"].dt.strftime("%Y-%m")

    weekly = df.groupby("주차").agg(
        기간_시작=("Event Date", "min"),
        기간_끝=("Event Date", "max"),
        보정비용=("보정비용", "sum"),
        구매완료=("구매 완료 (App+Web)", "sum"),
        구매액=("구매액 (App+Web)", "sum"),
    )
    weekly["ROAS"] = (weekly["구매액"] / weekly["보정비용"].replace(0, float("nan")) * 100).round(0)
    weekly["CPA"] = (weekly["보정비용"] / weekly["구매완료"].replace(0, float("nan"))).round(0)
    weekly["기간"] = weekly["기간_시작"].dt.strftime("%m/%d") + "~" + weekly["기간_끝"].dt.strftime("%m/%d")

    weekly_display = weekly[["기간", "보정비용", "구매완료", "구매액", "ROAS", "CPA"]].copy()
    for col in ["보정비용", "구매액"]:
        weekly_display[col] = weekly_display[col].apply(lambda x: f"{x/10000:,.0f}만")
    weekly_display["ROAS"] = weekly_display["ROAS"].apply(lambda x: f"{x:,.0f}%")
    weekly_display["CPA"] = weekly_display["CPA"].apply(lambda x: f"{x:,.0f}원")
    weekly_display["구매완료"] = weekly_display["구매완료"].apply(lambda x: f"{x:,.0f}")
    print(weekly_display.to_string())

    # === 유형×주간 ROAS 추이 ===
    print(f"\n{'=' * 60}")
    print("유형별 주간 ROAS 추이")
    print(f"{'=' * 60}")

    type_weekly = df.groupby(["캠페인유형", "주차"]).agg(
        보정비용=("보정비용", "sum"),
        구매액=("구매액 (App+Web)", "sum"),
    )
    type_weekly["ROAS"] = (type_weekly["구매액"] / type_weekly["보정비용"].replace(0, float("nan")) * 100).round(0)

    # 피벗 테이블
    roas_pivot = type_weekly["ROAS"].unstack(level="주차").fillna(0)
    roas_pivot = roas_pivot.map(lambda x: f"{x:,.0f}%" if x > 0 else "-")
    print(roas_pivot.to_string())

    # === 저장 ===
    output_path = os.path.join(OUTPUT_DIR, "kakao_cleaned.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n[저장] {output_path}")
    print(f"[저장] 최종 행 수: {len(df):,}")

    # 예외 캠페인도 별도 저장
    excluded_path = os.path.join(OUTPUT_DIR, "kakao_excluded.csv")
    df_excluded.to_csv(excluded_path, index=False, encoding="utf-8-sig")
    print(f"[저장] 예외 캠페인: {excluded_path} ({len(df_excluded):,}행)")

    return df


if __name__ == "__main__":
    df = main()
