"""
Airbridge CSV 전처리 스크립트
- raw CSV(5만+ 줄)를 분석 유형별로 집계하여 소규모 CSV로 출력
- /analyze-performance 커맨드의 프롬프트 과부하 방지용
"""

import argparse
import glob
import os
import sys
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# === 프로젝트 경로 ===
# marketing/scripts/pipeline/ → 4단계 상위 = joo-mkt/
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # joo-mkt/
RAW_DATA_DIR = BASE_DIR / "marketing" / "raw-data"
OUTPUT_DIR = BASE_DIR / "outputs" / "processed" / "pipeline"

# === 예외 캠페인 목록 (kakao-naming-convention.md 기준) ===
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


def find_latest_csv():
    """marketing/raw-data/ 내 가장 최신 Airbridge CSV 파일 탐색"""
    pattern = str(RAW_DATA_DIR / "*.csv")
    files = glob.glob(pattern)
    files = [f for f in files if "변경" not in f]
    if not files:
        print("오류: marketing/raw-data/에 CSV 파일이 없습니다.")
        sys.exit(1)
    return max(files, key=os.path.getmtime)


def load_and_preprocess(csv_path):
    """CSV 로드 → 카카오 필터 → 예외 제외 → 유형 분류 → 비용 보정 및 추가 지표 매핑"""
    df = pd.read_csv(csv_path)

    col_map = {
        "Event Date": "date",
        "Channel": "channel",
        "Campaign": "campaign",
        "Ad Group": "ad_group",
        "Ad Creative": "creative",
        "Impressions (Channel)": "impressions",
        "Clicks (Channel)": "clicks",
        "Cost (Channel)": "cost_raw",
        "구매 완료 (App+Web)": "purchases",
        "구매액 (App+Web)": "revenue",
        "회원가입 (App+Web)": "signup",
        "장바구니 담기 (App+Web)": "add_to_cart",
        "completeProductOption (App+Web)": "option_complete",
    }
    # 매핑되지 않은 컬럼은 무시하고 필요한 것만 가져오기
    existing_cols = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=existing_cols)
    
    # 누락된 옵셔널 컬럼들 0으로 채우기
    for opt_col in ["signup", "add_to_cart", "option_complete"]:
        if opt_col not in df.columns:
            df[opt_col] = 0

    df["date"] = pd.to_datetime(df["date"])

    # 1. 카카오 채널만 필터
    df = df[df["channel"] == "kakao"].copy()

    # 2. 예외 캠페인 제외
    df = df[~df["campaign"].isin(EXCLUDED_CAMPAIGNS)]
    df = df[~df["campaign"].str.contains(",", na=False)]
    df["campaign"] = df["campaign"].str.replace(
        r"-retarget-purchase-retarget-purchase", "-retarget-purchase", regex=True
    )

    # 3. 캠페인 유형 분류 (상위가 앞세워지게 네이밍 수정)
    def classify(name):
        n = str(name).lower()
        if n == "kakaodisplay_conversion_catalog":
            return "RT-카탈로그"
        if "ad_pbtd" in n:
            return "AD-PBTD"
        if "pbtd" in n and "ua" in n:
            return "UA-PBTD"
        if "pbtd" in n and "retarget" in n:
            return "RT-PBTD"
        if "sel" in n and "ua" in n:
            return "UA-SEL"
        if "sel" in n and "retarget" in n:
            return "RT-SEL"
        if "traffic" in n:
            return "Traffic"
        if "ua" in n:
            return "UA-일반"
        if "retarget" in n:
            return "RT-일반"
        return "기타"

    df["campaign_type"] = df["campaign"].apply(classify)

    # 상위 집계 그룹
    ua_types = {"UA-일반", "UA-SEL", "UA-PBTD"}
    rt_types = {"RT-일반", "RT-SEL", "RT-PBTD", "AD-PBTD", "RT-카탈로그"}
    df["group"] = df["campaign_type"].apply(
        lambda x: "UA전체" if x in ua_types else ("RT전체" if x in rt_types else x)
    )

    # 4. 비용 보정
    df["cost_adjusted"] = df.apply(
        lambda r: r["cost_raw"] / 1.5 if r["date"] < pd.Timestamp("2026-03-01") else r["cost_raw"] / 1.763,
        axis=1,
    )

    # 5. 지면(placement) 파싱: 캠페인명 첫 토큰으로 bizboard/display 구분
    def parse_placement(name):
        n = str(name).lower()
        if n.startswith("bizboard"):
            return "BZ"
        elif n.startswith("display") or n.startswith("kakaodisplay"):
            return "DP"
        return "ETC"

    df["placement"] = df["campaign"].apply(parse_placement)

    # 숫자 컬럼 타입 보장
    num_cols = ["impressions", "clicks", "cost_raw", "cost_adjusted", "purchases", "revenue", "signup", "add_to_cart", "option_complete"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def calc_metrics(agg):
    """집계된 DataFrame에 퍼널 및 효율 등 파생 지표 추가"""
    # 0으로 나누기 방지: 0 → NaN 변환 후 float 캐스팅
    c_raw = agg["cost_raw"].replace(0, np.nan)
    c_adj = agg["cost_adjusted"].replace(0, np.nan)
    clk = agg["clicks"].replace(0, np.nan)
    imp = agg["impressions"].replace(0, np.nan)
    pur = agg["purchases"].replace(0, np.nan)
    sign = agg["signup"].replace(0, np.nan)

    agg["CPC"] = (c_raw / clk).round(0)
    agg["CTR"] = (clk / imp * 100).round(2)
    agg["ROAS"] = (agg["revenue"] / c_adj * 100).round(1)
    
    agg["가입CPA"] = (c_adj / sign).round(0)
    agg["가입CVR"] = (agg["signup"] / clk * 100).round(2)
    
    agg["구매CPA"] = (c_adj / pur).round(0)
    agg["구매CVR"] = (agg["purchases"] / clk * 100).round(2)
    
    agg["장바구니CVR"] = (agg["add_to_cart"] / clk * 100).round(2)
    agg["옵션완료CVR"] = (agg["option_complete"] / clk * 100).round(2)
    
    agg["객단가"] = (agg["revenue"] / pur).round(0)

    # CPM 추가 (cost_raw 기준, 1000노출당 비용)
    agg["CPM"] = (c_raw / imp * 1000).round(0)

    return agg.fillna(0)


def get_week_range(ref_date):
    start = ref_date - timedelta(days=ref_date.weekday())
    end = start + timedelta(days=6)
    return start, end


# === 지표 및 유형 순서 정의 ===
METRIC_ORDER = [
    "cost_adjusted", "spend_share(%)", "CPC", "CPM", "CTR", "ROAS",
    "가입CPA", "가입CVR", "구매CPA", "구매CVR",
    "장바구니CVR", "옵션완료CVR", "객단가", "purchases", "revenue"
]

TYPE_ORDER = [
    "UA-일반", "RT-일반", "UA-SEL", "RT-SEL", 
    "UA-PBTD", "RT-PBTD", "RT-카탈로그", "AD-PBTD", 
    "Traffic", "UA전체", "RT전체"
]


def sort_by_type(df):
    if "campaign_type" not in df.columns:
        return df
    df["sort_idx"] = df["campaign_type"].apply(lambda x: TYPE_ORDER.index(x) if x in TYPE_ORDER else 99)
    return df.sort_values("sort_idx").drop(columns=["sort_idx"])


def get_week_number(d):
    return f"W{d.isocalendar()[1]:02d}"


def get_velocity_by_adgroup(df_full, ref_date):
    """최근 3일 vs 이전 3일 ROAS·비용 추세 (주차 경계 무시, 기준일 기반)"""
    # 기준일로부터 직전 3일 (D, D-1, D-2) vs 그 이전 3일 (D-3, D-4, D-5)
    recent_start = ref_date - pd.Timedelta(days=2)
    past_start = ref_date - pd.Timedelta(days=5)

    res = []
    for (ag, ct, pl), sub in df_full.groupby(["ad_group", "campaign_type", "placement"]):
        recent_sub = sub[(sub["date"] >= recent_start) & (sub["date"] <= ref_date)]
        past_sub = sub[(sub["date"] >= past_start) & (sub["date"] < recent_start)]

        r_cost = recent_sub["cost_adjusted"].sum()
        r_rev = recent_sub["revenue"].sum()
        recent_roas = (r_rev / r_cost * 100) if r_cost > 0 else 0

        p_cost = past_sub["cost_adjusted"].sum()
        p_rev = past_sub["revenue"].sum()
        past_roas = (p_rev / p_cost * 100) if p_cost > 0 else 0

        # ROAS 추이: N/A(데이터부족) / ▼·▲(±30%p 이상) / →(변동 미미)
        trend = "N/A"
        if p_cost > 0 and r_cost > 0:
            diff = recent_roas - past_roas
            if diff <= -30:
                trend = f"▼({diff:.0f}%p)"
            elif diff >= 30:
                trend = f"▲(+{diff:.0f}%p)"
            else:
                trend = f"→({diff:+.0f}%p)"

        # 비용 변화율
        cost_change = "N/A"
        if p_cost > 0 and r_cost > 0:
            pct = (r_cost - p_cost) / p_cost * 100
            cost_change = f"{pct:+.0f}%"

        res.append({
            "ad_group": ag,
            "campaign_type": ct,
            "placement": pl,
            "3일_ROAS추세": trend,
            "3일_비용변화": cost_change,
        })
    return pd.DataFrame(res)


def parse_adgroup(name):
    """AdGroup 텍스트에서 타겟과 브랜드 분리"""
    n = str(name).lower()
    demo, brand = "", ""
    parts = n.split('-', 1)
    if len(parts) > 1:
        demo = parts[0]
        rest = parts[1]
        
        # 브랜드 파싱 로직 보강
        if "_br_" in rest:
            brand = rest.split("_br_")[1].split("-")[0]
        elif "_ct_" in rest:
            brand = rest.split("_ct_")[1].split("-")[0]
        else:
            # -프로모션 앞부분 추출
            brand = rest.split("-")[0].split("_")[-1]
    else:
        brand = n.split("_")[-1]
    return pd.Series({"Target": demo, "Brand": brand})


def parse_creative(name):
    """소재명에서 유형, 버전, 메인옵션, 상세옵션, 투입일 추출
    - creative_naming.py 공유 모듈 기반 정규화
    """
    # 같은 디렉토리의 공유 모듈 import
    import sys as _sys
    _pipeline_dir = str(Path(__file__).parent)
    if _pipeline_dir not in _sys.path:
        _sys.path.insert(0, _pipeline_dir)
    from creative_naming import normalize_main, normalize_detailed, is_legacy_main, is_saletap
    import re

    n = str(name).lower()
    res = {
        "launch_date_raw": "n/a",
        "creative_type": "etc",
        "creative_version": "etc",
        "option_main": "etc",
        "option_detailed": "etc"
    }

    tokens = re.split(r'[-_]', n)

    if len(tokens) >= 1 and re.match(r'^\d{6}$', tokens[0]):
        res["launch_date_raw"] = tokens[0]

    # 타입 및 버전 (순서 기반 추정)
    types = {"img", "vid", "msg", "carouselfeed", "carouselcommerce", "widelist", "wideimage"}
    versions = {"single", "dynamic", "carousel", "catalog", "seeding"}

    for t in tokens:
        if t in types: res["creative_type"] = t
        if t in versions: res["creative_version"] = t

    # saletap 패턴 특수 처리
    if is_saletap(n):
        res["option_main"] = "prm"
        res["option_detailed"] = "sale"
        return pd.Series(res)

    # 메인/상세 옵션 추출 (토큰 순회 + 정규화)
    for i, t in enumerate(tokens):
        # 별칭 포함 main 매칭 (logo → log 등)
        main_norm = normalize_main(t)
        if main_norm:
            res["option_main"] = main_norm
            if i + 1 < len(tokens):
                detail = tokens[i+1]
                detail_norm = normalize_detailed(detail)
                res["option_detailed"] = detail_norm if detail_norm else "etc"
            break
        # old 컨벤션 main (pho) → main=sku, detailed=pho로 재분류
        if is_legacy_main(t):
            res["option_main"] = "sku"
            res["option_detailed"] = t  # "pho"
            if i + 1 < len(tokens):
                detail_norm = normalize_detailed(tokens[i+1])
                if detail_norm:
                    res["option_detailed"] = detail_norm
            break

    return pd.Series(res)


def generate_creative(df, ref_date):
    """소재별 성과 및 요소별 집계 생성 (당월 기준)"""
    agg_cols = ["impressions", "clicks", "cost_raw", "cost_adjusted", "purchases", "revenue", "signup", "add_to_cart", "option_complete"]
    
    # 1. 당월 데이터 필터
    month_start = ref_date.replace(day=1)
    df_m = df[(df["date"] >= month_start) & (df["date"] <= ref_date)].copy()
    
    if df_m.empty:
        return {}

    # 2. 소재별 집계
    cre_agg = df_m.groupby(["campaign_type", "placement", "ad_group", "creative"])[agg_cols].sum().reset_index()
    cre_agg = calc_metrics(cre_agg)
    
    # 파싱
    parsed_ag = cre_agg["ad_group"].apply(parse_adgroup)
    parsed_cre = cre_agg["creative"].apply(parse_creative)
    cre_agg = pd.concat([cre_agg, parsed_ag, parsed_cre], axis=1)
    
    # 3. 피로도 및 상대 지표 (RPI) 산출
    # launch_date 파싱 (260310 -> 2026-03-10)
    def to_date(s):
        try:
            return pd.to_datetime("20" + str(s), format="%Y%m%d")
        except:
            return pd.NaT
    
    cre_agg["launch_dt"] = cre_agg["launch_date_raw"].apply(to_date)
    cre_agg["days_active"] = (ref_date - cre_agg["launch_dt"]).dt.days
    
    # RPI: 해당 캠페인 유형 평균 ROAS 대비 비율
    type_avg = cre_agg.groupby("campaign_type")["ROAS"].transform("mean")
    cre_agg["RPI"] = (cre_agg["ROAS"] / type_avg * 100).round(1)
    
    # 4. 요소별(Elements) 집계
    # Main Object별
    main_agg = cre_agg.groupby("option_main")[agg_cols].sum().reset_index()
    main_agg = calc_metrics(main_agg)
    main_agg["category"] = "option_main"
    main_agg = main_agg.rename(columns={"option_main": "value"})
    
    # Detailed Option별
    det_agg = cre_agg.groupby("option_detailed")[agg_cols].sum().reset_index()
    det_agg = calc_metrics(det_agg)
    det_agg["category"] = "option_detailed"
    det_agg = det_agg.rename(columns={"option_detailed": "value"})

    # 브랜드별
    brand_agg = cre_agg.groupby("Brand")[agg_cols].sum().reset_index()
    brand_agg = calc_metrics(brand_agg)
    brand_agg["category"] = "brand"
    brand_agg = brand_agg.rename(columns={"Brand": "value"})
    
    elements = pd.concat([main_agg, det_agg, brand_agg], ignore_index=True)

    return {
        "creative-performance": cre_agg.sort_values("cost_adjusted", ascending=False),
        "creative-elements": elements
    }


def generate_weekly(df, ref_date):
    """주간 성과 데이터프레임 콜렉션 생성"""
    agg_cols = ["impressions", "clicks", "cost_raw", "cost_adjusted", "purchases", "revenue", "signup", "add_to_cart", "option_complete"]
    this_start, this_end = get_week_range(ref_date)
    prev_start, prev_end = get_week_range(ref_date - timedelta(weeks=1))

    this_week = df[(df["date"] >= this_start) & (df["date"] <= this_end)]
    actual_days = this_week["date"].nunique()
    
    status_suffix = f" (진행 중, {actual_days}/7일)" if actual_days < 7 else ""
    this_week_label = f"{get_week_number(this_start)} ({this_start.strftime('%m/%d')}~{this_end.strftime('%m/%d')}{status_suffix})"
    prev_week_label = f"{get_week_number(prev_start)} ({prev_start.strftime('%m/%d')}~{prev_end.strftime('%m/%d')})"

    # --- 1. Total (월간/주차/데일리) ---
    month_start = ref_date.replace(day=1)
    month_df = df[df["date"] >= month_start]
    monthly = calc_metrics(month_df[agg_cols].sum().to_frame().T)
    monthly["period"] = f"{month_start.strftime('%m/%Y')} 누적 ({month_start.strftime('%m/%d')}~{ref_date.strftime('%m/%d')})"
    monthly["level"] = "1. 월간"

    weeks_data = []
    for i in range(3):
        w_start, w_end = get_week_range(ref_date - timedelta(weeks=i))
        w_df = df[(df["date"] >= w_start) & (df["date"] <= w_end)]
        if len(w_df) == 0: continue
        w_agg = calc_metrics(w_df[agg_cols].sum().to_frame().T)
        w_days = w_df["date"].nunique()
        w_suffix = f" ({w_days}/7일)" if w_days < 7 else ""
        w_agg["period"] = f"{get_week_number(w_start)} ({w_start.strftime('%m/%d')}~{w_end.strftime('%m/%d')}{w_suffix})"
        w_agg["level"] = "2. 주차별"
        weeks_data.append(w_agg)

    daily_data = []
    for i in range(7):
        d = ref_date - timedelta(days=i)
        d_df = df[df["date"] == d]
        if len(d_df) == 0: continue
        d_agg = calc_metrics(d_df[agg_cols].sum().to_frame().T)
        d_agg["period"] = d.strftime("%m/%d (%a)")
        d_agg["level"] = "3. 데일리"
        daily_data.append(d_agg)

    total = pd.concat([monthly] + weeks_data + daily_data, ignore_index=True)
    out_cols = [c for c in ["period", "level"] + METRIC_ORDER if c in total.columns and c != "spend_share(%)"]
    total = total[out_cols]

    # --- 2. by_type ---
    prev_week = df[(df["date"] >= prev_start) & (df["date"] <= prev_end)]
    
    type_this = calc_metrics(this_week.groupby("campaign_type")[agg_cols].sum().reset_index())
    type_this["week"] = this_week_label
    type_prev = calc_metrics(prev_week.groupby("campaign_type")[agg_cols].sum().reset_index())
    type_prev["week"] = prev_week_label

    group_this = calc_metrics(this_week.groupby("group")[agg_cols].sum().reset_index()).rename(columns={"group": "campaign_type"})
    group_this["week"] = this_week_label
    group_prev = calc_metrics(prev_week.groupby("group")[agg_cols].sum().reset_index()).rename(columns={"group": "campaign_type"})
    group_prev["week"] = prev_week_label

    by_type = pd.concat([type_this, group_this, type_prev, group_prev], ignore_index=True)
    by_type = sort_by_type(by_type)
    by_type = by_type[[c for c in ["week", "campaign_type"] + METRIC_ORDER if c in by_type.columns and c != "spend_share(%)"]]

    # --- 3. general_detail (UA-일반, RT-일반 전용) ---
    gen_types = {"UA-일반", "RT-일반"}
    gen_df = df[df["campaign_type"].isin(gen_types)]

    gen_weeks = []
    for i in range(3):
        w_start, w_end = get_week_range(ref_date - timedelta(weeks=i))
        w_df = gen_df[(gen_df["date"] >= w_start) & (gen_df["date"] <= w_end)]
        if len(w_df) == 0: continue
        w_agg = calc_metrics(w_df.groupby("campaign_type")[agg_cols].sum().reset_index())
        w_days = w_df["date"].nunique()
        w_suffix = f" ({w_days}/7일)" if w_days < 7 else ""
        w_agg["period"] = f"{get_week_number(w_start)} ({w_start.strftime('%m/%d')}~{w_end.strftime('%m/%d')}{w_suffix})"
        w_agg["level"] = "1. 주차별"
        gen_weeks.append(w_agg)

    gen_daily = []
    for i in range(7):
        d = ref_date - timedelta(days=i)
        d_df = gen_df[gen_df["date"] == d]
        if len(d_df) == 0: continue
        d_agg = calc_metrics(d_df.groupby("campaign_type")[agg_cols].sum().reset_index())
        d_agg["period"] = d.strftime("%m/%d (%a)")
        d_agg["level"] = "2. 데일리"
        gen_daily.append(d_agg)

    general_detail = pd.concat(gen_weeks + gen_daily, ignore_index=True) if (gen_weeks or gen_daily) else pd.DataFrame()
    if not general_detail.empty:
        general_detail = sort_by_type(general_detail)
        general_detail = general_detail[[c for c in ["period", "level", "campaign_type"] + METRIC_ORDER if c in general_detail.columns and c != "spend_share(%)"]]

    # --- 4. adgroup (placement 포함 3차원 집계) ---
    gen_this = gen_df[(gen_df["date"] >= this_start) & (gen_df["date"] <= this_end)]

    adgroup_this = gen_this.groupby(["ad_group", "campaign_type", "placement"])[agg_cols].sum().reset_index()
    adgroup_this = calc_metrics(adgroup_this)
    adgroup_this["week"] = this_week_label

    # 파싱
    parsed = adgroup_this["ad_group"].apply(parse_adgroup)
    adgroup_this = pd.concat([adgroup_this, parsed], axis=1)

    # 3일 가속도 추세 (주차 경계 무시, 기준일 기반, 전체 데이터 사용)
    velocity_df = get_velocity_by_adgroup(gen_df, ref_date)
    if not velocity_df.empty:
        adgroup_this = pd.merge(adgroup_this, velocity_df, on=["ad_group", "campaign_type", "placement"], how="left")
    else:
        adgroup_this["3일_ROAS추세"] = "-"
        adgroup_this["3일_비용변화"] = "-"

    # 성공군 분류 로직 + LTV 초저가 트래픽군 (Potential 기준 상향: UA 400%, RT 600%)
    def get_success_tier(r):
        roas = r["ROAS"]
        cvr = r["구매CVR"]
        if r["campaign_type"] == "UA-일반":
            if roas >= 600: return "Winner"
            if roas >= 400 or cvr >= 1.0: return "Potential"
        elif r["campaign_type"] == "RT-일반":
            if roas >= 1000: return "Winner"
            if roas >= 600 or cvr >= 1.0: return "Potential"

        # 저가 트래픽 필터 (가입CPA 5000 이하 or CPC 150 이하)
        if (r["가입CPA"] > 0 and r["가입CPA"] <= 5000) or (r["CPC"] > 0 and r["CPC"] <= 150):
            return "저가트래픽(LTV)"

        return "Fail"

    adgroup_this["success_type"] = adgroup_this.apply(get_success_tier, axis=1)

    # 누적 1만원 이상 필터 (노이즈 배제)
    adgroup_this = adgroup_this[adgroup_this["cost_adjusted"] >= 10000].copy()

    # 비용 비중 계산
    total_ua_c = adgroup_this[adgroup_this["campaign_type"] == "UA-일반"]["cost_adjusted"].sum()
    total_rt_c = adgroup_this[adgroup_this["campaign_type"] == "RT-일반"]["cost_adjusted"].sum()
    def get_share(r):
        if r["campaign_type"] == "UA-일반" and total_ua_c > 0:
            return (r["cost_adjusted"] / total_ua_c * 100)
        elif r["campaign_type"] == "RT-일반" and total_rt_c > 0:
            return (r["cost_adjusted"] / total_rt_c * 100)
        return 0
    adgroup_this["spend_share(%)"] = adgroup_this.apply(get_share, axis=1).round(1)

    # 정렬: 보정비용 내림차순 (핵심 그룹 우선 파악)
    adgroup_this = adgroup_this.sort_values("cost_adjusted", ascending=False)

    final_cols = ["week", "success_type", "Target", "Brand", "ad_group", "campaign_type", "placement", "3일_ROAS추세", "3일_비용변화"] + [c for c in METRIC_ORDER if c in adgroup_this.columns]
    adgroup_this = adgroup_this[final_cols]

    return {
        "weekly-total": total,
        "weekly-by-type": by_type,
        "weekly-general-detail": general_detail,
        "weekly-adgroup": adgroup_this,
    }


def main():
    parser = argparse.ArgumentParser(description="Airbridge CSV 전처리")
    parser.add_argument("--file", help="CSV 파일 경로 (미지정 시 자동 탐색)")
    parser.add_argument("--type", default="weekly", choices=["weekly", "creative"])
    parser.add_argument("--date", help="기준 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    csv_path = args.file or find_latest_csv()
    ref_date = pd.Timestamp(args.date) if args.date else pd.Timestamp.now().normalize() - timedelta(days=1)
    
    df = load_and_preprocess(csv_path)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.type == "weekly":
        results = generate_weekly(df, ref_date)
        for name, res_df in results.items():
            out_path = OUTPUT_DIR / f"{name}.csv"
            res_df.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"주간 성과 → {out_path} ({len(res_df)}행)")

    elif args.type == "creative":
        results = generate_creative(df, ref_date)
        for name, res_df in results.items():
            out_path = OUTPUT_DIR / f"{name}.csv"
            res_df.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"소재 성과 → {out_path} ({len(res_df)}행)")

if __name__ == "__main__":
    main()
