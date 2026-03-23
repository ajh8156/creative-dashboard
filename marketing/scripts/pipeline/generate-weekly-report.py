"""
주간 성과 리포트 자동 생성 스크립트
- outputs/processed/*.csv 4개를 읽어 마크다운 리포트 자동 생성
- 섹션 1~5: 테이블 완전 자동화
- Executive Summary + 섹션 6: LLM 플레이스홀더
- insights-context.md: LLM용 핵심 수치 요약
"""

import argparse
import math
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# === 경로 ===
# marketing/scripts/pipeline/ → 4단계 상위 = joo-mkt/
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
OUTPUT_DIR = BASE_DIR / "outputs" / "processed" / "pipeline"
REPORT_DIR = BASE_DIR / "marketing" / "reports" / "weekly"

# === 요일 매핑 ===
DOW_MAP = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


# ─────────────────────────────────────────
# 포맷팅 유틸리티
# ─────────────────────────────────────────

def fmt_cost(v):
    """보정비용 포맷: 892만, 1,695만, 1.25억"""
    if pd.isna(v) or v == 0:
        return "0"
    man = v / 10000
    if man >= 10000:  # 1억 이상
        return f"{man / 10000:.2f}억"
    elif man >= 1000:  # 1000만 이상
        return f"{man:,.0f}만"
    else:
        return f"{man:,.0f}만"


def fmt_revenue(v):
    """구매액 포맷: 4,691만, 7.11억"""
    if pd.isna(v) or v == 0:
        return "0"
    man = v / 10000
    if man >= 10000:
        return f"{man / 10000:.2f}억"
    else:
        return f"{man:,.0f}만"


def fmt_won(v):
    """원 단위: 7,206원"""
    if pd.isna(v) or v == 0:
        return "-"
    return f"{v:,.0f}원"


def fmt_pct(v, d=1):
    """퍼센트: 525.9%"""
    if pd.isna(v) or v == 0:
        return "0.0%" if d == 1 else "0.00%"
    return f"{v:.{d}f}%"


def fmt_int(v, suffix="건"):
    """정수+접미사: 1,238건"""
    if pd.isna(v) or v == 0:
        return f"0{suffix}"
    return f"{v:,.0f}{suffix}"


def fmt_trend(trend_str):
    """3일추이 문자열에 HTML 컬러 태그 적용"""
    if pd.isna(trend_str) or trend_str == "N/A":
        return "N/A"
    s = str(trend_str)
    if s.startswith("▲"):
        return f'<span style="color:red">{s}</span>'
    elif s.startswith("▼"):
        return f'<span style="color:blue">{s}</span>'
    return s  # → 표기


def fmt_tier_short(t):
    """분류 약자"""
    return {"Winner": "W", "Potential": "P", "저가트래픽(LTV)": "LTV", "Fail": "F"}.get(t, t)


def fmt_adgroup_name(row):
    """Ad Group 표시명: Target / Brand"""
    target = str(row.get("Target", "")).strip()
    brand = str(row.get("Brand", "")).strip()
    if target and target != "nan" and brand and brand != "nan":
        return f"{target} / {brand}"
    # ad_group에서 파싱 시도
    ag = str(row.get("ad_group", ""))
    if "-" in ag:
        parts = ag.split("-")
        return f"{parts[0]} / {parts[-1].replace('promotion', '').replace('_', ' ').strip()}"
    return ag


def parse_daily_label(period_str):
    """'03/18 (Wed)' 형태에서 날짜 파싱"""
    m = re.match(r"(\d{2}/\d{2})\s*\((\w+)\)", str(period_str))
    if m:
        return m.group(0)
    return str(period_str)


def bold_if(val, condition):
    """조건부 볼드"""
    return f"**{val}**" if condition else str(val)


# ─────────────────────────────────────────
# CSV 로드
# ─────────────────────────────────────────

def load_csvs():
    """전처리된 CSV 4개 로드"""
    total = pd.read_csv(OUTPUT_DIR / "weekly-total.csv")
    by_type = pd.read_csv(OUTPUT_DIR / "weekly-by-type.csv")
    detail = pd.read_csv(OUTPUT_DIR / "weekly-general-detail.csv")
    adgroup = pd.read_csv(OUTPUT_DIR / "weekly-adgroup.csv")
    return total, by_type, detail, adgroup


# ─────────────────────────────────────────
# 섹션 빌더
# ─────────────────────────────────────────

def build_header(ref_date, adgroup_df):
    """리포트 헤더 + 메타 정보"""
    # 주차 정보 파싱 (adgroup의 week 컬럼에서)
    week_str = str(adgroup_df.iloc[0]["week"])
    # 진행 중 일수 파싱: "3/7일" 패턴
    m = re.search(r"(\d+)/(\d+)일", week_str)
    days_done = int(m.group(1)) if m else "?"
    days_total = int(m.group(2)) if m else 7

    # 주차 번호
    iso_week = ref_date.isocalendar()[1]

    # 주 시작/종료
    week_start = ref_date - timedelta(days=ref_date.weekday())  # 월요일
    week_end = week_start + timedelta(days=6)

    lines = []
    lines.append(f"# 주간 성과 리포트 — {ref_date.year} W{iso_week}")
    lines.append("")
    lines.append(f"> **기간**: W{iso_week} ({week_start.strftime('%m/%d')}~{week_end.strftime('%m/%d')}) — 진행 중 {days_done}/{days_total}일 ({week_start.strftime('%m/%d')}~{ref_date.strftime('%m/%d')})")
    lines.append(f"> **데이터 소스**: Airbridge raw CSV → `preprocess-airbridge.py` 전처리")
    lines.append(f"> **비용 기준**: 보정비용 (3월~: raw ÷ 1.763)")
    lines.append(f"> **OKR**: UA ROAS ≥ 600% | RT ROAS ≥ 1,000% | CPA ≤ 10,600원")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("{{EXECUTIVE_SUMMARY}}")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 이번 주 주요 변수")
    lines.append("")
    lines.append("> _마케터 메모란: 기획전 오픈, 프로모션, 시즌 이슈 등 기록_")
    lines.append("> -")
    lines.append("> -")
    lines.append("> -")
    lines.append("")
    lines.append("---")
    return "\n".join(lines)


def build_section1(total_df):
    """섹션 1: Total 카카오 성과"""
    lines = ["## 1. Total 카카오 성과", ""]

    # 월간 누적
    monthly = total_df[total_df["level"] == "1. 월간"]
    if not monthly.empty:
        r = monthly.iloc[0]
        period_label = str(r["period"]).replace("누적 ", "누적\n")
        # 기간 파싱
        m = re.search(r"\((.+?)\)", str(r["period"]))
        period_range = m.group(1) if m else ""
        lines.append(f"### 월간 누적 ({period_range})")
        lines.append("")
        lines.append("| 보정비용 | ROAS | 구매CPA | 구매CVR | 객단가 | 구매완료 | 구매액 |")
        lines.append("|---|---|---|---|---|---|---|")
        lines.append(f"| {fmt_cost(r['cost_adjusted'])} | {fmt_pct(r['ROAS'])} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {fmt_won(r['객단가'])} | {fmt_int(r['purchases'])} | {fmt_revenue(r['revenue'])} |")
        lines.append("")

    # 주차별 추이
    weekly = total_df[total_df["level"] == "2. 주차별"].copy()
    if not weekly.empty:
        lines.append("### 주차별 추이")
        lines.append("")
        lines.append("| 주차 | 보정비용 | ROAS | 구매CPA | 구매CVR | 객단가 | 구매완료 | 구매액 |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for i, (_, r) in enumerate(weekly.iterrows()):
            # 주차 라벨: W12 (3/7일)
            period = str(r["period"])
            m_days = re.search(r"(\d+/\d+일)", period)
            days_label = f" ({m_days.group(1)})" if m_days else ""
            m_week = re.search(r"W(\d+)", period)
            week_num = m_week.group(0) if m_week else period[:10]
            label = f"**{week_num}**{days_label}" if i == 0 else week_num
            # ROAS 볼드: 최고치
            roas_str = fmt_pct(r["ROAS"])
            if i > 0 and r["ROAS"] == weekly["ROAS"].max():
                roas_str = f"**{roas_str}**"
            lines.append(f"| {label} | {fmt_cost(r['cost_adjusted'])} | {roas_str} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {fmt_won(r['객단가'])} | {fmt_int(r['purchases'])} | {fmt_revenue(r['revenue'])} |")
        lines.append("")

    # 데일리 추이
    daily = total_df[total_df["level"] == "3. 데일리"].copy()
    if not daily.empty:
        lines.append("### 데일리 추이 (최근 7일)")
        lines.append("")
        lines.append("| 날짜 | 보정비용 | ROAS | 구매CPA | 구매CVR | 구매완료 |")
        lines.append("|---|---|---|---|---|---|")
        # 최저/최고 ROAS 파악
        min_roas = daily["ROAS"].min()
        max_roas = daily["ROAS"].max()
        min_cpa = daily["구매CPA"].min()
        max_cpa = daily["구매CPA"].max()
        for i, (_, r) in enumerate(daily.iterrows()):
            period = str(r["period"])
            label = f"**{period}**" if i == 0 else period
            roas_str = fmt_pct(r["ROAS"])
            cpa_str = fmt_won(r["구매CPA"])
            cvr_str = fmt_pct(r["구매CVR"], 2)
            # 최저 ROAS/최고 CPA 볼드
            if r["ROAS"] == min_roas:
                roas_str = f"**{roas_str}**"
            if r["구매CPA"] == max_cpa:
                cpa_str = f"**{cpa_str}**"
            if r["구매CVR"] == daily["구매CVR"].min():
                cvr_str = f"**{cvr_str}**"
            lines.append(f"| {label} | {fmt_cost(r['cost_adjusted'])} | {roas_str} | {cpa_str} | {cvr_str} | {fmt_int(r['purchases'])} |")
        lines.append("")

    lines.append("---")
    return "\n".join(lines)


def build_section2(by_type_df):
    """섹션 2: 캠페인 유형별 성과"""
    lines = ["## 2. 캠페인 유형별 성과", ""]

    # 현재/전주 분리 (week 컬럼 기준, 첫 번째가 현주차)
    weeks = by_type_df["week"].unique()
    current_week = weeks[0] if len(weeks) > 0 else ""
    prev_week = weeks[1] if len(weeks) > 1 else ""
    curr = by_type_df[by_type_df["week"] == current_week]
    prev = by_type_df[by_type_df["week"] == prev_week]

    def get_row(df, ctype):
        rows = df[df["campaign_type"] == ctype]
        return rows.iloc[0] if not rows.empty else None

    # --- UA 유형 ---
    lines.append("### UA 유형")
    lines.append("")
    lines.append("| 유형 | 보정비용 | ROAS | 구매CPA | 구매CVR | 가입CVR | 객단가 | 구매완료 | 구매액 |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    ua_types = ["UA-일반", "UA-SEL", "UA-PBTD"]
    for ct in ua_types:
        r = get_row(curr, ct)
        if r is not None:
            lines.append(f"| {ct} | {fmt_cost(r['cost_adjusted'])} | {fmt_pct(r['ROAS'])} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {fmt_pct(r['가입CVR'], 2)} | {fmt_won(r['객단가'])} | {fmt_int(r['purchases'])} | {fmt_revenue(r['revenue'])} |")
    # UA전체
    ua_total = get_row(curr, "UA전체")
    if ua_total is not None:
        lines.append(f"| **UA전체** | **{fmt_cost(ua_total['cost_adjusted'])}** | **{fmt_pct(ua_total['ROAS'])}** | **{fmt_won(ua_total['구매CPA'])}** | **{fmt_pct(ua_total['구매CVR'], 2)}** | **{fmt_pct(ua_total['가입CVR'], 2)}** | **{fmt_won(ua_total['객단가'])}** | **{fmt_int(ua_total['purchases'])}** | **{fmt_revenue(ua_total['revenue'])}** |")
        ua_roas = ua_total["ROAS"]
        ua_okr = ua_roas / 600 * 100
        lines.append("")
        lines.append(f"**OKR 달성률**: UA ROAS {fmt_pct(ua_roas)} / 목표 600% = **{ua_okr:.1f}%** ❌" if ua_okr < 100 else f"**OKR 달성률**: UA ROAS {fmt_pct(ua_roas)} / 목표 600% = **{ua_okr:.1f}%** ✅")
    lines.append("")

    # UA WoW 비교
    lines.append("| WoW 비교 | W12 | W11 | 방향 |")
    lines.append("|---|---|---|---|")
    for ct in ua_types:
        c = get_row(curr, ct)
        p = get_row(prev, ct)
        if c is not None and p is not None:
            # ROAS
            diff_roas = c["ROAS"] - p["ROAS"]
            arrow = '<span style="color:red">▲</span>' if diff_roas > 0 else '<span style="color:blue">▼</span>'
            lines.append(f"| {ct} ROAS | {fmt_pct(c['ROAS'])} | {fmt_pct(p['ROAS'])} | {arrow} {diff_roas:+.1f}%p |")
            # 가입CVR
            diff_cvr = c["가입CVR"] - p["가입CVR"]
            arrow2 = '<span style="color:red">▲</span>' if diff_cvr > 0 else '<span style="color:blue">▼</span>'
            lines.append(f"| {ct} 가입CVR | {fmt_pct(c['가입CVR'], 2)} | {fmt_pct(p['가입CVR'], 2)} | {arrow2} {diff_cvr:+.2f}%p |")
    lines.append("")

    # --- RT 유형 ---
    lines.append("### RT 유형")
    lines.append("")
    lines.append("| 유형 | 보정비용 | ROAS | 구매CPA | 구매CVR | 객단가 | 구매완료 | 구매액 |")
    lines.append("|---|---|---|---|---|---|---|---|")
    rt_types = ["RT-일반", "RT-SEL", "RT-PBTD", "RT-카탈로그", "AD-PBTD"]
    for ct in rt_types:
        r = get_row(curr, ct)
        if r is not None:
            lines.append(f"| {ct} | {fmt_cost(r['cost_adjusted'])} | {fmt_pct(r['ROAS'])} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {fmt_won(r['객단가'])} | {fmt_int(r['purchases'])} | {fmt_revenue(r['revenue'])} |")
    rt_total = get_row(curr, "RT전체")
    if rt_total is not None:
        lines.append(f"| **RT전체** | **{fmt_cost(rt_total['cost_adjusted'])}** | **{fmt_pct(rt_total['ROAS'])}** | **{fmt_won(rt_total['구매CPA'])}** | **{fmt_pct(rt_total['구매CVR'], 2)}** | **{fmt_won(rt_total['객단가'])}** | **{fmt_int(rt_total['purchases'])}** | **{fmt_revenue(rt_total['revenue'])}** |")
        rt_roas = rt_total["ROAS"]
        rt_okr = rt_roas / 1000 * 100
        lines.append("")
        lines.append(f"**OKR 달성률**: RT ROAS {fmt_pct(rt_roas)} / 목표 1,000% = **{rt_okr:.1f}%** ❌" if rt_okr < 100 else f"**OKR 달성률**: RT ROAS {fmt_pct(rt_roas)} / 목표 1,000% = **{rt_okr:.1f}%** ✅")
    lines.append("")

    # RT WoW 비교
    lines.append("| WoW 비교 | W12 | W11 | 방향 |")
    lines.append("|---|---|---|---|")
    for ct in rt_types:
        c = get_row(curr, ct)
        p = get_row(prev, ct)
        if c is not None and p is not None:
            diff = c["ROAS"] - p["ROAS"]
            arrow = '<span style="color:red">▲</span>' if diff > 0 else '<span style="color:blue">▼</span>'
            lines.append(f"| {ct} ROAS | {fmt_pct(c['ROAS'])} | {fmt_pct(p['ROAS'])} | {arrow} {diff:+.1f}%p |")
    lines.append("")

    # CPA 달성 현황
    lines.append("### CPA 달성 현황")
    lines.append("")
    lines.append("| 구분 | 구매CPA | 목표 10,600원 | 판정 |")
    lines.append("|---|---|---|---|")
    for label, ct in [("UA전체", "UA전체"), ("RT전체", "RT전체")]:
        r = get_row(curr, ct)
        if r is not None:
            cpa = r["구매CPA"]
            pct = cpa / 10600 * 100
            mark = "✅" if cpa <= 10600 else "❌"
            lines.append(f"| {label} | {fmt_won(cpa)} | {pct:.1f}% | {mark} |")
    lines.append("")
    lines.append("---")
    return "\n".join(lines)


def build_section3(detail_df):
    """섹션 3: 일반 기획전 상세"""
    lines = ["## 3. 일반 기획전 상세 (카이 담당)", ""]

    for ctype, label in [("UA-일반", "UA-일반"), ("RT-일반", "RT-일반")]:
        sub = detail_df[detail_df["campaign_type"] == ctype]
        weekly = sub[sub["level"] == "1. 주차별"]
        daily = sub[sub["level"] == "2. 데일리"]

        # 주차별 추이
        lines.append(f"### {label} 주차별 추이")
        lines.append("")
        if ctype == "UA-일반":
            lines.append("| 기간 | 보정비용 | CTR | CPC | CPM | ROAS | 구매CPA | 구매CVR | 옵션완료CVR | 장바구니CVR | 가입CVR | 구매완료 |")
            lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
        else:
            lines.append("| 기간 | 보정비용 | CTR | CPC | CPM | ROAS | 구매CPA | 구매CVR | 옵션완료CVR | 장바구니CVR | 구매완료 |")
            lines.append("|---|---|---|---|---|---|---|---|---|---|---|")

        for i, (_, r) in enumerate(weekly.iterrows()):
            period = str(r["period"])
            m_days = re.search(r"(\d+/\d+일)", period)
            days_label = f" ({m_days.group(1)})" if m_days else ""
            m_week = re.search(r"W(\d+)", period)
            week_num = m_week.group(0) if m_week else period[:10]
            label_str = f"**{week_num}**{days_label}" if i == 0 else week_num
            roas_str = fmt_pct(r["ROAS"])
            if i > 0 and r["ROAS"] == weekly["ROAS"].max():
                roas_str = f"**{roas_str}**"
            if ctype == "UA-일반":
                lines.append(f"| {label_str} | {fmt_cost(r['cost_adjusted'])} | {fmt_pct(r['CTR'], 2)} | {fmt_won(r['CPC'])} | {fmt_won(r['CPM'])} | {roas_str} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {fmt_pct(r['옵션완료CVR'], 2)} | {fmt_pct(r['장바구니CVR'], 2)} | {fmt_pct(r['가입CVR'], 2)} | {fmt_int(r['purchases'])} |")
            else:
                lines.append(f"| {label_str} | {fmt_cost(r['cost_adjusted'])} | {fmt_pct(r['CTR'], 2)} | {fmt_won(r['CPC'])} | {fmt_won(r['CPM'])} | {roas_str} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {fmt_pct(r['옵션완료CVR'], 2)} | {fmt_pct(r['장바구니CVR'], 2)} | {fmt_int(r['purchases'])} |")
        lines.append("")

        # 데일리
        lines.append(f"### {label} 데일리")
        lines.append("")
        if ctype == "UA-일반":
            lines.append("| 날짜 | 보정비용 | CTR | CPC | CPM | ROAS | 구매CPA | 구매CVR | 가입CVR | 구매완료 |")
            lines.append("|---|---|---|---|---|---|---|---|---|---|")
        else:
            lines.append("| 날짜 | 보정비용 | CTR | CPC | CPM | ROAS | 구매CPA | 구매CVR | 장바구니CVR | 구매완료 |")
            lines.append("|---|---|---|---|---|---|---|---|---|---|")

        if not daily.empty:
            min_roas = daily["ROAS"].min()
            max_roas = daily["ROAS"].max()
        for i, (_, r) in enumerate(daily.iterrows()):
            period = str(r["period"])
            label_str = f"**{period}**" if i == 0 else period
            roas_str = fmt_pct(r["ROAS"])
            cpa_str = fmt_won(r["구매CPA"])
            ctr_str = fmt_pct(r["CTR"], 2)
            cvr_str = fmt_pct(r["구매CVR"], 2)
            # 최저/최고 볼드
            if r["ROAS"] == min_roas:
                roas_str = f"**{roas_str}**"
            if r["CTR"] == daily["CTR"].min():
                ctr_str = f"**{ctr_str}**"
            if r["구매CVR"] == daily["구매CVR"].max() and i == 0:
                cvr_str = f"**{cvr_str}**"
            if ctype == "UA-일반":
                lines.append(f"| {label_str} | {fmt_cost(r['cost_adjusted'])} | {ctr_str} | {fmt_won(r['CPC'])} | {fmt_won(r['CPM'])} | {roas_str} | {cpa_str} | {cvr_str} | {fmt_pct(r['가입CVR'], 2)} | {fmt_int(r['purchases'])} |")
            else:
                lines.append(f"| {label_str} | {fmt_cost(r['cost_adjusted'])} | {ctr_str} | {fmt_won(r['CPC'])} | {fmt_won(r['CPM'])} | {roas_str} | {cpa_str} | {cvr_str} | {fmt_pct(r['장바구니CVR'], 2)} | {fmt_int(r['purchases'])} |")
        lines.append("")

    lines.append("---")
    return "\n".join(lines)


def build_section4(adgroup_df):
    """섹션 4: 일반 기획전 그룹별 성과"""
    total_count = len(adgroup_df)
    ua = adgroup_df[adgroup_df["campaign_type"] == "UA-일반"].copy()
    rt = adgroup_df[adgroup_df["campaign_type"] == "RT-일반"].copy()
    winners = adgroup_df[adgroup_df["success_type"] == "Winner"]
    ltv = adgroup_df[adgroup_df["success_type"] == "저가트래픽(LTV)"]

    lines = [
        "## 4. 일반 기획전 그룹별 성과",
        "",
        f"> 필터: 보정비용 ≥ 1만원 집행 Ad Group 대상 ({total_count}건, BZ/DP 지면 분리)",
        "> 정렬: **보정비용 내림차순** (핵심 그룹 우선 파악)",
        "",
    ]

    # --- Winner ---
    lines.append(f"### Winner (UA ROAS ≥ 600% / RT ROAS ≥ 1,000%) — {len(winners)}건")
    lines.append("")
    lines.append("| Ad Group | 지면 | 유형 | 보정비용 | spend | ROAS | 구매CPA | 구매CVR | 가입CVR | 3일추이 | 구매완료 |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for _, r in winners.iterrows():
        name = fmt_adgroup_name(r)
        trend = fmt_trend(r["3일_ROAS추세"])
        cost_chg = str(r["3일_비용변화"]) if pd.notna(r["3일_비용변화"]) and r["3일_비용변화"] != "N/A" else ""
        trend_display = f"{trend} 비용{cost_chg}" if cost_chg and cost_chg != "N/A" else trend
        utype = "UA" if "UA" in str(r["campaign_type"]) else "RT"
        lines.append(f"| {name} | {r['placement']} | {utype} | {fmt_cost(r['cost_adjusted'])} | {r['spend_share(%)']:.1f}% | **{fmt_pct(r['ROAS'])}** | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {fmt_pct(r['가입CVR'], 2)} | {trend_display} | {fmt_int(r['purchases'])} |")
    lines.append("")

    # --- UA-일반 전체 ---
    lines.append(f"### UA-일반 전체 그룹 성과 ({len(ua)}건, 보정비용 내림차순)")
    lines.append("")
    lines.append("| Ad Group | 지면 | 분류 | 보정비용 | spend | CTR | CPC | ROAS | 구매CPA | 구매CVR | 가입CVR | 3일추이 | 비용변화 | 구매완료 |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for _, r in ua.iterrows():
        name = fmt_adgroup_name(r)
        tier = fmt_tier_short(r["success_type"])
        trend = fmt_trend(r["3일_ROAS추세"])
        cost_chg = str(r["3일_비용변화"]) if pd.notna(r["3일_비용변화"]) and r["3일_비용변화"] != "N/A" else "N/A"
        roas_str = fmt_pct(r["ROAS"])
        if r["ROAS"] == 0:
            roas_str = f"**{roas_str}**"
        lines.append(f"| {name} | {r['placement']} | {tier} | {fmt_cost(r['cost_adjusted'])} | {r['spend_share(%)']:.1f}% | {fmt_pct(r['CTR'], 2)} | {fmt_won(r['CPC'])} | {roas_str} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {fmt_pct(r['가입CVR'], 2)} | {trend} | {cost_chg} | {int(r['purchases'])} |")
    lines.append("")
    lines.append("> W=Winner, P=Potential, LTV=저가트래픽, F=Fail")
    lines.append('> 3일추이: <span style="color:red">▲</span>/<span style="color:blue">▼</span> = ±30%p 이상 변동 | → = 변동 미미(±30%p 이내) | N/A = 비교 데이터 부족')
    lines.append("")

    # --- RT-일반 전체 ---
    lines.append(f"### RT-일반 전체 그룹 성과 ({len(rt)}건, 보정비용 내림차순)")
    lines.append("")
    lines.append("| Ad Group | 지면 | 분류 | 보정비용 | spend | CTR | CPC | ROAS | 구매CPA | 구매CVR | 3일추이 | 비용변화 | 구매완료 |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for _, r in rt.iterrows():
        name = fmt_adgroup_name(r)
        tier = fmt_tier_short(r["success_type"])
        trend = fmt_trend(r["3일_ROAS추세"])
        cost_chg = str(r["3일_비용변화"]) if pd.notna(r["3일_비용변화"]) and r["3일_비용변화"] != "N/A" else "N/A"
        roas_str = fmt_pct(r["ROAS"])
        if r["ROAS"] == 0:
            roas_str = f"**{roas_str}**"
        lines.append(f"| {name} | {r['placement']} | {tier} | {fmt_cost(r['cost_adjusted'])} | {r['spend_share(%)']:.1f}% | {fmt_pct(r['CTR'], 2)} | {fmt_won(r['CPC'])} | {roas_str} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {trend} | {cost_chg} | {int(r['purchases'])} |")
    lines.append("")
    lines.append("> P=Potential, F=Fail")
    lines.append('> 3일추이: <span style="color:red">▲</span>/<span style="color:blue">▼</span> = ±30%p 이상 변동 | → = 변동 미미(±30%p 이내) | N/A = 비교 데이터 부족')
    lines.append("")

    # --- 저가트래픽 ---
    if not ltv.empty:
        lines.append(f"### 저가트래픽(LTV) — {len(ltv)}건")
        lines.append("")
        lines.append("> RT 씨드(seed) 확보 관점: 가입CPA 기준으로 평가")
        lines.append("")
        lines.append("| Ad Group | 유형 | 보정비용 | 가입CPA | 가입CVR | CPC | ROAS |")
        lines.append("|---|---|---|---|---|---|---|")
        for _, r in ltv.iterrows():
            name = fmt_adgroup_name(r)
            utype = f"{r['campaign_type'].replace('-일반', '')}-{r['placement']}"
            lines.append(f"| {name} | {utype} | {fmt_cost(r['cost_adjusted'])} | {fmt_won(r['가입CPA'])} | {fmt_pct(r['가입CVR'], 2)} | {fmt_won(r['CPC'])} | {fmt_pct(r['ROAS'])} |")
        lines.append("")

    lines.append("---")
    return "\n".join(lines)


def build_section5(adgroup_df):
    """섹션 5: 성공 기획전 발굴율"""
    total = len(adgroup_df)
    ua = adgroup_df[adgroup_df["campaign_type"] == "UA-일반"]
    rt = adgroup_df[adgroup_df["campaign_type"] == "RT-일반"]

    # 분류 집계
    tiers = ["Winner", "Potential", "저가트래픽(LTV)", "Fail"]
    counts = {t: len(adgroup_df[adgroup_df["success_type"] == t]) for t in tiers}

    lines = [
        "## 5. 성공 기획전 발굴율",
        "",
        "### W12 분류 요약 (BZ/DP 분리 후)",
        "",
        "| 분류 | 건수 | 비율 | 기준 |",
        "|---|---|---|---|",
    ]
    criteria = {
        "Winner": "UA ≥ 600% / RT ≥ 1,000%",
        "Potential": "UA ≥ 400% or CVR ≥ 1.0% / RT ≥ 600% or CVR ≥ 1.0%",
        "저가트래픽(LTV)": "CPC ≤ 150원 or 가입CPA ≤ 5,000원",
        "Fail": "상기 미해당",
    }
    for t in tiers:
        c = counts[t]
        pct = c / total * 100 if total > 0 else 0
        lines.append(f"| **{t}** | {c}건 | {pct:.1f}% | {criteria[t]} |")
    lines.append(f"| **합계** | **{total}건** | 100% | |")
    lines.append("")

    # 유형별 분포
    lines.append("### 유형별 분포")
    lines.append("")
    lines.append("| 분류 | UA-일반 | RT-일반 |")
    lines.append("|---|---|---|")
    ua_total = len(ua)
    rt_total = len(rt)
    for t in tiers:
        ua_c = len(ua[ua["success_type"] == t])
        rt_c = len(rt[rt["success_type"] == t])
        ua_pct = ua_c / ua_total * 100 if ua_total > 0 else 0
        rt_pct = rt_c / rt_total * 100 if rt_total > 0 else 0
        lines.append(f"| {t} | {ua_c}건 ({ua_pct:.1f}%) | {rt_c}건 ({rt_pct:.1f}%) |")
    lines.append(f"| **합계** | **{ua_total}건** | **{rt_total}건** |")
    lines.append("")

    # 지면별 성과 비교 (BZ/DP 모두 있는 그룹)
    # ad_group + campaign_type 기준으로 BZ/DP 모두 있는 그룹 찾기
    grouped = adgroup_df.groupby(["ad_group", "campaign_type"])["placement"].apply(set).reset_index()
    both = grouped[grouped["placement"].apply(lambda x: "BZ" in x and "DP" in x)]

    if not both.empty:
        lines.append("### 지면별 성과 비교 (주목할 그룹)")
        lines.append("")
        lines.append("| Ad Group | 유형 | BZ ROAS | DP ROAS | 차이 | 시사점 |")
        lines.append("|---|---|---|---|---|---|")
        for _, grp in both.iterrows():
            ag = grp["ad_group"]
            ct = grp["campaign_type"]
            bz_row = adgroup_df[(adgroup_df["ad_group"] == ag) & (adgroup_df["campaign_type"] == ct) & (adgroup_df["placement"] == "BZ")]
            dp_row = adgroup_df[(adgroup_df["ad_group"] == ag) & (adgroup_df["campaign_type"] == ct) & (adgroup_df["placement"] == "DP")]
            if bz_row.empty or dp_row.empty:
                continue
            bz = bz_row.iloc[0]
            dp = dp_row.iloc[0]
            diff = bz["ROAS"] - dp["ROAS"]
            bz_tier = fmt_tier_short(bz["success_type"])
            dp_tier = fmt_tier_short(dp["success_type"])
            utype = "UA" if "UA" in ct else "RT"
            # 시사점 자동 판단
            if dp_tier == "F" and bz_tier in ("W", "P") and diff > 200:
                hint = "**DP OFF 검토**"
            elif diff > 200:
                hint = "DP 소재 개선 검토"
            else:
                hint = "DP 유지하되 모니터링"
            # 브랜드명 추출
            brand = str(bz.get("Brand", "")).strip()
            if not brand or brand == "nan":
                brand = ag.split("-")[-1] if "-" in ag else ag
            lines.append(f"| {brand} | {utype} | {fmt_pct(bz['ROAS'])} ({bz_tier}) | {fmt_pct(dp['ROAS'])} ({dp_tier}) | {diff:+.1f}%p | {hint} |")
        lines.append("")

    lines.append("---")
    return "\n".join(lines)


def build_section6_placeholder():
    """섹션 6: LLM 플레이스홀더"""
    return "{{INSIGHTS_AND_ACTIONS}}"


def build_footer(ref_date):
    """리포트 하단"""
    today = datetime.now().strftime("%Y-%m-%d")
    return f"\n---\n\n*생성일: {today} | 데이터 기준: {ref_date.strftime('%Y-%m-%d')}*\n"


# ─────────────────────────────────────────
# Insights Context 생성 (LLM용 소형 요약)
# ─────────────────────────────────────────

def build_insights_context(total_df, by_type_df, detail_df, adgroup_df, ref_date):
    """LLM이 읽을 핵심 수치 요약 (50~80줄)"""
    lines = [f"# Weekly Insights Context (기준일: {ref_date.strftime('%Y-%m-%d')})", ""]

    # 1. 3주 ROAS 추이
    weekly = total_df[total_df["level"] == "2. 주차별"]
    lines.append("## 3주 ROAS 추이")
    for _, r in weekly.iterrows():
        m = re.search(r"W(\d+)", str(r["period"]))
        wk = m.group(0) if m else "?"
        lines.append(f"- {wk}: ROAS {r['ROAS']:.1f}% | CPA {r['구매CPA']:,.0f}원 | CVR {r['구매CVR']:.2f}% | 구매 {r['purchases']:,.0f}건")
    lines.append("")

    # 2. OKR 달성률
    weeks = by_type_df["week"].unique()
    curr = by_type_df[by_type_df["week"] == weeks[0]]
    prev = by_type_df[by_type_df["week"] == weeks[1]] if len(weeks) > 1 else pd.DataFrame()

    lines.append("## OKR 달성률")
    for ct in ["UA전체", "RT전체"]:
        r = curr[curr["campaign_type"] == ct]
        if not r.empty:
            r = r.iloc[0]
            target = 600 if "UA" in ct else 1000
            pct = r["ROAS"] / target * 100
            lines.append(f"- {ct}: ROAS {r['ROAS']:.1f}% / 목표 {target}% = {pct:.1f}%")
    for ct in ["UA전체", "RT전체"]:
        r = curr[curr["campaign_type"] == ct]
        if not r.empty:
            lines.append(f"- {ct} CPA: {r.iloc[0]['구매CPA']:,.0f}원 / 목표 10,600원")
    lines.append("")

    # 3. 유형별 WoW 변화
    lines.append("## 유형별 WoW 변화 (W12 vs W11)")
    for ct in ["UA-일반", "UA-SEL", "UA-PBTD", "RT-일반", "RT-SEL", "RT-PBTD", "RT-카탈로그", "AD-PBTD"]:
        c = curr[curr["campaign_type"] == ct]
        p = prev[prev["campaign_type"] == ct] if not prev.empty else pd.DataFrame()
        if not c.empty and not p.empty:
            diff = c.iloc[0]["ROAS"] - p.iloc[0]["ROAS"]
            lines.append(f"- {ct}: {c.iloc[0]['ROAS']:.1f}% (WoW {diff:+.1f}%p)")
    lines.append("")

    # 4. UA/RT 일반 퍼널 변화
    lines.append("## 퍼널 전환율 (UA-일반)")
    ua_detail = detail_df[detail_df["campaign_type"] == "UA-일반"]
    ua_weekly = ua_detail[ua_detail["level"] == "1. 주차별"]
    for _, r in ua_weekly.iterrows():
        m = re.search(r"W(\d+)", str(r["period"]))
        wk = m.group(0) if m else "?"
        opt2cart = r["장바구니CVR"] / r["옵션완료CVR"] * 100 if r["옵션완료CVR"] > 0 else 0
        cart2buy = r["구매CVR"] / r["장바구니CVR"] * 100 if r["장바구니CVR"] > 0 else 0
        lines.append(f"- {wk}: 옵션완료 {r['옵션완료CVR']:.2f}% → 장바구니 {r['장바구니CVR']:.2f}% ({opt2cart:.1f}%) → 구매 {r['구매CVR']:.2f}% ({cart2buy:.1f}%)")
    lines.append("")

    lines.append("## 퍼널 전환율 (RT-일반)")
    rt_detail = detail_df[detail_df["campaign_type"] == "RT-일반"]
    rt_weekly = rt_detail[rt_detail["level"] == "1. 주차별"]
    for _, r in rt_weekly.iterrows():
        m = re.search(r"W(\d+)", str(r["period"]))
        wk = m.group(0) if m else "?"
        opt2cart = r["장바구니CVR"] / r["옵션완료CVR"] * 100 if r["옵션완료CVR"] > 0 else 0
        cart2buy = r["구매CVR"] / r["장바구니CVR"] * 100 if r["장바구니CVR"] > 0 else 0
        lines.append(f"- {wk}: 옵션완료 {r['옵션완료CVR']:.2f}% → 장바구니 {r['장바구니CVR']:.2f}% ({opt2cart:.1f}%) → 구매 {r['구매CVR']:.2f}% ({cart2buy:.1f}%)")
    lines.append("")

    # 5. 데일리 최근 3일 (회복/하락 시그널)
    lines.append("## 데일리 최근 3일")
    daily = total_df[total_df["level"] == "3. 데일리"].head(3)
    for _, r in daily.iterrows():
        lines.append(f"- {r['period']}: ROAS {r['ROAS']:.1f}% | CPA {r['구매CPA']:,.0f}원 | CVR {r['구매CVR']:.2f}%")
    lines.append("")

    # 6. Winner 목록
    winners = adgroup_df[adgroup_df["success_type"] == "Winner"]
    lines.append(f"## Winner ({len(winners)}건)")
    for _, r in winners.iterrows():
        name = fmt_adgroup_name(r)
        trend = r['3일_ROAS추세'] if pd.notna(r['3일_ROAS추세']) else "N/A"
        lines.append(f"- {name} {r['placement']} ({r['campaign_type']}): ROAS {r['ROAS']:.1f}% | 비용 {fmt_cost(r['cost_adjusted'])} ({r['spend_share(%)']:.1f}%) | 추이 {trend}")
    lines.append("")

    # 7. 3일추이 상승 TOP 5 (비용 고려)
    has_trend = adgroup_df[adgroup_df["3일_ROAS추세"].str.startswith("▲", na=False)].copy()
    if not has_trend.empty:
        # 추이에서 수치 추출
        def extract_trend_val(s):
            m = re.search(r"[+-]?\d+", str(s))
            return int(m.group()) if m else 0
        has_trend["_trend_val"] = has_trend["3일_ROAS추세"].apply(extract_trend_val)
        has_trend = has_trend.sort_values("_trend_val", ascending=False).head(5)
        lines.append("## 상승 시그널 TOP 5")
        for _, r in has_trend.iterrows():
            name = fmt_adgroup_name(r)
            lines.append(f"- {name} {r['placement']} ({r['campaign_type']}): {r['3일_ROAS추세']} | ROAS {r['ROAS']:.1f}% | 비용 {fmt_cost(r['cost_adjusted'])}")
        lines.append("")

    # 8. 3일추이 하락 TOP 5 (비용 고려)
    has_down = adgroup_df[adgroup_df["3일_ROAS추세"].str.startswith("▼", na=False)].copy()
    if not has_down.empty:
        has_down["_trend_val"] = has_down["3일_ROAS추세"].apply(extract_trend_val)
        has_down = has_down.sort_values("_trend_val").head(5)
        lines.append("## 하락 시그널 TOP 5")
        for _, r in has_down.iterrows():
            name = fmt_adgroup_name(r)
            lines.append(f"- {name} {r['placement']} ({r['campaign_type']}): {r['3일_ROAS추세']} | ROAS {r['ROAS']:.1f}% | 비용 {fmt_cost(r['cost_adjusted'])} | 비용변화 {r['3일_비용변화']}")
        lines.append("")

    # 9. RT Winner 근접 (ROAS 700%+, RT-일반)
    rt_high = adgroup_df[(adgroup_df["campaign_type"] == "RT-일반") & (adgroup_df["ROAS"] >= 700) & (adgroup_df["success_type"] != "Winner")].sort_values("ROAS", ascending=False)
    if not rt_high.empty:
        lines.append("## RT Winner 근접 후보 (ROAS 700%+)")
        for _, r in rt_high.iterrows():
            name = fmt_adgroup_name(r)
            lines.append(f"- {name} {r['placement']}: ROAS {r['ROAS']:.1f}% | 비용 {fmt_cost(r['cost_adjusted'])} | 추이 {r['3일_ROAS추세']}")
        lines.append("")

    # 10. OFF 대상 (UA Fail ROAS 200% 미만)
    ua_fail_low = adgroup_df[(adgroup_df["campaign_type"] == "UA-일반") & (adgroup_df["success_type"] == "Fail") & (adgroup_df["ROAS"] < 200)].sort_values("ROAS")
    if not ua_fail_low.empty:
        total_save = ua_fail_low["cost_adjusted"].sum()
        lines.append(f"## OFF 대상 (UA Fail ROAS <200%, {len(ua_fail_low)}건, 절감 ~{fmt_cost(total_save)})")
        for _, r in ua_fail_low.iterrows():
            name = fmt_adgroup_name(r)
            lines.append(f"- {name} {r['placement']}: ROAS {r['ROAS']:.1f}% | 비용 {fmt_cost(r['cost_adjusted'])}")
        lines.append("")

    # 11. 분류 요약
    lines.append("## 분류 요약")
    for t in ["Winner", "Potential", "저가트래픽(LTV)", "Fail"]:
        c = len(adgroup_df[adgroup_df["success_type"] == t])
        lines.append(f"- {t}: {c}건 ({c/len(adgroup_df)*100:.1f}%)")

    return "\n".join(lines)


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="주간 성과 리포트 자동 생성")
    parser.add_argument("--date", required=True, help="기준 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    ref_date = pd.Timestamp(args.date)
    iso_week = ref_date.isocalendar()[1]

    # CSV 로드
    total_df, by_type_df, detail_df, adgroup_df = load_csvs()

    # 리포트 조립
    sections = [
        build_header(ref_date, adgroup_df),
        "",
        build_section1(total_df),
        "",
        build_section2(by_type_df),
        "",
        build_section3(detail_df),
        "",
        build_section4(adgroup_df),
        "",
        build_section5(adgroup_df),
        "",
        build_section6_placeholder(),
        "",
        build_footer(ref_date),
    ]
    report = "\n".join(sections)

    # 리포트 저장
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"weekly-performance-{ref_date.year}-W{iso_week}.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"리포트 → {report_path}")

    # Insights context 저장
    context = build_insights_context(total_df, by_type_df, detail_df, adgroup_df, ref_date)
    context_path = OUTPUT_DIR / "weekly-insights-context.md"
    context_path.write_text(context, encoding="utf-8")
    print(f"인사이트 컨텍스트 → {context_path}")


if __name__ == "__main__":
    main()
