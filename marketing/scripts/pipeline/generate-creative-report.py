"""
소재 성과 리포트 자동 생성 스크립트
- outputs/processed/creative-performance.csv 및 creative-elements.csv를 읽어 마크다운 리포트 생성
- 5만원 이상 소진 필터 적용 (Top/Bottom)
- LLM용 creative-insights-context.md 생성
"""

import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

# === 경로 ===
# marketing/scripts/pipeline/ → 4단계 상위 = joo-mkt/
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
OUTPUT_DIR = BASE_DIR / "outputs" / "processed" / "pipeline"
REPORT_DIR = BASE_DIR / "marketing" / "reports" / "creative"

# ─────────────────────────────────────────
# 포맷팅 유틸리티
# ─────────────────────────────────────────

def fmt_cost(v):
    if pd.isna(v) or v == 0: return "0"
    man = v / 10000
    if man >= 1000: return f"{man:,.0f}만"
    return f"{man:,.1f}만"

def fmt_pct(v, d=1):
    if pd.isna(v) or v == 0: return "0.0%"
    return f"{v:.{d}f}%"

def fmt_won(v):
    if pd.isna(v) or v == 0: return "-"
    return f"{v:,.0f}원"

def fmt_int(v):
    if pd.isna(v) or v == 0: return "0"
    return f"{v:,.0f}"

# ─────────────────────────────────────────
# 섹션 빌더
# ─────────────────────────────────────────

def build_report(df, ele_df, ref_date):
    iso_date = ref_date.strftime("%Y-%m-%d")
    
    lines = [
        f"# 소재 성과 분석 리포트 — {iso_date}",
        "",
        f"> **분석 기간**: 당월 (01일 ~ {iso_date})",
        "> **데이터 소스**: Airbridge Raw → `preprocess-airbridge.py` (Step 1)",
        "> **비용 기준**: 보정비용",
        "",
        "---",
        "",
        "{{EXECUTIVE_SUMMARY}}",
        "",
        "---",
        "",
        "## 1. 소재 성과 종합",
        ""
    ]
    
    # 전체 요약
    total = df[df["cost_adjusted"] > 0]
    avg_roas = (total["revenue"].sum() / total["cost_adjusted"].sum() * 100) if total["cost_adjusted"].sum() > 0 else 0
    avg_ctr = (total["clicks"].sum() / total["impressions"].sum() * 100) if total["impressions"].sum() > 0 else 0
    total_spend = total["cost_adjusted"].sum()
    
    lines.append("| 지표 | 수치 |")
    lines.append("|---|---|")
    lines.append(f"| 총 보정비용 | {fmt_cost(total_spend)} |")
    lines.append(f"| 평균 ROAS | {fmt_pct(avg_roas)} |")
    lines.append(f"| 평균 CTR | {fmt_pct(avg_ctr, 2)} |")
    lines.append("")
    
    # Top/Bottom 10 (소진 5만원 이상)
    df_50k = df[df["cost_adjusted"] >= 50000].copy()
    
    lines.append("## 2. 효율 소재 Top 10 (소진 5만원↑)")
    lines.append("")
    lines.append("| 소재명 | 유형 | 보정비용 | ROAS | CTR | 구매CPA | 구매CVR | RPI |")
    lines.append("|---|---|---|---|---|---|---|---|")
    top_roas = df_50k.sort_values("ROAS", ascending=False).head(10)
    for _, r in top_roas.iterrows():
        lines.append(f"| {r['creative']} | {r['campaign_type']} | {fmt_cost(r['cost_adjusted'])} | **{fmt_pct(r['ROAS'])}** | {fmt_pct(r['CTR'], 2)} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {r['RPI']}% |")
    lines.append("")
    
    lines.append("## 3. 저효율 소재 Bottom 10 (소진 5만원↑)")
    lines.append("")
    lines.append("| 소재명 | 유형 | 보정비용 | ROAS | CTR | 구매CPA | 구매CVR | RPI |")
    lines.append("|---|---|---|---|---|---|---|---|")
    bot_roas = df_50k.sort_values("ROAS", ascending=True).head(10)
    for _, r in bot_roas.iterrows():
        lines.append(f"| {r['creative']} | {r['campaign_type']} | {fmt_cost(r['cost_adjusted'])} | {fmt_pct(r['ROAS'])} | {fmt_pct(r['CTR'], 2)} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} | {r['RPI']}% |")
    lines.append("")
    
    lines.append("## 4. 요소별 성과 (Elements)")
    lines.append("")
    for cat in ["option_main", "option_detailed", "brand"]:
        sub = ele_df[ele_df["category"] == cat].sort_values("cost_adjusted", ascending=False)
        lines.append(f"### {cat.replace('_', ' ').title()}별 성과")
        lines.append("")
        lines.append("| 구분 | 보정비용 | ROAS | CTR | 구매CPA | 구매CVR |")
        lines.append("|---|---|---|---|---|---|")
        for _, r in sub.iterrows():
            lines.append(f"| {r['value']} | {fmt_cost(r['cost_adjusted'])} | {fmt_pct(r['ROAS'])} | {fmt_pct(r['CTR'], 2)} | {fmt_won(r['구매CPA'])} | {fmt_pct(r['구매CVR'], 2)} |")
        lines.append("")
        
    lines.append("---")
    lines.append("")
    lines.append("{{INSIGHTS_AND_ACTIONS}}")
    lines.append("")
    lines.append(f"\n*생성일: {datetime.now().strftime('%Y-%m-%d')} | 데이터 기준: {iso_date}*")
    
    return "\n".join(lines)


def build_context(df, ele_df, ref_date):
    """LLM용 핵심 수약"""
    iso_date = ref_date.strftime("%Y-%m-%d")
    df_50k = df[df["cost_adjusted"] >= 50000].copy()
    
    lines = [
        f"# Creative Insights Context (기준일: {iso_date})",
        "",
        "## 1. 베스트 소재 (ROAS TOP 5, 소진 5만↑)",
    ]
    for _, r in df_50k.sort_values("ROAS", ascending=False).head(5).iterrows():
        lines.append(f"- {r['creative']}: ROAS {r['ROAS']:.1f}% | CTR {r['CTR']:.2f}% | 경과일 {r['days_active']}일 | RPI {r['RPI']}%")
    
    lines.append("\n## 2. 워스트 소재 (ROAS BOT 5, 소진 5만↑)")
    for _, r in df_50k.sort_values("ROAS", ascending=True).head(5).iterrows():
        lines.append(f"- {r['creative']}: ROAS {r['ROAS']:.1f}% | CVR {r['구매CVR']:.2f}% | CPA {r['구매CPA']:,.0f}원")
        
    lines.append("\n## 3. 요소별 효율 (Option Main)")
    main_ele = ele_df[ele_df["category"] == "option_main"].sort_values("ROAS", ascending=False)
    for _, r in main_ele.iterrows():
        lines.append(f"- {r['value']}: ROAS {r['ROAS']:.1f}% | CTR {r['CTR']:.2f}% | 비중 {r['cost_adjusted']/ele_df[ele_df['category']=='option_main']['cost_adjusted'].sum()*100:.1f}%")

    lines.append("\n## 4. 피로도 징후 (투입 14일 초과 중 하위)")
    old_cre = df[(df["days_active"] > 14) & (df["cost_adjusted"] > 10000)].sort_values("ROAS").head(5)
    for _, r in old_cre.iterrows():
        lines.append(f"- {r['creative']}: ROAS {r['ROAS']:.1f}% | 투입후 {r['days_active']}일 | 누적소진 {fmt_cost(r['cost_adjusted'])}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    ref_date = pd.Timestamp(args.date)
    
    df = pd.read_csv(OUTPUT_DIR / "creative-performance.csv")
    ele_df = pd.read_csv(OUTPUT_DIR / "creative-elements.csv")
    
    report = build_report(df, ele_df, ref_date)
    context = build_context(df, ele_df, ref_date)
    
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"creative-performance-{args.date}.md"
    report_path.write_text(report, encoding="utf-8")
    
    context_path = OUTPUT_DIR / "creative-insights-context.md"
    context_path.write_text(context, encoding="utf-8")
    
    print(f"소재 리포트 → {report_path}")
    print(f"인사이트 컨텍스트 → {context_path}")

if __name__ == "__main__":
    main()
