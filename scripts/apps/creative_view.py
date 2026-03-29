import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import datetime, timedelta
import re
import os
import json
import uuid

# === 설정 ===
st.set_page_config(page_title="Growth Creative Dashboard v5", layout="wide")

# === 경로 설정 (로컬 및 클라우드 배포 통합) ===
CUR_DIR = Path(__file__).resolve().parent
BASE_DIR = CUR_DIR.parent.parent

# 1. 데이터 디렉토리 설정
# joo-mkt/data/processed/dashboard/
DATA_DIR = BASE_DIR / "data" / "processed" / "dashboard"
CONFIG_DIR = BASE_DIR / "docs" / "taxonomy"

CLEANED_DATA_PATH = DATA_DIR / "kakao_cleaned.csv"
CACHE_PATH = DATA_DIR / "kakao_dashboard_final_v4.parquet"

# 2. 파일 경로 설정
MEMO_PATH = CONFIG_DIR / "memo.md"
ACTION_LOG_PATH = CONFIG_DIR / "action_log.json"
WINNING_HISTORY_PATH = CONFIG_DIR / "winning_history.json"
AB_MEMO_PATH = CONFIG_DIR / "ab_memo.json"

# === 상수 ===
NUM_COLS = ['cost', 'revenue', 'purchases', 'clicks', 'impressions', 'cart', 'option_comp']
AGG_DICT = {c: 'sum' for c in NUM_COLS}
# C-1: purchases 제거
DISPLAY_COLS = ['cost', 'CTR', 'CPC', 'ROAS', 'CPA', 'CVR', 'Option_CVR', 'Cart_CVR', 'AOV']
# C-5: 재사용 지표 선택 목록
METRIC_OPTIONS = ["ROAS", "CTR", "CPC", "CVR", "CPA"]

# === JSON 파일 I/O 헬퍼 ===
def _load_json(path):
    """JSON 파일 로드. 없으면 빈 리스트 반환."""
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def _save_json(path, data):
    """JSON 파일 저장."""
    os.makedirs(path.parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# === 1. 데이터 로드 및 스마트 캐시 엔진 ===
@st.cache_data
def load_v4_data_optimized():
    CACHE_PATH = DATA_DIR / "kakao_dashboard_final_v4.parquet"

    if CLEANED_DATA_PATH.exists():
        csv_mtime = os.path.getmtime(CLEANED_DATA_PATH)
        cache_exists = CACHE_PATH.exists()
        cache_mtime = os.path.getmtime(CACHE_PATH) if cache_exists else 0

        # creative_naming.py 수정 시각도 캐시 무효화 조건에 포함
        _naming_path = CUR_DIR.parent / 'scripts' / 'pipeline' / 'creative_naming.py'
        naming_mtime = os.path.getmtime(_naming_path) if _naming_path.exists() else 0

        # 캐시가 없거나 CSV/정규화 모듈이 더 최신인 경우 재생성
        if not cache_exists or csv_mtime > cache_mtime or naming_mtime > cache_mtime:
            with st.spinner("🚀 대시보드 구동용 최종 데이터 셋 생성 중 (최초 1회)..."):
                cols = [
                    'Event Date', 'Ad Creative', 'Ad Creative ID', 'Ad Group', 'Campaign',
                    '보정비용', '구매액 (App+Web)', '구매 완료 (App+Web)',
                    'Clicks (Channel)', 'Impressions (Channel)', '캠페인유형', '지면',
                    '장바구니 담기 (App+Web)', 'completeProductOption (App+Web)'
                ]
                df = pd.read_csv(CLEANED_DATA_PATH, usecols=cols, encoding="utf-8-sig")

                # 컬럼명 통일
                df = df.rename(columns={
                    'Event Date': 'date', 'Ad Creative ID': 'creative_id', 'Ad Creative': 'creative',
                    'Ad Group': 'ad_group', 'Campaign': 'campaign', '보정비용': 'cost',
                    '구매액 (App+Web)': 'revenue', '구매 완료 (App+Web)': 'purchases',
                    'Clicks (Channel)': 'clicks', 'Impressions (Channel)': 'impressions',
                    '캠페인유형': 'campaign_type', '지면': 'placement',
                    '장바구니 담기 (App+Web)': 'cart', 'completeProductOption (App+Web)': 'option_comp'
                })
                df['date'] = pd.to_datetime(df['date'])

                # NaN/카탈로그/기타 제거
                df = df.dropna(subset=['campaign', 'ad_group', 'creative'])
                df = df[~df['campaign'].str.contains('catalog|Conversion_Catalog', case=False, na=False)]
                df = df[df['campaign'] != '기타']

                # 브랜드 파싱
                def parse_brand_v4(ad_group):
                    if not isinstance(ad_group, str): return "etc"
                    match = re.search(r'_br_([^_ \-]+)', ad_group)
                    return match.group(1) if match else "etc"
                df['brand'] = df['ad_group'].apply(parse_brand_v4)

                # 소재 인덱스 파싱 (네이밍 컨벤션 기반, creative_naming.py 정규화 적용)
                import sys as _sys
                _pipeline_dir = str(CUR_DIR.parent / 'scripts' / 'pipeline')
                if _pipeline_dir not in _sys.path:
                    _sys.path.insert(0, _pipeline_dir)
                from creative_naming import normalize_main, normalize_detailed, is_legacy_main, is_saletap

                SIZE_LAYOUT_CODES = {'11', '21', '34', '169', 'l', 'c', 'r'}
                SIZE_PATTERN = re.compile(r'^\d+x\d+$')
                NUMBERING_PATTERN = re.compile(r'^\d+[a-b]?$')

                def _is_size_or_number(token):
                    """SIZE값이나 넘버링인지 판별 — option이 아닌 값 필터용"""
                    if not token:
                        return False
                    return (SIZE_PATTERN.match(token)
                            or token in SIZE_LAYOUT_CODES
                            or NUMBERING_PATTERN.match(token))

                def parse_index_v4(name):
                    res = {'launch_dt': None, 'idx_type': 'etc', 'option_main': 'etc',
                           'option_detailed': 'etc', 'size_layout': 'etc', 'is_valid_index': False}
                    if not isinstance(name, str): return res
                    # 공백/Zero-Width 문자 정리
                    n = name.lower().strip()
                    n = re.sub(r'\s+', '', n)
                    n = n.replace('\u200b', '').replace('\ufeff', '')
                    date_match = re.search(r'^(\d{6})', n)
                    if date_match:
                        try:
                            res['launch_dt'] = pd.to_datetime(date_match.group(1), format='%y%m%d')
                            res['is_valid_index'] = True
                        except: pass
                    # saletap 특수 처리 (Fix-4: idx_type + size_layout 보강)
                    if is_saletap(n):
                        res['option_main'] = 'prm'
                        res['option_detailed'] = 'sale'
                        res['idx_type'] = 'img'
                        size_m = re.search(r'(\d+x\d+)', n)
                        if size_m:
                            res['size_layout'] = size_m.group(1)
                        return res
                    # 1차: 위치 기반 파싱
                    parts = name.split('-')
                    if len(parts) >= 2:
                        res['idx_type'] = parts[1]
                    option_str = None
                    if len(parts) >= 4:
                        option_str = parts[3]
                    elif len(parts) >= 3:
                        sub = parts[2].split('_', 1)
                        if len(sub) > 1:
                            option_str = sub[1]
                    if option_str:
                        opts = option_str.lower().split('_')
                        option_tokens = []
                        for o in opts:
                            if SIZE_PATTERN.match(o) or o in SIZE_LAYOUT_CODES:
                                res['size_layout'] = o
                            elif not NUMBERING_PATTERN.match(o):
                                option_tokens.append(o)
                        raw_main = option_tokens[0] if len(option_tokens) > 0 else None
                        main_norm = normalize_main(raw_main)
                        if main_norm:
                            res['option_main'] = main_norm
                        elif raw_main and is_legacy_main(raw_main):
                            res['option_main'] = 'sku'
                            res['option_detailed'] = raw_main
                        raw_detail = option_tokens[1] if len(option_tokens) > 1 else None
                        if raw_detail and res['option_detailed'] == 'etc':
                            detail_norm = normalize_detailed(raw_detail)
                            res['option_detailed'] = detail_norm if detail_norm else 'etc'
                        if res['size_layout'] == 'etc' and len(option_tokens) > 2:
                            last = option_tokens[-1]
                            if last in SIZE_LAYOUT_CODES:
                                res['size_layout'] = last
                    # 2차 fallback: 위치 기반 실패 시 토큰 순회
                    if res['option_main'] == 'etc':
                        tokens = re.split(r'[-_]', n)
                        for i, t in enumerate(tokens):
                            if _is_size_or_number(t):
                                continue
                            m = normalize_main(t)
                            if m:
                                res['option_main'] = m
                                if i + 1 < len(tokens):
                                    nt = tokens[i+1]
                                    if not _is_size_or_number(nt):
                                        d = normalize_detailed(nt)
                                        if d:
                                            res['option_detailed'] = d
                                break
                            if is_legacy_main(t):
                                res['option_main'] = 'sku'
                                res['option_detailed'] = t
                                break
                    return res

                parsed_list = df['creative'].apply(parse_index_v4).tolist()
                df['launch_dt'] = [x['launch_dt'] for x in parsed_list]
                df['idx_type'] = [x['idx_type'] for x in parsed_list]
                df['option_main'] = [x['option_main'] for x in parsed_list]
                df['option_detailed'] = [x['option_detailed'] for x in parsed_list]
                df['size_layout'] = [x['size_layout'] for x in parsed_list]
                df['is_valid_index'] = [x['is_valid_index'] for x in parsed_list]
                df['days_active'] = (df['date'] - df['launch_dt']).dt.days

                # 특성 분류
                def classify_feature(camp):
                    if not isinstance(camp, str): return '일반'
                    c = camp.upper()
                    if 'PBTD' in c: return 'PBTD'
                    if 'SEL' in c: return 'SEL'
                    if 'AD' in c: return 'AD'
                    return '일반'
                df['feature'] = df['campaign'].apply(classify_feature)

                df.to_parquet(CACHE_PATH, engine='pyarrow')

    # CSV가 없더라도 Parquet가 있다면 로드, 둘 다 없으면 에러
    if CACHE_PATH.exists():
        return pd.read_parquet(CACHE_PATH)
    else:
        st.error(f"데이터 파일({CACHE_PATH.name})을 찾을 수 없습니다. (경로: {CACHE_PATH.absolute()})")
        return pd.DataFrame()

# === 지표 계산 엔진 ===
def get_metrics_v4(df_agg):
    res = df_agg.copy()
    res['ROAS'] = (res['revenue'] / res['cost'] * 100).fillna(0)
    res['CTR'] = (res['clicks'] / res['impressions'] * 100).fillna(0)
    res['CPC'] = (res['cost'] / res['clicks']).fillna(0)
    res['CPA'] = (res['cost'] / res['purchases']).replace([np.inf, -np.inf], 0).fillna(0)
    res['CVR'] = (res['purchases'] / res['clicks'] * 100).fillna(0)
    res['AOV'] = (res['revenue'] / res['purchases']).replace([np.inf, -np.inf], 0).fillna(0)
    res['Cart_CVR'] = (res['cart'] / res['clicks'] * 100).fillna(0)
    res['Option_CVR'] = (res['option_comp'] / res['clicks'] * 100).fillna(0)
    return res

# === 데이터 로드 ===
df_raw = load_v4_data_optimized()

# === 포매팅 헬퍼 ===
def format_df_v4(df):
    """지표 컬럼별 포맷 적용 (정수/퍼센트 구분)"""
    fmt = {}
    for c in df.columns:
        if c in ('cost', 'purchases', 'CPC', 'CPA', 'AOV'):
            fmt[c] = "{:,.0f}"
        elif c in ('ROAS', 'CTR', 'CVR', 'Cart_CVR', 'Option_CVR'):
            fmt[c] = "{:,.2f}%"
    return df.style.format(fmt, na_rep="-")

def display_metrics(df, extra_cols=None):
    """지정된 지표 컬럼 + 앞에 붙일 식별 컬럼만 필터링"""
    cols = (extra_cols or []) + [c for c in DISPLAY_COLS if c in df.columns]
    return df[cols]

# C-4: Y축 한국식 원화 표기 헬퍼
def format_krw_axis(fig, axis='y', secondary=False):
    """Y축 값을 한국식 원화 표기로 변환 (2,000만, 1억 등)"""
    def _krw_text(val):
        if val >= 1e8:
            return f"{val/1e8:,.1f}억"
        elif val >= 1e4:
            return f"{val/1e4:,.0f}만"
        else:
            return f"{val:,.0f}"

    # 현재 데이터 범위에서 적절한 tick 생성
    if axis == 'y':
        if secondary:
            data_vals = [t.y for t in fig.data if hasattr(t, 'yaxis') and t.yaxis == 'y2']
        else:
            data_vals = [t.y for t in fig.data if not hasattr(t, 'yaxis') or t.yaxis != 'y2']
        if not data_vals:
            return fig
        all_vals = []
        for v in data_vals:
            if v is not None:
                all_vals.extend([x for x in v if x is not None and not np.isnan(x)])
        if not all_vals:
            return fig
        max_val = max(all_vals)
        # tick 간격 결정
        if max_val >= 5e8:
            step = 1e8
        elif max_val >= 1e8:
            step = 5e7
        elif max_val >= 5e7:
            step = 1e7
        elif max_val >= 1e7:
            step = 5e6
        elif max_val >= 5e6:
            step = 1e6
        else:
            step = 5e5
        ticks = np.arange(0, max_val + step, step)
        texts = [_krw_text(v) for v in ticks]
        if secondary:
            fig.update_yaxes(tickvals=ticks.tolist(), ticktext=texts, secondary_y=True)
        else:
            fig.update_yaxes(tickvals=ticks.tolist(), ticktext=texts, secondary_y=False)
    return fig

# === 사이드바: 필터링 ===
st.sidebar.title("🛠️ Growth Filters v5")
if not df_raw.empty:
    # 분석 기간
    max_d = df_raw['date'].max()
    min_d = df_raw['date'].min()
    d_range = st.sidebar.date_input("Analysis Period", [max_d - timedelta(days=30), max_d], min_value=min_d, max_value=max_d)

    # 1) 기획전 특성 (최상위 필터 — 캠페인 유형에 캐스케이딩)
    all_features = sorted(df_raw['feature'].dropna().unique().tolist())
    features = st.sidebar.multiselect("기획전 특성", options=all_features, default=all_features)

    # 2) 캠페인 유형 (선택된 기획전 특성에 연동)
    available_types = sorted(df_raw[df_raw['feature'].isin(features)]['campaign_type'].dropna().unique().tolist())
    default_types = [t for t in available_types if '카탈로그' not in t and '미분류' not in t and 'Traffic' not in t]
    ua_rt = st.sidebar.multiselect("캠페인 유형", options=available_types, default=default_types)

    # 3) 분석 지면
    all_placements = sorted([p for p in df_raw['placement'].dropna().unique() if p != '기타'])
    placements = st.sidebar.multiselect("분석 지면", options=all_placements, default=all_placements)

    # 캠페인 필터
    camps_all = sorted(df_raw['campaign'].dropna().unique().tolist())
    target_camps = st.sidebar.multiselect("특정 캠페인 선택 (Optional)", options=camps_all)

    # 그룹 필터 (캠페인 선택 시 해당 캠페인의 그룹만 표시)
    if target_camps:
        groups_all = sorted(df_raw[df_raw['campaign'].isin(target_camps)]['ad_group'].dropna().unique().tolist())
    else:
        groups_all = sorted(df_raw['ad_group'].dropna().unique().tolist())
    target_groups = st.sidebar.multiselect("특정 그룹 선택 (Optional)", options=groups_all)

    # 소진 금액 필터
    min_spend = st.sidebar.select_slider("최소 소진 금액 필터", options=list(range(0, 1000001, 50000)), value=50000)

    # 인덱스 필터
    filter_old = st.sidebar.checkbox("인덱스 매칭 소재만 보기 (신규 소재 집중)", value=True)

    # C-2: 비용 0원 필터
    filter_zero_cost = st.sidebar.checkbox("비용 0원 소재 제외", value=True)

    # 필터 적용
    mask = (df_raw['campaign_type'].isin(ua_rt)) & (df_raw['feature'].isin(features)) & (df_raw['placement'].isin(placements))
    if len(d_range) == 2:
        mask = mask & (df_raw['date'] >= pd.Timestamp(d_range[0])) & (df_raw['date'] <= pd.Timestamp(d_range[1]))
    if target_camps:
        mask = mask & (df_raw['campaign'].isin(target_camps))
    if target_groups:
        mask = mask & (df_raw['ad_group'].isin(target_groups))
    if filter_old:
        mask = mask & (df_raw['is_valid_index'] == True)

    df_f = df_raw[mask].copy()

    # C-2: 비용 0원 필터 적용 (소재 단위 집계 후 cost > 0만 유지)
    if filter_zero_cost:
        # 소재별 총 비용 기준으로 0원 소재 제외
        zero_cost_creatives = df_f.groupby('creative')['cost'].sum()
        zero_cost_creatives = zero_cost_creatives[zero_cost_creatives == 0].index
        df_f = df_f[~df_f['creative'].isin(zero_cost_creatives)]
else:
    st.error("데이터 로딩 실패")
    st.stop()

# === 페이지 네비게이션 ===
page = st.sidebar.radio("Navigation", [
    "1. Summary & Insights",
    "2. Top Creatives",
    "3. Detailed Trend",
    "4. A/B Testing Lab",
    "5. Advanced Analytics",
    "6. Operations Hub"
])

# ============================================================
# === PAGE 1: Summary & Insights ===
# ============================================================
if page == "1. Summary & Insights":
    st.title("🎯 소재 성과 요약 및 인사이트")

    # 1-1: KPI 카드 (6개: Cost / ROAS / CPA / CPC / CTR / CVR)
    st.subheader("Filtered Aggregate Performance")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    total_c = df_f['cost'].sum()
    total_r = df_f['revenue'].sum()
    total_p = df_f['purchases'].sum()
    total_clk = df_f['clicks'].sum()
    total_imp = df_f['impressions'].sum()

    col1.metric("Total Cost", f"{total_c/10000:,.0f}만")
    col2.metric("ROAS", f"{(total_r/total_c*100):,.1f}%" if total_c > 0 else "0%")
    col3.metric("CPA", f"{(total_c/total_p):,.0f}원" if total_p > 0 else "-")
    col4.metric("CPC", f"{(total_c/total_clk):,.0f}원" if total_clk > 0 else "-")
    col5.metric("CTR", f"{(total_clk/total_imp*100):,.2f}%" if total_imp > 0 else "0%")
    col6.metric("CVR", f"{(total_p/total_clk*100):,.2f}%" if total_clk > 0 else "0%")

    # 소재 옵션 분석 (option_main / option_detailed)
    st.divider()
    st.subheader("📍 소재 옵션별 성과 (Winning Elements)")

    # 1-3: 지표 선택 + 1-4: 저비용 이상치 처리
    we_col1, we_col2 = st.columns([1, 1])
    with we_col1:
        win_metric = st.selectbox("차트 지표 선택", METRIC_OPTIONS, key="win_metric")
    with we_col2:
        cost_threshold = st.number_input("최소 비용 기준 (원)", value=min_spend, step=50000, key="win_cost_threshold")

    c1, c2 = st.columns(2)
    with c1:
        st.write("**Option Main (메인 오브젝트: log/sku/inf...)**")
        el1_perf = get_metrics_v4(df_f.groupby('option_main').agg(AGG_DICT)).reset_index()
        # 저비용 표시
        el1_perf['_low_cost'] = el1_perf['cost'] < cost_threshold
        el1_perf['_opacity'] = el1_perf['_low_cost'].map({True: 0.3, False: 1.0})
        fig1 = px.bar(el1_perf, x='option_main', y=win_metric, color=win_metric, text_auto='.1f',
                       color_continuous_scale='Blues', opacity=el1_perf['_opacity'].tolist())
        fig1.update_layout(title=f'{win_metric} by Option Main')
        st.plotly_chart(fig1, use_container_width=True)
        # cost 비중 도넛 차트
        fig1_pie = px.pie(el1_perf, values='cost', names='option_main',
                          title='Cost 비중 (Option Main)',
                          color_discrete_sequence=px.colors.sequential.Blues_r, hole=0.3)
        fig1_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig1_pie, use_container_width=True)

    with c2:
        st.write("**Option Detailed (상세: model/multi/brand...)**")
        el2_perf = get_metrics_v4(df_f.groupby('option_detailed').agg(AGG_DICT)).reset_index()
        el2_perf['_low_cost'] = el2_perf['cost'] < cost_threshold
        el2_perf['_opacity'] = el2_perf['_low_cost'].map({True: 0.3, False: 1.0})
        fig2 = px.bar(el2_perf, x='option_detailed', y=win_metric, color=win_metric, text_auto='.1f',
                       color_continuous_scale='Oranges', opacity=el2_perf['_opacity'].tolist())
        fig2.update_layout(title=f'{win_metric} by Option Detailed')
        st.plotly_chart(fig2, use_container_width=True)
        fig2_pie = px.pie(el2_perf, values='cost', names='option_detailed',
                          title='Cost 비중 (Option Detailed)',
                          color_discrete_sequence=px.colors.sequential.Oranges_r, hole=0.3)
        fig2_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig2_pie, use_container_width=True)

    # 1-2: etc 소재 목록 표시
    etc_main = df_f[df_f['option_main'] == 'etc']
    if not etc_main.empty:
        etc_summary = etc_main.groupby('creative')['cost'].sum().sort_values(ascending=False).reset_index()
        etc_summary.columns = ['소재명', '총 비용']
        with st.expander(f"🔍 etc에 포함된 소재 목록 ({len(etc_summary)}개, 총 {etc_summary['총 비용'].sum()/10000:,.0f}만원)"):
            etc_summary['총 비용'] = etc_summary['총 비용'].apply(lambda x: f"{x:,.0f}")
            st.dataframe(etc_summary, use_container_width=True, height=300)

    # 옵션별 요약 표
    st.subheader("📊 옵션별 성과 요약")
    option_type = st.selectbox("분석 축 선택", ["placement", "campaign_type", "feature", "brand", "option_main", "option_detailed", "size_layout"])
    opt_perf = get_metrics_v4(df_f.groupby(option_type).agg(AGG_DICT)).sort_values("cost", ascending=False).reset_index()
    st.dataframe(format_df_v4(display_metrics(opt_perf, [option_type])))

# ============================================================
# === PAGE 2: Top Creatives (신규) ===
# ============================================================
elif page == "2. Top Creatives":
    st.title("🏆 우수 소재 분석")

    # 2-1: 우수 소재 TOP 15 (Fix-1: creative_id 추가)
    st.subheader("📊 소재 성과 TOP 15")
    # 소재별 집계 (최소 소진 금액 이상) — creative_id 기준 고유 식별
    cre_agg = get_metrics_v4(df_f.groupby(['creative_id', 'creative']).agg(AGG_DICT)).reset_index()
    cre_agg = cre_agg[cre_agg['cost'] >= min_spend]

    top_sort_metric = st.selectbox("정렬 기준", METRIC_OPTIONS, key="top_sort")
    top_n_count = st.slider("표시 개수", 5, 30, 15, key="top_n")

    if not cre_agg.empty:
        # CPA는 낮을수록 좋으므로 오름차순
        asc = True if top_sort_metric == "CPA" else False
        top_creatives = cre_agg.sort_values(top_sort_metric, ascending=asc).head(top_n_count)
        top_display = top_creatives[['creative_id', 'creative', 'cost', 'ROAS', 'CTR', 'CPC', 'CPA', 'CVR', 'AOV']].copy()
        top_display['cost'] = top_display['cost'].apply(lambda x: f"{x:,.0f}")
        top_display['ROAS'] = top_display['ROAS'].apply(lambda x: f"{x:,.1f}%")
        top_display['CTR'] = top_display['CTR'].apply(lambda x: f"{x:,.2f}%")
        top_display['CPC'] = top_display['CPC'].apply(lambda x: f"{x:,.0f}")
        top_display['CPA'] = top_display['CPA'].apply(lambda x: f"{x:,.0f}")
        top_display['CVR'] = top_display['CVR'].apply(lambda x: f"{x:,.2f}%")
        top_display['AOV'] = top_display['AOV'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(top_display, use_container_width=True, hide_index=True)
    else:
        st.info("필터 조건에 맞는 소재가 없습니다.")

    # 2-2: 주간 우수 소재 성과 인사이트
    st.divider()
    st.subheader("📈 주간 성과 변화 (WoW)")

    if len(d_range) == 2:
        end_date = pd.Timestamp(d_range[1])
    else:
        end_date = df_f['date'].max()

    # 최근 1주 vs 전주
    week1_start = end_date - timedelta(days=6)
    week0_end = week1_start - timedelta(days=1)
    week0_start = week0_end - timedelta(days=6)

    # Fix-2: WoW 기준 설명 캡션
    st.caption(
        f"기준: 이번 주 {week1_start.strftime('%m/%d')}~{end_date.strftime('%m/%d')} vs "
        f"전주 {week0_start.strftime('%m/%d')}~{week0_end.strftime('%m/%d')} | "
        f"최소 소진 {min_spend:,}원 이상 | 양주 모두 운영된 소재만 비교"
    )

    df_w1 = df_f[(df_f['date'] >= week1_start) & (df_f['date'] <= end_date)]
    df_w0 = df_f[(df_f['date'] >= week0_start) & (df_f['date'] <= week0_end)]

    if not df_w1.empty and not df_w0.empty:
        # Fix-1: creative_id 기준 집계
        w1_agg = get_metrics_v4(df_w1.groupby(['creative_id', 'creative']).agg(AGG_DICT)).reset_index()
        w0_agg = get_metrics_v4(df_w0.groupby(['creative_id', 'creative']).agg(AGG_DICT)).reset_index()
        w1_agg = w1_agg[w1_agg['cost'] >= min_spend]
        w0_agg = w0_agg[w0_agg['cost'] >= min_spend]

        # 두 주 모두 있는 소재만 비교 (creative_id 기준)
        common_ids = set(w1_agg['creative_id']) & set(w0_agg['creative_id'])
        if common_ids:
            w1_cmp = w1_agg[w1_agg['creative_id'].isin(common_ids)].set_index('creative_id')
            w0_cmp = w0_agg[w0_agg['creative_id'].isin(common_ids)].set_index('creative_id')

            # creative_id → creative 이름 매핑
            id_to_name = w1_cmp['creative'].to_dict()

            wow = pd.DataFrame({
                'creative_id': list(common_ids),
                'creative': [id_to_name.get(cid, '') for cid in common_ids],
                'This Week ROAS': [w1_cmp.loc[cid, 'ROAS'] if cid in w1_cmp.index else 0 for cid in common_ids],
                'Last Week ROAS': [w0_cmp.loc[cid, 'ROAS'] if cid in w0_cmp.index else 0 for cid in common_ids],
            })
            wow['Δ ROAS'] = wow['This Week ROAS'] - wow['Last Week ROAS']
            wow['Δ ROAS %'] = ((wow['This Week ROAS'] - wow['Last Week ROAS']) / wow['Last Week ROAS'].replace(0, np.nan) * 100).fillna(0)
            wow = wow.sort_values('This Week ROAS', ascending=False).head(10)

            up_count = (wow['Δ ROAS'] > 0).sum()
            down_count = (wow['Δ ROAS'] < 0).sum()
            st.info(f"📊 주간 비교 대상 소재 {len(common_ids)}개 중 상위 10개: ROAS 상승 **{up_count}개**, 하락 **{down_count}개**")

            wow_display = wow.copy()
            wow_display['This Week ROAS'] = wow_display['This Week ROAS'].apply(lambda x: f"{x:,.1f}%")
            wow_display['Last Week ROAS'] = wow_display['Last Week ROAS'].apply(lambda x: f"{x:,.1f}%")
            wow_display['Δ ROAS'] = wow_display['Δ ROAS'].apply(lambda x: f"{x:+,.1f}%p")
            wow_display['Δ ROAS %'] = wow_display['Δ ROAS %'].apply(lambda x: f"{x:+,.1f}%")
            st.dataframe(wow_display, use_container_width=True, hide_index=True)
        else:
            st.info("전주와 금주 모두 운영된 소재가 없습니다.")
    else:
        st.info("주간 비교를 위한 데이터가 부족합니다.")

    # 2-3: 옵션별 우수 소재 필터
    st.divider()
    st.subheader("🔎 옵션별 우수 소재")
    opt_filter = st.selectbox("옵션 기준", ["option_main", "option_detailed", "placement", "brand"], key="top_opt_filter")
    opt_vals = sorted(df_f[opt_filter].dropna().unique().tolist())
    sel_opt = st.selectbox(f"{opt_filter} 값 선택", opt_vals, key="top_opt_val")

    if sel_opt:
        opt_df = df_f[df_f[opt_filter] == sel_opt]
        opt_cre = get_metrics_v4(opt_df.groupby(['creative_id', 'creative']).agg(AGG_DICT)).reset_index()
        opt_cre = opt_cre[opt_cre['cost'] >= min_spend].sort_values('ROAS', ascending=False).head(10)
        if not opt_cre.empty:
            st.dataframe(format_df_v4(display_metrics(opt_cre, ['creative_id', 'creative'])), use_container_width=True)
        else:
            st.info("해당 옵션에 최소 소진 기준을 충족하는 소재가 없습니다.")

# ============================================================
# === PAGE 3: Detailed Trend (기존 Page 2) ===
# ============================================================
elif page == "3. Detailed Trend":
    st.title("📈 상세 데이터 (Time-series Trend)")

    view_all = st.checkbox("인덱스 미부합 소재 포함해서 보기", value=False)
    if len(d_range) == 2:
        df_period = df_raw[(df_raw['date'] >= pd.Timestamp(d_range[0])) & (df_raw['date'] <= pd.Timestamp(d_range[1]))]
    else:
        df_period = df_raw
    df_detail = df_period if view_all else df_f

    # 3-2: 추이 차트 지표 선택
    trend_metric = st.selectbox("차트 우측 Y축 지표", METRIC_OPTIONS, key="trend_metric")

    tab_m, tab_w, tab_d = st.tabs(["Monthly", "Weekly", "Daily"])

    # --- Monthly ---
    with tab_m:
        st.subheader("월간 성과")
        df_m = df_detail.copy()
        df_m['period'] = df_m['date'].dt.month.astype(str) + '월'
        df_m['_month'] = df_m['date'].dt.month
        trend_m = get_metrics_v4(df_m.groupby(['period', '_month', 'creative_id', 'creative']).agg(AGG_DICT)).reset_index()
        trend_m = trend_m.sort_values(['_month', 'cost'], ascending=[False, False])
        st.dataframe(format_df_v4(display_metrics(trend_m.drop(columns=['_month']), ['period', 'creative_id', 'creative'])), height=600)

        # 월별 집계 추이 시각화 (선택 지표)
        monthly_agg = get_metrics_v4(df_m.groupby(['period', '_month']).agg(AGG_DICT)).reset_index()
        monthly_agg = monthly_agg.sort_values('_month')
        if len(monthly_agg) > 1:
            fig_m = make_subplots(specs=[[{"secondary_y": True}]])
            fig_m.add_trace(go.Bar(x=monthly_agg['period'], y=monthly_agg['cost'], name='소진비용',
                                   marker_color='rgba(55,83,109,0.6)'), secondary_y=False)
            fig_m.add_trace(go.Scatter(x=monthly_agg['period'], y=monthly_agg[trend_metric],
                                       name=f'{trend_metric}', mode='lines+markers',
                                       line=dict(color='#FF6B35', width=2)), secondary_y=True)
            fig_m.update_layout(title=f'Monthly 소진비용 & {trend_metric} 추이',
                                legend=dict(orientation="h", yanchor="bottom", y=1.02))
            fig_m.update_yaxes(title_text="소진비용", secondary_y=False)
            fig_m.update_yaxes(title_text=trend_metric, secondary_y=True)
            format_krw_axis(fig_m, 'y', secondary=False)
            st.plotly_chart(fig_m, use_container_width=True)

    # --- Weekly (ISO 주차) ---
    with tab_w:
        st.subheader("주간 성과")
        df_w = df_detail.copy()
        df_w['_wp'] = df_w['date'].dt.to_period('W')
        df_w['_ws'] = df_w['_wp'].apply(lambda r: r.start_time)
        df_w['_we'] = df_w['_ws'] + pd.Timedelta(days=6)
        df_w['_wn'] = df_w['date'].dt.isocalendar().week.astype(int)
        df_w['period'] = df_w.apply(
            lambda r: f"W{r['_wn']} ({r['_ws'].strftime('%m/%d')}~{r['_we'].strftime('%m/%d')})", axis=1
        )
        # 3-1: creative 기준 정렬 (소재별 주차가 세로로 나열)
        trend_w = get_metrics_v4(df_w.groupby(['period', '_ws', 'creative_id', 'creative']).agg(AGG_DICT)).reset_index()
        trend_w = trend_w.sort_values(['creative', '_ws'], ascending=[True, True])
        st.dataframe(format_df_v4(display_metrics(trend_w.drop(columns=['_ws']), ['creative_id', 'creative', 'period'])), height=600)

        # 주간 집계 추이 시각화
        weekly_agg = get_metrics_v4(df_w.groupby(['period', '_ws']).agg(AGG_DICT)).reset_index()
        weekly_agg = weekly_agg.sort_values('_ws')
        if len(weekly_agg) > 1:
            fig_w = make_subplots(specs=[[{"secondary_y": True}]])
            fig_w.add_trace(go.Bar(x=weekly_agg['period'], y=weekly_agg['cost'], name='소진비용',
                                   marker_color='rgba(55,83,109,0.6)'), secondary_y=False)
            fig_w.add_trace(go.Scatter(x=weekly_agg['period'], y=weekly_agg[trend_metric],
                                       name=f'{trend_metric}', mode='lines+markers',
                                       line=dict(color='#FF6B35', width=2)), secondary_y=True)
            fig_w.update_layout(title=f'Weekly 소진비용 & {trend_metric} 추이',
                                legend=dict(orientation="h", yanchor="bottom", y=1.02))
            fig_w.update_yaxes(title_text="소진비용", secondary_y=False)
            fig_w.update_yaxes(title_text=trend_metric, secondary_y=True)
            format_krw_axis(fig_w, 'y', secondary=False)
            st.plotly_chart(fig_w, use_container_width=True)

        # Fix-5: 특정 소재 주간 추적
        st.divider()
        st.markdown("#### 🔍 특정 소재 주간 추적")
        all_creatives_w = sorted(trend_w['creative'].dropna().unique().tolist())
        tracked_cres = st.multiselect("추적할 소재 선택", all_creatives_w, key="weekly_track")
        if tracked_cres:
            tracked_data = trend_w[trend_w['creative'].isin(tracked_cres)].copy()
            tracked_data = tracked_data.sort_values(['creative', '_ws'])
            st.dataframe(format_df_v4(display_metrics(tracked_data.drop(columns=['_ws']), ['creative_id', 'creative', 'period'])), height=400)
            # 소재별 주차 라인 차트
            fig_track = px.line(tracked_data, x='period', y=trend_metric, color='creative',
                                markers=True, title=f'주간 {trend_metric} 추이 (선택 소재)')
            st.plotly_chart(fig_track, use_container_width=True)

    # --- Daily (소재별 추이) ---
    with tab_d:
        st.subheader("데일리 추이 (소재별)")
        # Fix-3: creative_id 매핑 표시를 위해 id-name 매핑 생성
        cre_id_map = df_detail.drop_duplicates('creative')[['creative_id', 'creative']].set_index('creative')['creative_id'].to_dict()
        cre_options = sorted(df_detail['creative'].dropna().unique().tolist())
        if cre_options:
            sel_cre = st.selectbox("소재 선택", options=cre_options)
            if sel_cre:
                st.caption(f"Creative ID: {cre_id_map.get(sel_cre, 'N/A')}")
                df_d = df_detail[df_detail['creative'] == sel_cre]
                trend_d = get_metrics_v4(df_d.groupby('date').agg(AGG_DICT)).reset_index()
                trend_d = trend_d.sort_values('date')
                trend_d_display = trend_d.copy()
                trend_d_display['date'] = trend_d_display['date'].dt.strftime('%Y-%m-%d')
                st.dataframe(format_df_v4(display_metrics(trend_d_display, ['date'])), height=400)
                # Fix-6: 2개 차트로 분리 (CTR / CPA 스케일 상이)
                if len(trend_d) > 1:
                    # 차트 1: 소진비용(bar) + CTR(line)
                    fig_ctr = make_subplots(specs=[[{"secondary_y": True}]])
                    fig_ctr.add_trace(
                        go.Bar(x=trend_d['date'], y=trend_d['cost'], name='소진비용',
                               marker_color='rgba(55,83,109,0.6)', opacity=0.7),
                        secondary_y=False)
                    fig_ctr.add_trace(
                        go.Scatter(x=trend_d['date'], y=trend_d['CTR'], name='CTR (%)',
                                   mode='lines+markers', line=dict(color='#FF6B35', width=2)),
                        secondary_y=True)
                    fig_ctr.update_layout(title=f"{sel_cre} — 소진비용 & CTR", xaxis_title='일자',
                                          legend=dict(orientation="h", yanchor="bottom", y=1.02))
                    fig_ctr.update_yaxes(title_text="소진비용", secondary_y=False)
                    fig_ctr.update_yaxes(title_text="CTR (%)", secondary_y=True)
                    format_krw_axis(fig_ctr, 'y', secondary=False)
                    st.plotly_chart(fig_ctr, use_container_width=True)

                    # 차트 2: 소진비용(bar) + CPA(line)
                    fig_cpa = make_subplots(specs=[[{"secondary_y": True}]])
                    fig_cpa.add_trace(
                        go.Bar(x=trend_d['date'], y=trend_d['cost'], name='소진비용',
                               marker_color='rgba(55,83,109,0.6)', opacity=0.7),
                        secondary_y=False)
                    fig_cpa.add_trace(
                        go.Scatter(x=trend_d['date'], y=trend_d['CPA'], name='CPA (원)',
                                   mode='lines+markers', line=dict(color='#2EC4B6', width=2)),
                        secondary_y=True)
                    fig_cpa.update_layout(title=f"{sel_cre} — 소진비용 & CPA", xaxis_title='일자',
                                          legend=dict(orientation="h", yanchor="bottom", y=1.02))
                    fig_cpa.update_yaxes(title_text="소진비용", secondary_y=False)
                    fig_cpa.update_yaxes(title_text="CPA (원)", secondary_y=True)
                    format_krw_axis(fig_cpa, 'y', secondary=False)
                    format_krw_axis(fig_cpa, 'y', secondary=True)
                    st.plotly_chart(fig_cpa, use_container_width=True)
        else:
            st.info("필터 조건에 맞는 소재가 없습니다.")

# ============================================================
# === PAGE 4: A/B Testing Lab (기존 Page 3) ===
# ============================================================
elif page == "4. A/B Testing Lab":
    st.title("🆚 전문 A/B 테스트")

    # Fix-7: A/B 테스트 메모 정형화 (JSON 기반)
    ab_memos = _load_json(AB_MEMO_PATH)

    with st.expander("📝 A/B 테스트 기록 등록", expanded=False):
        with st.form("ab_memo_form", clear_on_submit=True):
            ab_col1, ab_col2 = st.columns(2)
            with ab_col1:
                ab_name = st.text_input("테스트명", placeholder="예: stoneisland 화보형 vs 가격소구", key="ab_name")
                ab_purpose = st.selectbox("테스트 목적", ["CTR 개선", "CVR 개선", "CPA 절감", "소재 유형 검증", "타겟 비교", "기타"], key="ab_purpose")
                ab_creative_a = st.text_input("Creative A", placeholder="소재명 or ID", key="ab_cre_a")
                ab_creative_b = st.text_input("Creative B", placeholder="소재명 or ID", key="ab_cre_b")
            with ab_col2:
                ab_start = st.date_input("시작일", value=datetime.now().date(), key="ab_start")
                ab_hypothesis = st.text_area("가설", placeholder="예: 화보형이 CTR 높지만 CVR은 가격소구가 우위일 것", key="ab_hyp", height=68)
                ab_result = st.selectbox("결과", ["진행중", "A 승리", "B 승리", "무승부", "중단"], key="ab_result")
                ab_result_memo = st.text_input("결과 메모", placeholder="예: CTR A +40%, CVR B +15% → 혼재", key="ab_rmemo")

            submitted_ab = st.form_submit_button("테스트 등록")
            if submitted_ab and ab_name:
                new_ab = {
                    "id": str(uuid.uuid4())[:8],
                    "name": ab_name,
                    "purpose": ab_purpose,
                    "creative_a": ab_creative_a,
                    "creative_b": ab_creative_b,
                    "start_date": str(ab_start),
                    "hypothesis": ab_hypothesis,
                    "result": ab_result,
                    "result_memo": ab_result_memo,
                    "created_at": datetime.now().isoformat()
                }
                ab_memos.append(new_ab)
                _save_json(AB_MEMO_PATH, ab_memos)
                st.success("A/B 테스트가 등록되었습니다!")
                st.rerun()

    # 기존 테스트 이력
    if ab_memos:
        ab_rows = []
        for m in sorted(ab_memos, key=lambda x: x.get('created_at', ''), reverse=True):
            ab_rows.append({
                "테스트명": m['name'],
                "목적": m.get('purpose', ''),
                "A": m.get('creative_a', '')[:25],
                "B": m.get('creative_b', '')[:25],
                "시작일": m.get('start_date', ''),
                "결과": m.get('result', '진행중'),
                "결과 메모": m.get('result_memo', ''),
                "_id": m['id']
            })
        st.dataframe(pd.DataFrame(ab_rows).drop(columns=['_id']), use_container_width=True, hide_index=True)

        # 삭제
        del_ab = st.selectbox("삭제할 테스트", [""] + [f"{r['테스트명']} | {r['시작일']}" for r in ab_rows], key="del_ab")
        if del_ab and st.button("선택 항목 삭제", key="del_ab_btn"):
            idx = [f"{r['테스트명']} | {r['시작일']}" for r in ab_rows].index(del_ab)
            target_id = ab_rows[idx]['_id']
            ab_memos = [m for m in ab_memos if m['id'] != target_id]
            _save_json(AB_MEMO_PATH, ab_memos)
            st.success("삭제 완료!")
            st.rerun()

    # 소재 선별 필터 (필터된 데이터 기반)
    c_list = sorted(df_f['campaign'].dropna().unique().tolist())
    sel_camp = st.selectbox("Target Campaign", options=["All"] + c_list)
    df_target = df_f if sel_camp == "All" else df_f[df_f['campaign'] == sel_camp]

    g_list = sorted(df_target['ad_group'].dropna().unique().tolist())
    sel_group = st.selectbox("Target Ad Group", options=["All"] + g_list)
    df_target = df_target if sel_group == "All" else df_target[df_target['ad_group'] == sel_group]

    cre_list = sorted(df_target['creative'].dropna().unique().tolist())
    if len(cre_list) < 2:
        st.warning("비교할 소재가 2개 이상 필요합니다. 필터를 조정해주세요.")
    else:
        col1, col2 = st.columns(2)
        with col1: a_cre = st.selectbox("Creative A", cre_list, index=0)
        with col2: b_cre = st.selectbox("Creative B", cre_list, index=min(1, len(cre_list)-1))

        if a_cre and b_cre:
            a_data = df_target[df_target['creative'] == a_cre][NUM_COLS].sum().to_frame().T
            b_data = df_target[df_target['creative'] == b_cre][NUM_COLS].sum().to_frame().T
            a_perf = get_metrics_v4(a_data).iloc[0]
            b_perf = get_metrics_v4(b_data).iloc[0]

            # 4-3: 자동 Winner 표시
            def _judge_winner(a, b):
                """CTR/ROAS/CVR 기반 Winner 판정"""
                a_wins = 0
                b_wins = 0
                for m in ['CTR', 'ROAS', 'CVR']:
                    if a[m] > b[m] * 1.1:  # +10% 이상 우세
                        a_wins += 1
                    elif b[m] > a[m] * 1.1:
                        b_wins += 1
                if a_wins >= 2 and b_wins == 0:
                    return "A", "✅ Creative A Winner"
                elif b_wins >= 2 and a_wins == 0:
                    return "B", "✅ Creative B Winner"
                elif a_wins > 0 and b_wins > 0:
                    return "?", "⚠️ 판단불가 (지표 혼재)"
                else:
                    return "=", "➖ 유의미한 차이 없음"

            winner, verdict = _judge_winner(a_perf, b_perf)

            # Winner 판정 결과 표시
            if winner == "A":
                st.success(verdict)
            elif winner == "B":
                st.success(verdict)
            elif winner == "?":
                st.warning(verdict)
            else:
                st.info(verdict)

            # 비교표
            comparison = pd.DataFrame({
                "Metric": ["비용", "CTR", "CPC", "ROAS", "CPA", "구매CVR", "옵션완료CVR", "장바구니CVR", "AOV"],
                f"A: {a_cre[:25]}": [
                    f"{a_perf['cost']:,.0f}", f"{a_perf['CTR']:.2f}%", f"{a_perf['CPC']:,.0f}",
                    f"{a_perf['ROAS']:.2f}%", f"{a_perf['CPA']:,.0f}",
                    f"{a_perf['CVR']:.2f}%", f"{a_perf['Option_CVR']:.2f}%", f"{a_perf['Cart_CVR']:.2f}%",
                    f"{a_perf['AOV']:,.0f}"
                ],
                f"B: {b_cre[:25]}": [
                    f"{b_perf['cost']:,.0f}", f"{b_perf['CTR']:.2f}%", f"{b_perf['CPC']:,.0f}",
                    f"{b_perf['ROAS']:.2f}%", f"{b_perf['CPA']:,.0f}",
                    f"{b_perf['CVR']:.2f}%", f"{b_perf['Option_CVR']:.2f}%", f"{b_perf['Cart_CVR']:.2f}%",
                    f"{b_perf['AOV']:,.0f}"
                ]
            })
            st.table(comparison)

            # 4-4: 지표별 A vs B 바 차트
            st.subheader("📊 지표별 A vs B 비교")
            bar_metrics = ['CTR', 'CVR', 'ROAS', 'CPA', 'AOV']
            bar_data = pd.DataFrame({
                'Metric': bar_metrics * 2,
                'Creative': ['A'] * len(bar_metrics) + ['B'] * len(bar_metrics),
                'Value': [a_perf[m] for m in bar_metrics] + [b_perf[m] for m in bar_metrics]
            })
            fig_ab_bar = px.bar(bar_data, x='Metric', y='Value', color='Creative', barmode='group',
                                color_discrete_map={'A': '#636EFA', 'B': '#FF6B35'},
                                title='A vs B 지표 비교')
            st.plotly_chart(fig_ab_bar, use_container_width=True)

            # 4-1: 트렌드 차트 지표 선택
            st.subheader("Daily Trend Comparison")
            ab_metric = st.selectbox("트렌드 지표 선택", METRIC_OPTIONS, key="ab_trend_metric")
            a_daily = get_metrics_v4(df_target[df_target['creative'] == a_cre].groupby('date').agg(AGG_DICT)).reset_index()
            b_daily = get_metrics_v4(df_target[df_target['creative'] == b_cre].groupby('date').agg(AGG_DICT)).reset_index()
            a_daily['Creative'] = 'A'; b_daily['Creative'] = 'B'
            combined = pd.concat([a_daily, b_daily])
            fig_trend = px.line(combined, x='date', y=ab_metric, color='Creative', markers=True,
                                title=f'Daily {ab_metric} Trend')
            st.plotly_chart(fig_trend, use_container_width=True)

# ============================================================
# === PAGE 5: Advanced Analytics ===
# ============================================================
elif page == "5. Advanced Analytics":
    st.title("🔬 데이터 사이언스 분석")

    # 버블 매트릭스
    st.subheader("Scale vs Efficiency Matrix (Bubble Chart)")
    df_id = get_metrics_v4(df_f.groupby('creative_id').agg(AGG_DICT)).reset_index()
    id_name = df_f.drop_duplicates('creative_id').set_index('creative_id')['creative'].to_dict()
    df_id['name'] = df_id['creative_id'].map(id_name)

    top_n = st.slider("지출 상위 N개 소재만 보기", 10, 100, 30)
    df_bubble = df_id[df_id['cost'] >= min_spend].sort_values("cost", ascending=False).head(top_n)

    fig_b = px.scatter(df_bubble, x='cost', y='ROAS', size='revenue', color='ROAS', hover_name='name',
                       log_x=True, color_continuous_scale='Viridis', title=f"Top {top_n} Spend Creatives Matrix")
    st.plotly_chart(fig_b, use_container_width=True)

    # Prediction — 소재명 기반 선택
    st.divider()
    st.subheader("🔮 Efficiency Prediction & D-Day Forecast")

    df_pred = df_id[df_id['cost'] > 100000].copy()
    if not df_pred.empty:
        name_to_ids = df_pred.groupby('name')['creative_id'].apply(list).to_dict()
        sel_name = st.selectbox("분석 대상 소재 선택 (지출 10만 이상)", options=sorted(name_to_ids.keys()))
        if sel_name:
            ids = name_to_ids[sel_name]
            if len(ids) > 1:
                pred_target = st.selectbox("동일 소재명 — ID 선택", options=ids)
            else:
                pred_target = ids[0]
            st.caption(f"Creative ID: {pred_target}")

            df_hist = get_metrics_v4(df_raw[df_raw['creative_id'] == pred_target].groupby('date').agg(AGG_DICT)).reset_index()

            if len(df_hist) >= 5:
                X = np.arange(len(df_hist))
                Y = df_hist['ROAS'].values
                slope, intercept = np.polyfit(X, Y, 1)
                if slope < 0:
                    d_day = (400 - intercept) / slope - len(df_hist)
                    st.warning(f"예측: **{max(0.0, float(d_day)):.1f}일** 뒤 ROAS 400% 임계점 도달")
                else:
                    st.success("효율 상승 추세입니다!")

                p_fig = px.line(df_hist, x='date', y='ROAS', title=f"ROAS Trend: {sel_name}")
                st.plotly_chart(p_fig, use_container_width=True)
            else:
                st.info("시계열 데이터가 부족합니다 (최소 5일 이상 필요)")
    else:
        st.info("지출 10만 이상 소재가 없습니다.")

    # Element Winner Insights
    st.divider()
    st.subheader("💡 Element Winner Insights")
    attr_type = st.radio("분석 요소", ["option_main (메인 오브젝트)", "option_detailed (상세 구분)", "size_layout (사이즈/배열)"], horizontal=True)
    attr_col = attr_type.split(' ')[0]

    attr_res = get_metrics_v4(df_f.groupby(['placement', attr_col]).agg(AGG_DICT)).reset_index()
    fig_i = px.bar(attr_res, x=attr_col, y='CTR', color='placement', barmode='group', title=f"CTR by {attr_col} & Placement")
    st.plotly_chart(fig_i, use_container_width=True)

# ============================================================
# === PAGE 6: Operations Hub (기존 History Memo 고도화) ===
# ============================================================
elif page == "6. Operations Hub":
    st.title("📋 Operations Hub")
    st.caption("PDCA 루프: 현상 관찰 → 원인 추정 → 액션 실행 → Before/After 성과 비교")

    tab_ops, tab_winning = st.tabs([
        "📋 Operations Log",
        "🏆 Brand Winning History"
    ])

    # ----------------------------------------------------------
    # Tab 1: Operations Log (통합형 — Action Log + Diary 통합)
    # ----------------------------------------------------------
    with tab_ops:
        st.subheader("운영 로그 (현상 → 판단 → 액션 → 결과)")

        action_log = _load_json(ACTION_LOG_PATH)

        # 입력 폼
        with st.form("ops_form", clear_on_submit=True):
            st.markdown("#### 새 운영 기록")
            of_col1, of_col2 = st.columns(2)
            with of_col1:
                ops_date = st.date_input("날짜", value=datetime.now().date(), key="ops_date")
                ops_priority = st.selectbox("우선순위", ["🔴 긴급", "🟡 주의", "🟢 모니터링"], key="ops_pri")
                ops_level = st.selectbox("레벨", ["캠페인", "광고그룹", "소재", "전체"], key="ops_level")
                ops_campaign = st.selectbox("대상 캠페인", sorted(df_raw['campaign'].dropna().unique().tolist()), key="ops_camp")
                ops_target = st.text_input("대상 (소재/그룹)", key="ops_target")
            with of_col2:
                ops_category = st.selectbox("카테고리", ["예산", "소재", "타겟팅", "랜딩", "시즈널", "기타"], key="ops_cat")
                ops_observation = st.text_area("현상", placeholder="데이터 기반 관찰 (예: UA ROAS 3일 연속 하락 850%→620%)", key="ops_obs", height=68)
                ops_hypothesis = st.text_input("원인 추정", placeholder="가설 (예: 주말 트래픽 품질 저하 + 소재 피로도)", key="ops_hyp")
                ops_action_type = st.selectbox("액션 유형", ["소재 ON", "소재 OFF", "예산 증액", "예산 감액", "소재 교체", "타겟 변경", "기타"], key="ops_act_type")
                ops_action_detail = st.text_area("액션 내용", placeholder="구체적 실행 (예: 소재 A OFF, 소재 B 신규 투입)", key="ops_act_detail", height=68)
            ops_expected = st.text_input("기대 효과", placeholder="예: CPA 15% 개선 예상", key="ops_expected")
            ops_status = st.selectbox("상태", ["계획", "진행중", "완료", "보류"], key="ops_status")

            submitted_ops = st.form_submit_button("기록 등록")
            if submitted_ops and ops_observation:
                new_entry = {
                    "id": str(uuid.uuid4())[:8],
                    "date": str(ops_date),
                    "priority": ops_priority,
                    "level": ops_level,
                    "campaign": ops_campaign,
                    "target": ops_target,
                    "category": ops_category,
                    "observation": ops_observation,
                    "hypothesis": ops_hypothesis,
                    "action_type": ops_action_type,
                    "action_detail": ops_action_detail,
                    "expected": ops_expected,
                    "status": ops_status,
                    "created_at": datetime.now().isoformat()
                }
                action_log.append(new_entry)
                _save_json(ACTION_LOG_PATH, action_log)
                st.success("운영 기록이 등록되었습니다!")
                st.rerun()

        # 필터
        if action_log:
            st.markdown("---")
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                f_status = st.multiselect("상태", ["계획", "진행중", "완료", "보류"], default=["계획", "진행중"], key="ops_f_status")
            with fc2:
                f_pri = st.multiselect("우선순위", ["🔴 긴급", "🟡 주의", "🟢 모니터링"], default=["🔴 긴급", "🟡 주의", "🟢 모니터링"], key="ops_f_pri")
            with fc3:
                f_cat = st.multiselect("카테고리", ["예산", "소재", "타겟팅", "랜딩", "시즈널", "기타"], default=["예산", "소재", "타겟팅", "랜딩", "시즈널", "기타"], key="ops_f_cat")

            pri_order = {"🔴 긴급": 0, "🟡 주의": 1, "🟢 모니터링": 2}
            filtered_log = [e for e in action_log
                            if e.get('status', '계획') in f_status
                            and e.get('priority', '🟢 모니터링') in f_pri
                            and e.get('category', '기타') in f_cat]
            filtered_log = sorted(filtered_log, key=lambda x: (
                pri_order.get(x.get('priority', ''), 9),
                -pd.Timestamp(x.get('date', '2020-01-01')).timestamp()
            ))

            if filtered_log:
                rows = []
                for entry in filtered_log:
                    act_d = pd.Timestamp(entry['date'])
                    camp = entry.get('campaign', '')

                    # Before/After 자동 계산 (D-3~D-1 vs D+1~D+3)
                    before_mask = (df_raw['date'] >= act_d - timedelta(days=3)) & (df_raw['date'] < act_d) & (df_raw['campaign'] == camp)
                    before_df = df_raw[before_mask]
                    after_mask = (df_raw['date'] > act_d) & (df_raw['date'] <= act_d + timedelta(days=3)) & (df_raw['campaign'] == camp)
                    after_df = df_raw[after_mask]

                    b_roas = (before_df['revenue'].sum() / before_df['cost'].sum() * 100) if before_df['cost'].sum() > 0 else 0
                    b_cpa = (before_df['cost'].sum() / before_df['purchases'].sum()) if before_df['purchases'].sum() > 0 else 0

                    if after_df.empty or after_df['cost'].sum() == 0:
                        a_roas_str = "⏳"
                        d_roas_str = "⏳"
                        verdict = "⏳ 측정 중"
                    else:
                        a_roas = (after_df['revenue'].sum() / after_df['cost'].sum() * 100)
                        a_roas_str = f"{a_roas:,.1f}%"
                        d_roas = a_roas - b_roas
                        d_roas_str = f"{d_roas:+,.1f}%p"
                        if d_roas > 50:
                            verdict = "✅ 효과적"
                        elif d_roas < -50:
                            verdict = "❌ 역효과"
                        else:
                            verdict = "⚠️ 미미"

                    rows.append({
                        "날짜": entry['date'],
                        "🚦": entry.get('priority', '🟢 모니터링'),
                        "레벨": entry.get('level', ''),
                        "대상": entry.get('target', '')[:20],
                        "카테고리": entry.get('category', ''),
                        "현상": entry.get('observation', '')[:30],
                        "액션": entry.get('action_type', ''),
                        "B-ROAS": f"{b_roas:,.1f}%",
                        "A-ROAS": a_roas_str,
                        "Δ": d_roas_str,
                        "상태": entry.get('status', '계획'),
                        "_id": entry['id']
                    })

                log_df = pd.DataFrame(rows)
                st.dataframe(log_df.drop(columns=['_id']), use_container_width=True, hide_index=True)

                # 상태 변경 + 삭제
                upd_col1, upd_col2 = st.columns(2)
                with upd_col1:
                    upd_sel = st.selectbox("상태 변경", [""] + [f"{r['날짜']} | {r['대상']} | {r['현상'][:15]}" for r in rows], key="ops_upd_sel")
                    new_status = st.selectbox("새 상태", ["계획", "진행중", "완료", "보류"], key="ops_new_status")
                    if upd_sel and st.button("상태 변경", key="ops_upd_btn"):
                        idx = [f"{r['날짜']} | {r['대상']} | {r['현상'][:15]}" for r in rows].index(upd_sel)
                        target_id = rows[idx]['_id']
                        for e in action_log:
                            if e['id'] == target_id:
                                e['status'] = new_status
                                break
                        _save_json(ACTION_LOG_PATH, action_log)
                        st.success("상태 변경 완료!")
                        st.rerun()

                with upd_col2:
                    del_sel = st.selectbox("삭제할 항목", [""] + [f"{r['날짜']} | {r['대상']} | {r['현상'][:15]}" for r in rows], key="ops_del_sel")
                    if del_sel and st.button("선택 항목 삭제", key="ops_del_btn"):
                        idx = [f"{r['날짜']} | {r['대상']} | {r['현상'][:15]}" for r in rows].index(del_sel)
                        target_id = rows[idx]['_id']
                        action_log = [e for e in action_log if e['id'] != target_id]
                        _save_json(ACTION_LOG_PATH, action_log)
                        st.success("삭제 완료!")
                        st.rerun()
            else:
                st.info("필터 조건에 맞는 기록이 없습니다.")
        else:
            st.info("등록된 운영 기록이 없습니다. 위 폼에서 새 기록을 등록해주세요.")

    # ----------------------------------------------------------
    # Tab 2: Brand Winning History
    # ----------------------------------------------------------
    with tab_winning:
        st.subheader("브랜드별 위닝 소재 히스토리")

        winning_history = _load_json(WINNING_HISTORY_PATH)

        # 자동 추천 영역
        st.markdown("#### 🤖 현재 데이터 기반 위닝 후보")
        brand_cre = get_metrics_v4(df_f.groupby(['brand', 'creative']).agg(AGG_DICT)).reset_index()
        brand_cre = brand_cre[brand_cre['cost'] >= min_spend]

        if not brand_cre.empty:
            top_candidates = brand_cre.sort_values('ROAS', ascending=False).groupby('brand').head(3)
            top_candidates = top_candidates[top_candidates['brand'] != 'etc']
            if not top_candidates.empty:
                cand_display = top_candidates[['brand', 'creative', 'ROAS', 'CTR', 'CVR', 'CPA', 'cost']].copy()
                cand_display['ROAS'] = cand_display['ROAS'].apply(lambda x: f"{x:,.1f}%")
                cand_display['CTR'] = cand_display['CTR'].apply(lambda x: f"{x:,.2f}%")
                cand_display['CVR'] = cand_display['CVR'].apply(lambda x: f"{x:,.2f}%")
                cand_display['CPA'] = cand_display['CPA'].apply(lambda x: f"{x:,.0f}")
                cand_display['cost'] = cand_display['cost'].apply(lambda x: f"{x:,.0f}")
                st.dataframe(cand_display, use_container_width=True, hide_index=True, height=250)

        # 입력 폼
        st.markdown("---")
        with st.form("winning_form", clear_on_submit=True):
            st.markdown("#### 위닝 소재 등록")
            wf_col1, wf_col2 = st.columns(2)
            with wf_col1:
                win_brand = st.text_input("브랜드", key="win_brand")
                win_creative = st.text_input("소재명", key="win_creative")
                win_period = st.text_input("운영 기간", placeholder="예: 2026-03-W2~W3", key="win_period")
            with wf_col2:
                win_metrics = st.text_input("핵심 성과", placeholder="예: ROAS 1,200%, CTR 2.1%", key="win_metrics")
                win_factor = st.selectbox("위닝 요인", ["가격소구", "화보형", "모델", "다중상품", "쿠폰", "브랜드파워", "기타"], key="win_factor")
                win_detail = st.text_input("위닝 상세 메모", placeholder="예: 아우터 30% 할인가 전면 배치", key="win_detail")

            submitted_win = st.form_submit_button("위닝 소재 등록")
            if submitted_win and win_brand and win_creative:
                cre_info = df_f[df_f['creative'] == win_creative]
                opt_main = cre_info['option_main'].iloc[0] if not cre_info.empty else ""
                opt_detailed = cre_info['option_detailed'].iloc[0] if not cre_info.empty else ""
                plc = cre_info['placement'].iloc[0] if not cre_info.empty else ""

                new_win = {
                    "id": str(uuid.uuid4())[:8],
                    "brand": win_brand,
                    "creative": win_creative,
                    "period": win_period,
                    "metrics": win_metrics,
                    "winning_factor": win_factor,
                    "winning_detail": win_detail,
                    "option_main": opt_main,
                    "option_detailed": opt_detailed,
                    "placement": plc,
                    "created_at": datetime.now().isoformat()
                }
                winning_history.append(new_win)
                _save_json(WINNING_HISTORY_PATH, winning_history)
                st.success("위닝 소재가 등록되었습니다!")
                st.rerun()

        # 위닝 히스토리 테이블
        if winning_history:
            st.markdown("---")
            st.markdown("#### 📚 위닝 히스토리")

            all_brands = sorted(set(w['brand'] for w in winning_history))
            filter_brand = st.selectbox("브랜드 필터", ["전체"] + all_brands, key="win_brand_filter")

            filtered_wins = winning_history if filter_brand == "전체" else [w for w in winning_history if w['brand'] == filter_brand]
            filtered_wins = sorted(filtered_wins, key=lambda x: x.get('created_at', ''), reverse=True)

            if filtered_wins:
                win_rows = []
                for w in filtered_wins:
                    win_rows.append({
                        "브랜드": w['brand'],
                        "소재명": w['creative'][:35],
                        "기간": w.get('period', ''),
                        "핵심 성과": w.get('metrics', ''),
                        "위닝 요인": w.get('winning_factor', ''),
                        "상세": w.get('winning_detail', ''),
                        "Main": w.get('option_main', ''),
                        "Detailed": w.get('option_detailed', ''),
                        "_id": w['id']
                    })
                win_df = pd.DataFrame(win_rows)
                st.dataframe(win_df.drop(columns=['_id']), use_container_width=True, hide_index=True)

                del_win = st.selectbox("삭제할 항목", [""] + [f"{r['브랜드']} | {r['소재명']}" for r in win_rows], key="del_win")
                if del_win and st.button("선택 항목 삭제", key="del_win_btn"):
                    idx = [f"{r['브랜드']} | {r['소재명']}" for r in win_rows].index(del_win)
                    target_id = win_rows[idx]['_id']
                    winning_history = [w for w in winning_history if w['id'] != target_id]
                    _save_json(WINNING_HISTORY_PATH, winning_history)
                    st.success("삭제 완료!")
                    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption(f"Last Log Date: {df_raw['date'].max().strftime('%Y-%m-%d')}")
