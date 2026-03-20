import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import re
import os

# === 설정 ===
st.set_page_config(page_title="Growth Creative Dashboard v4", layout="wide")

# === 경로 설정 (로컬 및 클라우드 배포 통합) ===
CUR_DIR = Path(__file__).resolve().parent

# 1. 데이터 디렉토리 설정
# 루트에 있는 경우와 outputs/processed/ 에 있는 경우 모두 대응
if (CUR_DIR / "outputs" / "processed").exists():
    DATA_DIR = CUR_DIR / "outputs" / "processed"
elif (CUR_DIR.parent.parent / "outputs" / "processed").exists():
    DATA_DIR = CUR_DIR.parent.parent / "outputs" / "processed"
else:
    # 깃허브 루트에 파일이 직접 있는 경우
    DATA_DIR = CUR_DIR

CLEANED_DATA_PATH = DATA_DIR / "kakao_cleaned.csv"
CACHE_PATH = DATA_DIR / "kakao_dashboard_final_v4.parquet"

# 2. 메모 파일 경로 설정
if (CUR_DIR / "memo.md").exists():
    MEMO_PATH = CUR_DIR / "memo.md"
else:
    # 로컬 경로 (marketing/dashboards/memo.md)
    MEMO_PATH = CUR_DIR / "memo.md" # 기본값은 현재 폴더

# === 상수 ===
NUM_COLS = ['cost', 'revenue', 'purchases', 'clicks', 'impressions', 'cart', 'option_comp']
AGG_DICT = {c: 'sum' for c in NUM_COLS}
# 테이블에 표시할 지표 (순서 고정)
DISPLAY_COLS = ['cost', 'CTR', 'CPC', 'ROAS', 'purchases', 'CPA', 'CVR', 'Option_CVR', 'Cart_CVR', 'AOV']

# === 1. 데이터 로드 및 스마트 캐시 엔진 ===
@st.cache_data
def load_v4_data_optimized():
    CACHE_PATH = DATA_DIR / "kakao_dashboard_final_v4.parquet"

    if CLEANED_DATA_PATH.exists():
        csv_mtime = os.path.getmtime(CLEANED_DATA_PATH)
        cache_exists = CACHE_PATH.exists()
        cache_mtime = os.path.getmtime(CACHE_PATH) if cache_exists else 0

        # 캐시가 없거나 CSV가 더 최신인 경우만 재생성
        if not cache_exists or csv_mtime > cache_mtime:
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

                # 소재 인덱스 파싱 (네이밍 컨벤션 기반)
                SIZE_LAYOUT_CODES = {'11', '21', '34', '169', 'l', 'c', 'r'}
                def parse_index_v4(name):
                    res = {'launch_dt': None, 'idx_type': 'etc', 'option_main': 'etc',
                           'option_detailed': 'etc', 'size_layout': 'etc', 'is_valid_index': False}
                    if not isinstance(name, str): return res
                    date_match = re.search(r'^(\d{6})', name)
                    if date_match:
                        try:
                            res['launch_dt'] = pd.to_datetime(date_match.group(1), format='%y%m%d')
                            res['is_valid_index'] = True
                        except: pass
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
                        opts = option_str.split('_')
                        res['option_main'] = opts[0] if len(opts) > 0 else 'etc'
                        res['option_detailed'] = opts[1] if len(opts) > 1 else 'etc'
                        last = opts[-1] if len(opts) > 2 else None
                        if last and last in SIZE_LAYOUT_CODES:
                            res['size_layout'] = last
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
        st.error(f"데이터 파일({CACHE_PATH.name})을 찾을 수 없습니다. 로컬에서 CSV를 먼저 가공해주세요.")
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

# === 사이드바: 필터링 ===
st.sidebar.title("🛠️ Growth Filters v4")
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
else:
    st.error("데이터 로딩 실패")
    st.stop()

# === 페이지 네비게이션 ===
page = st.sidebar.radio("Navigation", ["1. Summary & Insights", "2. Detailed Trend", "3. A/B Testing Lab", "4. Advanced Analytics", "5. History Memo"])

# === PAGE 1: Summary & Insights ===
if page == "1. Summary & Insights":
    st.title("🎯 소재 성과 요약 및 인사이트")

    # KPI 카드
    st.subheader("Filtered Aggregate Performance")
    col1, col2, col3, col4, col5 = st.columns(5)
    total_c = df_f['cost'].sum()
    total_r = df_f['revenue'].sum()
    total_p = df_f['purchases'].sum()

    col1.metric("Total Cost", f"{total_c/10000:,.0f}만")
    col2.metric("Total ROAS", f"{(total_r/total_c*100):,.1f}%" if total_c > 0 else "0%")
    col3.metric("Purchases", f"{total_p:,.0f}건")
    col4.metric("Avg CTR", f"{(df_f['clicks'].sum()/df_f['impressions'].sum()*100):,.2f}%" if df_f['impressions'].sum() > 0 else "0%")
    col5.metric("Active Creatives", f"{df_f['creative_id'].nunique()}개")

    # 소재 옵션 분석 (option_main / option_detailed)
    st.divider()
    st.subheader("📍 소재 옵션별 성과 (Winning Elements)")

    c1, c2 = st.columns(2)
    with c1:
        st.write("**Option Main (메인 오브젝트: log/sku/inf...)**")
        el1_perf = get_metrics_v4(df_f.groupby('option_main').agg(AGG_DICT)).reset_index()
        fig1 = px.bar(el1_perf, x='option_main', y='ROAS', color='ROAS', text_auto='.1f', color_continuous_scale='Blues')
        st.plotly_chart(fig1, use_container_width=True)

    with c2:
        st.write("**Option Detailed (상세: model/multi/brand...)**")
        el2_perf = get_metrics_v4(df_f.groupby('option_detailed').agg(AGG_DICT)).reset_index()
        fig2 = px.bar(el2_perf, x='option_detailed', y='ROAS', color='ROAS', text_auto='.1f', color_continuous_scale='Oranges')
        st.plotly_chart(fig2, use_container_width=True)

    # 옵션별 요약 표
    st.subheader("📊 옵션별 성과 요약")
    option_type = st.selectbox("분석 축 선택", ["placement", "campaign_type", "feature", "brand", "option_main", "option_detailed", "size_layout"])
    opt_perf = get_metrics_v4(df_f.groupby(option_type).agg(AGG_DICT)).sort_values("cost", ascending=False).reset_index()
    st.dataframe(format_df_v4(display_metrics(opt_perf, [option_type])))

# === PAGE 2: Detailed Trend ===
elif page == "2. Detailed Trend":
    st.title("📈 상세 데이터 (Time-series Trend)")

    view_all = st.checkbox("인덱스 미부합 소재 포함해서 보기", value=False)
    if len(d_range) == 2:
        df_period = df_raw[(df_raw['date'] >= pd.Timestamp(d_range[0])) & (df_raw['date'] <= pd.Timestamp(d_range[1]))]
    else:
        df_period = df_raw
    df_detail = df_period if view_all else df_f

    tab_m, tab_w, tab_d = st.tabs(["Monthly", "Weekly", "Daily"])

    # --- Monthly ---
    with tab_m:
        st.subheader("월간 성과")
        df_m = df_detail.copy()
        df_m['period'] = df_m['date'].dt.month.astype(str) + '월'
        # 정렬용 월 숫자 보존
        df_m['_month'] = df_m['date'].dt.month
        trend_m = get_metrics_v4(df_m.groupby(['period', '_month', 'creative_id', 'creative']).agg(AGG_DICT)).reset_index()
        trend_m = trend_m.sort_values(['_month', 'cost'], ascending=[False, False])
        st.dataframe(format_df_v4(display_metrics(trend_m.drop(columns=['_month']), ['period', 'creative'])))

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
        trend_w = get_metrics_v4(df_w.groupby(['period', '_ws', 'creative_id', 'creative']).agg(AGG_DICT)).reset_index()
        trend_w = trend_w.sort_values(['_ws', 'cost'], ascending=[False, False])
        st.dataframe(format_df_v4(display_metrics(trend_w.drop(columns=['_ws']), ['period', 'creative'])))

    # --- Daily (소재별 추이) ---
    with tab_d:
        st.subheader("데일리 추이 (소재별)")
        cre_options = sorted(df_detail['creative'].dropna().unique().tolist())
        if cre_options:
            sel_cre = st.selectbox("소재 선택", options=cre_options)
            if sel_cre:
                df_d = df_detail[df_detail['creative'] == sel_cre]
                trend_d = get_metrics_v4(df_d.groupby('date').agg(AGG_DICT)).reset_index()
                trend_d = trend_d.sort_values('date', ascending=False)
                trend_d['date'] = trend_d['date'].dt.strftime('%Y-%m-%d')
                st.dataframe(format_df_v4(display_metrics(trend_d, ['date'])))
                # ROAS 추이 차트
                if len(trend_d) > 1:
                    fig = px.line(trend_d, x='date', y='ROAS', markers=True, title=f"{sel_cre} Daily ROAS")
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("필터 조건에 맞는 소재가 없습니다.")

# === PAGE 3: A/B Testing Lab ===
elif page == "3. A/B Testing Lab":
    st.title("🆚 전문 A/B 테스트")

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
            # DataFrame으로 감싸서 get_metrics_v4 호환
            a_data = df_target[df_target['creative'] == a_cre][NUM_COLS].sum().to_frame().T
            b_data = df_target[df_target['creative'] == b_cre][NUM_COLS].sum().to_frame().T
            a_perf = get_metrics_v4(a_data).iloc[0]
            b_perf = get_metrics_v4(b_data).iloc[0]

            comparison = pd.DataFrame({
                "Metric": ["비용", "CTR", "CPC", "ROAS", "구매", "CPA", "구매CVR", "옵션완료CVR", "장바구니CVR", "AOV"],
                f"A: {a_cre[:25]}": [
                    f"{a_perf['cost']:,.0f}", f"{a_perf['CTR']:.2f}%", f"{a_perf['CPC']:,.0f}",
                    f"{a_perf['ROAS']:.2f}%", f"{a_perf['purchases']:,.0f}", f"{a_perf['CPA']:,.0f}",
                    f"{a_perf['CVR']:.2f}%", f"{a_perf['Option_CVR']:.2f}%", f"{a_perf['Cart_CVR']:.2f}%",
                    f"{a_perf['AOV']:,.0f}"
                ],
                f"B: {b_cre[:25]}": [
                    f"{b_perf['cost']:,.0f}", f"{b_perf['CTR']:.2f}%", f"{b_perf['CPC']:,.0f}",
                    f"{b_perf['ROAS']:.2f}%", f"{b_perf['purchases']:,.0f}", f"{b_perf['CPA']:,.0f}",
                    f"{b_perf['CVR']:.2f}%", f"{b_perf['Option_CVR']:.2f}%", f"{b_perf['Cart_CVR']:.2f}%",
                    f"{b_perf['AOV']:,.0f}"
                ]
            })
            st.table(comparison)

            # Daily ROAS 추이
            st.subheader("Daily ROAS Trend Comparison")
            a_daily = get_metrics_v4(df_target[df_target['creative'] == a_cre].groupby('date').agg(AGG_DICT)).reset_index()
            b_daily = get_metrics_v4(df_target[df_target['creative'] == b_cre].groupby('date').agg(AGG_DICT)).reset_index()
            a_daily['Creative'] = 'A'; b_daily['Creative'] = 'B'
            combined = pd.concat([a_daily, b_daily])
            fig_trend = px.line(combined, x='date', y='ROAS', color='Creative', markers=True)
            st.plotly_chart(fig_trend, use_container_width=True)

# === PAGE 4: Advanced Analytics ===
elif page == "4. Advanced Analytics":
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
        # 소재명 → ID 매핑
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

# === PAGE 5: History Memo ===
elif page == "5. History Memo":
    st.title("📝 운영 히스토리 메모장")

    if not MEMO_PATH.exists():
        os.makedirs(MEMO_PATH.parent, exist_ok=True)
        with open(MEMO_PATH, "w", encoding="utf-8") as f:
            f.write("# Operating History Memo\n\n- 2026-03-20: v4 대시보드 정식 운영 시작\n")

    with open(MEMO_PATH, "r", encoding="utf-8") as f:
        memo_val = f.read()

    edited_memo = st.text_area("Update History / Winning Insights", value=memo_val, height=500)

    if st.button("Save History"):
        with open(MEMO_PATH, "w", encoding="utf-8") as f:
            f.write(edited_memo)
        st.success("히스토리가 성공적으로 저장되었습니다.")
        st.rerun()

    st.markdown("---")
    st.markdown("### 📋 Preview")
    st.markdown(edited_memo)

st.sidebar.markdown("---")
st.sidebar.caption(f"Last Log Date: {df_raw['date'].max().strftime('%Y-%m-%d')}")
