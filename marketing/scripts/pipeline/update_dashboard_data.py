import pandas as pd
import numpy as np
from pathlib import Path
import re
import os

# === 경로 설정 (프로젝트 루트 기준) ===
# marketing/scripts/pipeline/ → 3단계 상위 = joo-mkt/
CUR_DIR = Path(__file__).resolve().parent
BASE_DIR = CUR_DIR.parent.parent.parent  # joo-mkt 루트
DATA_DIR = BASE_DIR / "outputs" / "processed" / "dashboard"
CLEANED_DATA_PATH = DATA_DIR / "kakao_cleaned.csv"
CACHE_PATH = DATA_DIR / "kakao_dashboard_final_v4.parquet"

def update_parquet_data():
    """creative_view.py에서 사용하는 전처리 로직을 실행하여 Parquet 파일을 최신화합니다."""
    
    if not CLEANED_DATA_PATH.exists():
        print(f"❌ 원본 CSV 파일({CLEANED_DATA_PATH.name})이 존재하지 않습니다. 전처리를 먼저 수행하세요.")
        return

    print(f"🚀 대시보드용 데이터 가공 중... ({CLEANED_DATA_PATH.name} -> {CACHE_PATH.name})")
    
    # 컬럼 정의
    cols = [
        'Event Date', 'Ad Creative', 'Ad Creative ID', 'Ad Group', 'Campaign',
        '보정비용', '구매액 (App+Web)', '구매 완료 (App+Web)',
        'Clicks (Channel)', 'Impressions (Channel)', '캠페인유형', '지면',
        '장바구니 담기 (App+Web)', 'completeProductOption (App+Web)'
    ]
    
    # 데이터 로드
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

    # 소재 인덱스 파싱
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

    # 저장
    df.to_parquet(CACHE_PATH, engine='pyarrow')
    print(f"✅ 데이터 가공 완료: {CACHE_PATH}")

if __name__ == "__main__":
    update_parquet_data()
