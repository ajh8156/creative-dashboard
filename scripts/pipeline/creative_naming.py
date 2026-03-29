"""
소재 네이밍 정규화 모듈
- 소재명_컨벤션_260320.xlsx (소재명_옵션규칙_new 시트) 기준
- parse_creative (파이프라인) + parse_index_v4 (대시보드) 공유
"""

# === option_main 정규화 ===
# 별칭 → 정규 코드
MAIN_ALIASES = {"logo": "log"}
# 유효한 option_main 코드 (5개)
VALID_MAINS = {"log", "sku", "inf", "txt", "prm"}
# old 컨벤션에서 main이었으나 new에서 detailed로 이동된 값
LEGACY_MAINS = {"pho"}


# === option_detailed 정규화 ===
# 별칭 → 정규 코드 (엑셀 "통합" 비고 기반)
DETAILED_ALIASES = {
    # one 통합 (n은 독립 값이므로 제외)
    "single": "one", "sku": "one",
    # multi 통합
    "two": "multi", "three": "multi", "triple": "multi",
    "3p": "multi", "four": "multi", "3multi": "multi",
    **{f"multi{i}": "multi" for i in range(2, 8)},
    # cat 통합
    "jacket": "cat", "coat": "cat", "knit": "cat",
    "shirts": "cat", "shirt": "cat", "pants": "cat",
    # set 통합
    "1plus1": "set", "mix": "set", "oneplusone": "set",
    # brand 통합 (detailed context에서 logo → brand)
    "disney": "brand", "wolsey": "brand", "logo": "brand",
    # sale 통합 (수기 변형)
    "discount": "sale", "disc": "sale", "dis": "sale", "price": "sale",
    # pho 통합 (수기 변형)
    "photo": "pho",
    # cat 통합 (수기 변형)
    "dressshoes": "cat", "shoes": "cat", "wallabee": "cat",
}
# 유효한 option_detailed 코드 (10개)
VALID_DETAILEDS = {"n", "one", "multi", "cat", "set", "brand", "model", "pho", "sale", "cp", "style"}

# saletap 패턴 → prm/sale 매핑용
SALETAP_PATTERN = "saletap"


def normalize_main(token):
    """토큰 → option_main 정규 코드. 매칭 없으면 None 반환."""
    if not token:
        return None
    t = MAIN_ALIASES.get(token, token)
    if t in VALID_MAINS:
        return t
    return None


def normalize_detailed(token):
    """토큰 → option_detailed 정규 코드. 매칭 없으면 None 반환."""
    if not token:
        return None
    # 1) 정확 매칭 (유효 코드)
    if token in VALID_DETAILEDS:
        return token
    # 2) 별칭 정확 매칭
    if token in DETAILED_ALIASES:
        return DETAILED_ALIASES[token]
    # 3) 부분 매칭 (multi3 등 숫자 조합 대응)
    for key, val in DETAILED_ALIASES.items():
        if key in token:
            return val
    return None


def is_legacy_main(token):
    """old 컨벤션 main 값인지 확인 (pho 등)."""
    return token in LEGACY_MAINS


def is_saletap(name):
    """소재명에 saletap 패턴 포함 여부."""
    return SALETAP_PATTERN in str(name).lower()
