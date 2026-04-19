from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from datetime import datetime


CODE_RE = re.compile(r"^\d{4}[A-Z]?$")
NUMERIC_RE = re.compile(r"^-?[\d,]+(?:\.\d+)?$")

FIXED_COLUMNS = [
    "id_name",
    "code",
    "name",
    "book_price",
    "last_price",
    "change_pct",
    "board_set_label",
    "tr_pl",
    "realized_pl",
    "unrealized_pl",
    "net_qty",
    "position_market_value",
    "net_pl_rate",
    "book_value_net",
    "send_label",
    "buy_qty",
    "sell_qty",
    "net_pl",
    "strike_price",
    "contract_month",
    "call_put",
    "product_type",
    "index_flag",
    "sell_price",
    "buy_price",
    "delta_qty",
    "buy_effective_qty",
    "sell_effective_qty",
    "buy_limit_amount",
    "sell_limit_amount",
    "buy_fill_amount",
    "sell_fill_amount",
    "buy_to_cover_required_qty",
    "cancel_required_qty",
    "pl_rate_exceeded",
    "margin_new_sell_fill_amount",
    "margin_new_sell_limit_amount",
    "prev_day_margin_new_sell_amount",
    "margin_new_sell_amount_total",
    "delta_value",
    "gamma_value",
    "terminal_id",
    "cash_position_amount",
    "sell_pos_count",
    "buy_pos_count",
    "sell_fill_count",
    "buy_fill_count",
    "prev_day_diff",
    "today_sell_price",
    "today_buy_price",
    "today_margin_new_sell_qty",
    "today_tr_pl_ds",
    "today_tr_pl_ev",
    "today_tr_pl_jpy",
    "today_tr_pl_foreign",
    "position_market_value_jpy",
    "position_market_value_foreign",
    "base_fx_rate",
    "live_fx_rate",
    "fx_book_rate",
    "currency",
    "margin_book_value_total",
    "margin_short_open_qty",
    "margin_long_open_qty",
]

INT_FIELDS = {
    "net_qty",
    "buy_qty",
    "sell_qty",
    "delta_qty",
    "buy_effective_qty",
    "sell_effective_qty",
    "buy_to_cover_required_qty",
    "cancel_required_qty",
    "sell_pos_count",
    "buy_pos_count",
    "sell_fill_count",
    "buy_fill_count",
    "today_margin_new_sell_qty",
    "margin_short_open_qty",
    "margin_long_open_qty",
}

FLOAT_FIELDS = {
    "book_price",
    "last_price",
    "change_pct",
    "tr_pl",
    "realized_pl",
    "unrealized_pl",
    "position_market_value",
    "net_pl_rate",
    "book_value_net",
    "net_pl",
    "strike_price",
    "sell_price",
    "buy_price",
    "buy_limit_amount",
    "sell_limit_amount",
    "buy_fill_amount",
    "sell_fill_amount",
    "pl_rate_exceeded",
    "margin_new_sell_fill_amount",
    "margin_new_sell_limit_amount",
    "prev_day_margin_new_sell_amount",
    "margin_new_sell_amount_total",
    "delta_value",
    "gamma_value",
    "cash_position_amount",
    "prev_day_diff",
    "today_sell_price",
    "today_buy_price",
    "today_tr_pl_ds",
    "today_tr_pl_ev",
    "today_tr_pl_jpy",
    "today_tr_pl_foreign",
    "position_market_value_jpy",
    "position_market_value_foreign",
    "base_fx_rate",
    "live_fx_rate",
    "fx_book_rate",
    "margin_book_value_total",
}

TSV_REQUIRED_HEADERS = {
    "\u30b3\u30fc\u30c9",
    "\u9298\u67c4\u540d",
    "\u7c3f\u4fa1",
    "\u6642\u4fa1",
    "\u30cd\u30c3\u30c8\u6570\u91cf",
    "\u5546\u54c1\u533a\u5206",
    "\u901a\u8ca8\u533a\u5206",
    "ID\u540d",
}

TSV_COLUMN_MAP = {
    "\u30b3\u30fc\u30c9": "code",
    "\u9298\u67c4\u540d": "name",
    "\u7c3f\u4fa1": "book_price",
    "\u6642\u4fa1": "last_price",
    "\u9a30\u843d\u7387": "change_pct",
    "TR\u640d\u76ca": "tr_pl",
    "\u5b9f\u73fe\u640d\u76ca": "realized_pl",
    "\u8a55\u4fa1\u640d\u76ca": "unrealized_pl",
    "\u30cd\u30c3\u30c8\u6570\u91cf": "net_qty",
    "\u30dd\u30b8\u30b7\u30e7\u30f3\u6642\u4fa1\u7dcf\u984d": "position_market_value",
    "\u30dd\u30b8\u30b7\u30e7\u30f3\u6642\u4fa1\u7dcf\u984d(\u5186\u8ca8)": "position_market_value_jpy",
    "\u30cd\u30c3\u30c8\u640d\u76ca\u7387": "net_pl_rate",
    "\u7c3f\u4fa1\u7dcf\u984d\u30cd\u30c3\u30c8": "book_value_net",
    "\u8cb7\u6570\u91cf": "buy_qty",
    "\u58f2\u6570\u91cf": "sell_qty",
    "\u640d\u76ca": "net_pl",
    "\u5546\u54c1\u533a\u5206": "product_type",
    "INDEX": "index_flag",
    "\u58f2\u4fa1\u683c": "sell_price",
    "\u8cb7\u4fa1\u683c": "buy_price",
    "\u30c7\u30eb\u30bf\u6570\u91cf": "delta_qty",
    "\u8cb7\u6709\u52b9\u6570\u91cf": "buy_effective_qty",
    "\u58f2\u6709\u52b9\u6570\u91cf": "sell_effective_qty",
    "\u8cb7\u6307\u5024\u91d1\u984d": "buy_limit_amount",
    "\u58f2\u6307\u5024\u91d1\u984d": "sell_limit_amount",
    "\u8cb7\u7d04\u5b9a\u91d1\u984d": "buy_fill_amount",
    "\u58f2\u7d04\u5b9a\u91d1\u984d": "sell_fill_amount",
    "\u8cb7\u623b\u5fc5\u8981\u6570\u91cf": "buy_to_cover_required_qty",
    "\u53d6\u6d88\u5fc5\u8981\u6570\u91cf": "cancel_required_qty",
    "\u640d\u76ca\u7387\u8d85\u904e": "pl_rate_exceeded",
    "\u4fe1\u7528\u65b0\u898f\u58f2\u7d04\u5b9a\u91d1\u984d": "margin_new_sell_fill_amount",
    "\u4fe1\u7528\u65b0\u898f\u58f2\u6307\u5024\u91d1\u984d": "margin_new_sell_limit_amount",
    "\u524d\u65e5\u4fe1\u7528\u65b0\u898f\u58f2\u91d1\u984d": "prev_day_margin_new_sell_amount",
    "\u4fe1\u7528\u65b0\u898f\u58f2\u91d1\u984d\u5408\u8a08": "margin_new_sell_amount_total",
    "\u30c7\u30eb\u30bf\u5024": "delta_value",
    "\u30ac\u30f3\u30de\u5024": "gamma_value",
    "ID": "terminal_id",
    "\u73fe\u7269\u30dd\u30b8\u30b7\u30e7\u30f3\u91d1\u984d": "cash_position_amount",
    "\u58f2POS\u6570": "sell_pos_count",
    "\u8cb7POS\u6570": "buy_pos_count",
    "\u58f2\u7d04\u5b9a\u6570": "sell_fill_count",
    "\u8cb7\u7d04\u5b9a\u6570": "buy_fill_count",
    "\u524d\u65e5\u6bd4": "prev_day_diff",
    "\u5f53\u65e5\u58f2\u4fa1\u683c": "today_sell_price",
    "\u5f53\u65e5\u8cb7\u4fa1\u683c": "today_buy_price",
    "\u5f53\u65e5\u4fe1\u7528\u65b0\u898f\u58f2\u6570\u91cf": "today_margin_new_sell_qty",
    "\u5f53\u65e5TR\u640d\u76ca(DS)": "today_tr_pl_ds",
    "\u5f53\u65e5TR\u640d\u76ca(EV)": "today_tr_pl_ev",
    "\u5f53\u65e5TR\u640d\u76ca(\u5186\u8ca8)": "today_tr_pl_jpy",
    "\u5f53\u65e5TR\u640d\u76ca(\u5916\u8ca8)": "today_tr_pl_foreign",
    "\u30dd\u30b8\u30b7\u30e7\u30f3\u6642\u4fa1\u7dcf\u984d(\u5916\u8ca8)": "position_market_value_foreign",
    "\u57fa\u6e96\u70ba\u66ff\u30ec\u30fc\u30c8": "base_fx_rate",
    "\u30ea\u30a2\u30eb\u70ba\u66ff\u30ec\u30fc\u30c8": "live_fx_rate",
    "\u70ba\u66ff\u7c3f\u4fa1": "fx_book_rate",
    "\u901a\u8ca8\u533a\u5206": "currency",
    "\u4fe1\u7528\u7c3f\u4fa1\u7dcf\u984d": "margin_book_value_total",
    "\u4fe1\u7528\u58f2\u5efa\u7389\u6b8b\u6570": "margin_short_open_qty",
    "\u4fe1\u7528\u8cb7\u5efa\u7389\u6b8b\u6570": "margin_long_open_qty",
    "ID\u540d": "id_name",
}

BLOCK_HEADER_LINES = {
    "ID\u540d",
    "\u30b3\u30fc\u30c9",
    "\u9298\u67c4\u540d",
    "\u7c3f\u4fa1",
    "\u6642\u4fa1",
    "\u9a30\u843d\u7387",
}


def normalize_text(value: str) -> str:
    return (
        value.replace("\u3000", " ")
        .replace("\u2015", "-")
        .replace("\u2014", "-")
        .replace("\u30fc", "-")
        .strip()
    )


def normalize_header_text(value: str) -> str:
    return value.replace("\u3000", " ").strip()


def parse_number(value: str | None) -> float | None:
    if value is None:
        return None
    text = normalize_text(value)
    if not text or text == "--":
        return None
    text = text.replace(",", "")
    if not NUMERIC_RE.match(text):
        return None
    return float(text)


def parse_int(value: str | None) -> int:
    parsed = parse_number(value)
    if parsed is None:
        return 0
    return int(round(parsed))


def is_code_line(value: str) -> bool:
    return bool(CODE_RE.match(normalize_text(value)))


def is_numeric_line(value: str) -> bool:
    return parse_number(value) is not None


def is_name_line(value: str) -> bool:
    text = normalize_text(value)
    return bool(text) and not is_code_line(text) and not is_numeric_line(text)


def detect_account_category(id_name: str) -> str:
    text = normalize_text(id_name)
    if "NPB" in text:
        return "NPB"
    if "\u4fe1\u7528" in text or "\uff08\u4fe1\uff09" in text or "(\u4fe1)" in text:
        return "\u4fe1\u7528"
    return "\u73fe\u7269"


def normalize_direction(net_qty: int) -> str:
    if net_qty > 0:
        return "\u8cb7\u3044"
    if net_qty < 0:
        return "\u58f2\u308a"
    return "\u30d5\u30e9\u30c3\u30c8"


def build_strategy_key(parsed: dict[str, object]) -> str:
    terminal_id = normalize_text(str(parsed.get("terminal_id") or ""))
    if terminal_id and terminal_id not in {"0", "--"}:
        return terminal_id
    stable = "|".join(
        [
            normalize_text(str(parsed.get("id_name") or "")),
            normalize_text(str(parsed.get("code") or "")),
            normalize_text(str(parsed.get("name") or "")),
            normalize_text(str(parsed.get("product_type") or "")),
        ]
    )
    return hashlib.sha1(stable.encode("utf-8")).hexdigest()[:12]


def empty_parsed_values() -> dict[str, object]:
    parsed: dict[str, object] = {}
    for field in FIXED_COLUMNS:
        if field in INT_FIELDS:
            parsed[field] = 0
        elif field in FLOAT_FIELDS:
            parsed[field] = None
        else:
            parsed[field] = None
    return parsed


def coerce_field_value(field: str, value: str | None) -> object:
    if field in INT_FIELDS:
        return parse_int(value)
    if field in FLOAT_FIELDS:
        return parse_number(value)
    text = normalize_text(value or "")
    return text or None


@dataclass
class ParsedPosition:
    id_name: str
    account_category: str
    code: str
    name: str
    product_type: str | None
    index_flag: str | None
    currency: str | None
    strategy_key: str
    direction: str
    terminal_id: str | None
    book_price: float | None
    last_price: float | None
    change_pct: float | None
    tr_pl: float | None
    realized_pl: float | None
    unrealized_pl: float | None
    net_qty: int
    position_market_value: float | None
    net_pl_rate: float | None
    book_value_net: float | None
    offset_trade: float | None
    buy_qty: int
    sell_qty: int
    net_pl: float | None
    strike_price: float | None
    contract_month: str | None
    call_put: str | None
    sell_price: float | None
    buy_price: float | None
    delta_qty: int
    buy_effective_qty: int
    sell_effective_qty: int
    buy_limit_amount: float | None
    sell_limit_amount: float | None
    buy_fill_amount: float | None
    sell_fill_amount: float | None
    buy_to_cover_required_qty: int
    cancel_required_qty: int
    pl_rate_exceeded: float | None
    margin_new_sell_fill_amount: float | None
    margin_new_sell_limit_amount: float | None
    prev_day_margin_new_sell_amount: float | None
    margin_new_sell_amount_total: float | None
    delta_value: float | None
    gamma_value: float | None
    cash_position_amount: float | None
    sell_pos_count: int
    buy_pos_count: int
    sell_fill_count: int
    buy_fill_count: int
    prev_day_diff: float | None
    today_sell_price: float | None
    today_buy_price: float | None
    today_margin_new_sell_qty: int
    today_tr_pl_ds: float | None
    today_tr_pl_ev: float | None
    today_tr_pl_jpy: float | None
    today_tr_pl_foreign: float | None
    position_market_value_jpy: float | None
    position_market_value_foreign: float | None
    base_fx_rate: float | None
    live_fx_rate: float | None
    fx_book_rate: float | None
    margin_book_value_total: float | None
    margin_short_open_qty: int
    margin_long_open_qty: int
    board_set_label: str | None
    send_label: str | None
    raw_values: str
    source_block: str

    def as_dict(self) -> dict:
        return asdict(self)


def finalize_parsed_position(parsed: dict[str, object], source_lines: list[str]) -> ParsedPosition:
    parsed["id_name"] = normalize_text(str(parsed.get("id_name") or ""))
    parsed["code"] = normalize_text(str(parsed.get("code") or ""))
    parsed["name"] = normalize_text(str(parsed.get("name") or ""))
    parsed["account_category"] = detect_account_category(str(parsed["id_name"]))

    net_qty = int(parsed.get("net_qty") or 0)
    buy_qty = int(parsed.get("buy_qty") or 0)
    sell_qty = int(parsed.get("sell_qty") or 0)
    if net_qty == 0 and (buy_qty or sell_qty):
        parsed["net_qty"] = buy_qty - sell_qty

    if parsed.get("buy_price") in (None, 0) and buy_qty > 0:
        parsed["buy_price"] = parsed.get("book_price")
    if parsed.get("sell_price") in (None, 0) and sell_qty > 0:
        parsed["sell_price"] = parsed.get("book_price")
    if parsed.get("position_market_value_jpy") in (None, 0):
        parsed["position_market_value_jpy"] = parsed.get("position_market_value")
    if parsed.get("position_market_value") in (None, 0):
        parsed["position_market_value"] = parsed.get("position_market_value_jpy")
    if parsed.get("net_pl") is None:
        parsed["net_pl"] = parsed.get("unrealized_pl")
    parsed["offset_trade"] = None

    parsed["direction"] = normalize_direction(int(parsed["net_qty"]))
    parsed["strategy_key"] = build_strategy_key(parsed)
    parsed["raw_values"] = "|".join(source_lines)
    parsed["source_block"] = "\n".join(source_lines)
    return ParsedPosition(**parsed)


def split_blocks(raw_text: str) -> list[list[str]]:
    lines = [normalize_text(line) for line in raw_text.splitlines()]
    lines = [line for line in lines if line not in BLOCK_HEADER_LINES]

    blocks: list[list[str]] = []
    current: list[str] = []
    i = 0
    while i < len(lines):
        if not current and not lines[i]:
            i += 1
            continue
        new_block = (
            i + 2 < len(lines)
            and is_name_line(lines[i])
            and is_code_line(lines[i + 1])
            and is_name_line(lines[i + 2])
        )
        if new_block:
            if current:
                blocks.append(current)
            current = [lines[i], lines[i + 1], lines[i + 2]]
            i += 3
            continue
        if current:
            current.append(lines[i])
        i += 1

    if current:
        blocks.append(current)
    return blocks


def parse_block(block: list[str]) -> ParsedPosition:
    padded = block[: len(FIXED_COLUMNS)] + [""] * max(0, len(FIXED_COLUMNS) - len(block))
    parsed = empty_parsed_values()
    for index, field in enumerate(FIXED_COLUMNS):
        parsed[field] = coerce_field_value(field, padded[index])
    return finalize_parsed_position(parsed, block[: len(FIXED_COLUMNS)])


def is_tsv_table(raw_text: str) -> bool:
    lines = [line for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return False
    headers = {normalize_header_text(cell) for cell in lines[0].split("\t")}
    return TSV_REQUIRED_HEADERS.issubset(headers)


def parse_tsv_positions(raw_text: str) -> list[ParsedPosition]:
    lines = [line.rstrip("\r") for line in raw_text.splitlines() if line.strip()]
    if len(lines) < 2:
        return []

    headers = [normalize_header_text(cell) for cell in lines[0].split("\t")]
    positions: list[ParsedPosition] = []
    for row_line in lines[1:]:
        cells = row_line.split("\t")
        if len(cells) < len(headers):
            cells.extend([""] * (len(headers) - len(cells)))

        row = {headers[idx]: cells[idx] if idx < len(cells) else "" for idx in range(len(headers))}
        code = normalize_text(row.get("\u30b3\u30fc\u30c9", ""))
        name = normalize_text(row.get("\u9298\u67c4\u540d", ""))
        if not code or not name:
            continue

        parsed = empty_parsed_values()
        for header, field in TSV_COLUMN_MAP.items():
            if header in row:
                parsed[field] = coerce_field_value(field, row.get(header))
        positions.append(finalize_parsed_position(parsed, [row_line]))
    return positions


def parse_positions(raw_text: str) -> list[ParsedPosition]:
    if is_tsv_table(raw_text):
        return parse_tsv_positions(raw_text)

    positions: list[ParsedPosition] = []
    for block in split_blocks(raw_text):
        if len(block) < 20:
            continue
        try:
            positions.append(parse_block(block))
        except Exception:
            continue
    return positions


# ================================================================
# 約定履歴 (trade executions) parser
# ================================================================

# normalize_text が長音符(ー) を "-" に変換するため、検出用はハイフン表記で統一
TRADE_HEADER_ALIASES = {
    "約定時間": "executed_at",
    "銘柄名": "name",
    "約定値段": "price",
    "約定数量": "quantity",
    "銘柄コ-ド": "code",
    "約定番号": "trade_no",
    "市場": "market",
    "受付番号": "receipt_no",
    "出来": "fill_flag",
    "社内処理番号": "internal_no",
    "売買": "side",
    "値段符号": "price_sign",
}

TRADE_REQUIRED_HEADERS = {"約定時間", "銘柄名", "約定値段", "約定数量", "銘柄コ-ド", "売買"}


@dataclass
class ParsedTrade:
    executed_at: str | None  # ISO "YYYY-MM-DDTHH:MM:SS" or None
    trade_date: str | None  # "YYYY-MM-DD" derived from executed_at
    code: str
    name: str
    market: str
    side: str  # 買 / 売
    price: float
    quantity: int
    trade_no: str
    receipt_no: str
    fill_flag: str
    internal_no: str
    price_sign: str
    raw_row: str

    def as_dict(self) -> dict:
        return asdict(self)


def _parse_trade_datetime(value: str) -> tuple[str | None, str | None]:
    """`2026/4/16 15:30` 形式を ISO datetime と YYYY-MM-DD に変換。失敗時は (None, None)。"""
    text = normalize_text(value)
    if not text:
        return None, None
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S"), dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # 日付のみ
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%dT00:00:00"), dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None, None


def _parse_trade_number(value: str) -> float:
    text = normalize_text(value).replace(",", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _parse_trade_int(value: str) -> int:
    text = normalize_text(value).replace(",", "")
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def is_trade_tsv(raw_text: str) -> bool:
    lines = [line for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return False
    header_cells = [normalize_text(c) for c in lines[0].split("\t")]
    return TRADE_REQUIRED_HEADERS.issubset(set(header_cells))


def parse_trade_tsv(raw_text: str, fallback_date: str | None = None) -> list[ParsedTrade]:
    """TSV 形式の約定履歴を ParsedTrade のリストに変換。

    fallback_date: executed_at に日付が含まれない場合に使う YYYY-MM-DD。
    """
    lines = [line for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return []
    header_cells = [normalize_text(c) for c in lines[0].split("\t")]
    # ヘッダ行が無いケースもあるが、取込フォーマットは必ずヘッダ付きと想定
    if not TRADE_REQUIRED_HEADERS.issubset(set(header_cells)):
        return []

    index_map = {TRADE_HEADER_ALIASES[h]: i for i, h in enumerate(header_cells) if h in TRADE_HEADER_ALIASES}

    trades: list[ParsedTrade] = []
    for row_line in lines[1:]:
        cells = row_line.split("\t")
        if len(cells) < 5:
            continue

        def _get(field: str) -> str:
            idx = index_map.get(field)
            if idx is None or idx >= len(cells):
                return ""
            return cells[idx]

        code = normalize_text(_get("code"))
        name = normalize_text(_get("name"))
        side = normalize_text(_get("side"))
        price = _parse_trade_number(_get("price"))
        quantity = _parse_trade_int(_get("quantity"))
        if not code or not side or price <= 0 or quantity <= 0:
            continue

        executed_at, trade_date = _parse_trade_datetime(_get("executed_at"))
        if trade_date is None and fallback_date:
            trade_date = fallback_date
            # 時刻のみのケースは executed_at が None のまま

        trades.append(
            ParsedTrade(
                executed_at=executed_at,
                trade_date=trade_date,
                code=code,
                name=name,
                market=normalize_text(_get("market")),
                side=side,
                price=price,
                quantity=quantity,
                trade_no=normalize_text(_get("trade_no")),
                receipt_no=normalize_text(_get("receipt_no")),
                fill_flag=normalize_text(_get("fill_flag")),
                internal_no=normalize_text(_get("internal_no")),
                price_sign=normalize_text(_get("price_sign")),
                raw_row=row_line,
            )
        )
    return trades
