from __future__ import annotations

import pandas as pd


COMPARE_KEYS = ["id_name", "code", "name", "account_category", "product_type", "strategy_key"]
SESSION_ORDER = ["寄付", "前場前半", "前場後半", "後場前半", "後場後半", "引け", "時間外", "時間不明"]
SESSION_GROUP_ORDER = ["前場", "後場", "引け・時間外", "時間不明"]
HOLDING_BUCKET_ORDER = ["0-5分", "5-30分", "30-120分", "120分以上", "時間不明"]
OVERNIGHT_HOLDING_DAY_ORDER = ["1日", "2-3日", "4-5日", "6-10日", "11日以上"]
OVERNIGHT_POSITION_SIZE_ORDER = ["50万円未満", "50-100万円", "100-300万円", "300-500万円", "500万円以上"]


def _fill_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = 0
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0)
    return result


def _empty_intraday_roundtrips() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "trade_date",
            "code",
            "name",
            "direction",
            "daytrade_qty",
            "realized_pl",
            "avg_buy_price",
            "avg_sell_price",
            "turnover",
        ]
    )


def _prepare_trades(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return trades_df.copy()

    working = trades_df.copy()
    for column in ["trade_date", "code", "name", "side"]:
        if column not in working.columns:
            working[column] = ""
        working[column] = working[column].fillna("").astype(str).str.strip()

    working = _fill_numeric(working, ["price", "quantity"])
    working = working[
        working["trade_date"].ne("")
        & working["code"].ne("")
        & working["side"].isin(["買", "売"])
        & (working["price"] > 0)
        & (working["quantity"] > 0)
    ].copy()
    if working.empty:
        return working

    working["_executed_ts"] = pd.to_datetime(working.get("executed_at"), errors="coerce")
    if "id" not in working.columns:
        working["id"] = range(1, len(working) + 1)
    working["_row_order"] = range(len(working))
    return working.sort_values(
        ["trade_date", "code", "_executed_ts", "id", "_row_order"],
        kind="stable",
        na_position="last",
    ).reset_index(drop=True)


def _estimate_roundtrip_turnover(group: pd.DataFrame) -> int:
    net_qty = 0
    turnover = 0
    for row in group.itertuples():
        delta = int(row.quantity) if row.side == "買" else -int(row.quantity)
        prev_qty = net_qty
        net_qty += delta
        if prev_qty != 0 and net_qty != 0 and prev_qty * net_qty < 0:
            turnover += 1
        elif prev_qty != 0 and net_qty == 0:
            turnover += 1
    return turnover


def build_intraday_roundtrips(trades_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "trade_date",
        "code",
        "name",
        "direction",
        "daytrade_qty",
        "realized_pl",
        "avg_buy_price",
        "avg_sell_price",
        "turnover",
    ]
    if trades_df.empty:
        return _empty_intraday_roundtrips()

    working = _prepare_trades(trades_df)
    if working.empty:
        return _empty_intraday_roundtrips()

    records: list[dict[str, object]] = []
    grouped = working.groupby(["trade_date", "code"], sort=True, dropna=False)
    for (trade_date, code), group in grouped:
        buy_df = group[group["side"] == "買"]
        sell_df = group[group["side"] == "売"]
        buy_qty = float(buy_df["quantity"].sum())
        sell_qty = float(sell_df["quantity"].sum())
        daytrade_qty = int(min(buy_qty, sell_qty))
        if daytrade_qty <= 0:
            continue

        buy_notional = float((buy_df["price"] * buy_df["quantity"]).sum())
        sell_notional = float((sell_df["price"] * sell_df["quantity"]).sum())
        avg_buy_price = buy_notional / buy_qty if buy_qty > 0 else 0.0
        avg_sell_price = sell_notional / sell_qty if sell_qty > 0 else 0.0
        realized_pl = (avg_sell_price - avg_buy_price) * daytrade_qty
        first_side = group.iloc[0]["side"]
        direction = "買い" if first_side == "買" else "売り"

        records.append(
            {
                "trade_date": trade_date,
                "code": code,
                "name": group.iloc[0]["name"],
                "direction": direction,
                "daytrade_qty": daytrade_qty,
                "realized_pl": realized_pl,
                "avg_buy_price": avg_buy_price,
                "avg_sell_price": avg_sell_price,
                "turnover": _estimate_roundtrip_turnover(group),
            }
        )

    if not records:
        return _empty_intraday_roundtrips()

    result = pd.DataFrame.from_records(records, columns=columns)
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    result = _fill_numeric(result, ["daytrade_qty", "realized_pl", "avg_buy_price", "avg_sell_price", "turnover"])
    result["daytrade_qty"] = result["daytrade_qty"].round(0).astype(int)
    result["turnover"] = result["turnover"].round(0).astype(int)
    return result.sort_values(["trade_date", "code"], kind="stable").reset_index(drop=True)


def classify_session(executed_at_iso: str | None) -> str:
    ts = pd.to_datetime(executed_at_iso, errors="coerce")
    if pd.isna(ts):
        return "時間不明"

    minute_of_day = ts.hour * 60 + ts.minute + ts.second / 60
    if 9 * 60 <= minute_of_day < 9 * 60 + 5:
        return "寄付"
    if 9 * 60 + 5 <= minute_of_day < 10 * 60 + 30:
        return "前場前半"
    if 10 * 60 + 30 <= minute_of_day < 11 * 60 + 30:
        return "前場後半"
    if 12 * 60 + 30 <= minute_of_day < 13 * 60 + 30:
        return "後場前半"
    if 13 * 60 + 30 <= minute_of_day < 14 * 60 + 55:
        return "後場後半"
    if 14 * 60 + 55 <= minute_of_day <= 15 * 60 + 30:
        return "引け"
    return "時間外"


def build_trade_session_summary(trades_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["session", "trade_count", "quantity", "notional", "avg_notional"]
    if trades_df.empty:
        return pd.DataFrame(columns=columns)

    working = _prepare_trades(trades_df)
    if working.empty:
        return pd.DataFrame(columns=columns)

    working["session"] = working["executed_at"].apply(classify_session)
    working["notional"] = working["price"] * working["quantity"]
    result = (
        working.groupby("session", dropna=False, observed=False)
        .agg(
            trade_count=("id", "count"),
            quantity=("quantity", "sum"),
            notional=("notional", "sum"),
            avg_notional=("notional", "mean"),
        )
        .reset_index()
    )
    result["session"] = pd.Categorical(result["session"], categories=SESSION_ORDER, ordered=True)
    return result.sort_values("session").reset_index(drop=True)


def build_trade_habit_profile(trades_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "trade_date",
        "code",
        "name",
        "entry_direction",
        "first_session",
        "executions",
        "buy_exec_count",
        "sell_exec_count",
        "total_qty",
        "total_notional",
        "first_executed_at",
        "last_executed_at",
        "holding_minutes",
        "execution_style",
    ]
    if trades_df.empty:
        return pd.DataFrame(columns=columns)

    working = _prepare_trades(trades_df)
    if working.empty:
        return pd.DataFrame(columns=columns)

    working["session"] = working["executed_at"].apply(classify_session)
    working["notional"] = working["price"] * working["quantity"]
    profile = (
        working.groupby(["trade_date", "code"], sort=True, dropna=False)
        .agg(
            name=("name", "first"),
            first_side=("side", "first"),
            first_session=("session", "first"),
            executions=("id", "count"),
            buy_exec_count=("side", lambda s: int((s == "買").sum())),
            sell_exec_count=("side", lambda s: int((s == "売").sum())),
            total_qty=("quantity", "sum"),
            total_notional=("notional", "sum"),
            first_executed_at=("executed_at", "first"),
            last_executed_at=("executed_at", "last"),
            first_ts=("_executed_ts", "min"),
            last_ts=("_executed_ts", "max"),
        )
        .reset_index()
    )
    profile["entry_direction"] = profile["first_side"].map({"買": "買い", "売": "売り"}).fillna("不明")
    profile["holding_minutes"] = (
        (profile["last_ts"] - profile["first_ts"]).dt.total_seconds().div(60).fillna(0)
    )
    profile["execution_style"] = profile["executions"].apply(
        lambda count: "単発" if count <= 2 else ("分割" if count <= 4 else "多段")
    )
    return (
        profile[columns]
        .sort_values(["trade_date", "first_executed_at", "code"], kind="stable")
        .reset_index(drop=True)
    )


def _classify_session_group(session: str | None) -> str:
    if session in {"寄付", "前場前半", "前場後半"}:
        return "前場"
    if session in {"後場前半", "後場後半"}:
        return "後場"
    if session in {"引け", "時間外"}:
        return "引け・時間外"
    return "時間不明"


def _bucket_holding_minutes(minutes: float | int | None) -> str:
    if minutes is None or pd.isna(minutes):
        return "時間不明"
    value = float(minutes)
    if value < 5:
        return "0-5分"
    if value < 30:
        return "5-30分"
    if value < 120:
        return "30-120分"
    return "120分以上"


def build_roundtrip_profile(trades_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "trade_date",
        "code",
        "name",
        "direction",
        "daytrade_qty",
        "realized_pl",
        "avg_buy_price",
        "avg_sell_price",
        "turnover",
        "first_session",
        "session_group",
        "first_executed_at",
        "last_executed_at",
        "holding_minutes",
        "holding_bucket",
        "executions",
        "total_notional",
        "execution_style",
        "outcome",
        "first_executed_ts",
    ]
    roundtrip_df = build_intraday_roundtrips(trades_df)
    if roundtrip_df.empty:
        return pd.DataFrame(columns=columns)

    habit_df = build_trade_habit_profile(trades_df)
    if habit_df.empty:
        profile = roundtrip_df.copy()
        profile["first_session"] = "時間不明"
        profile["session_group"] = "時間不明"
        profile["first_executed_at"] = None
        profile["last_executed_at"] = None
        profile["holding_minutes"] = None
        profile["holding_bucket"] = "時間不明"
        profile["executions"] = 0
        profile["total_notional"] = 0.0
        profile["execution_style"] = "不明"
        profile["outcome"] = profile["realized_pl"].apply(lambda value: "Win" if value > 0 else ("Lose" if value < 0 else "Even"))
        profile["first_executed_ts"] = pd.NaT
        return profile[columns]

    merge_cols = [
        "trade_date",
        "code",
        "first_session",
        "first_executed_at",
        "last_executed_at",
        "holding_minutes",
        "executions",
        "total_notional",
        "execution_style",
    ]
    profile = roundtrip_df.merge(habit_df[merge_cols], on=["trade_date", "code"], how="left")
    profile["first_session"] = profile["first_session"].fillna("時間不明")
    profile["session_group"] = profile["first_session"].apply(_classify_session_group)
    profile["holding_bucket"] = profile["holding_minutes"].apply(_bucket_holding_minutes)
    profile["outcome"] = profile["realized_pl"].apply(lambda value: "Win" if value > 0 else ("Lose" if value < 0 else "Even"))
    profile["first_executed_ts"] = pd.to_datetime(profile["first_executed_at"], errors="coerce")
    profile["session_group"] = pd.Categorical(profile["session_group"], categories=SESSION_GROUP_ORDER, ordered=True)
    profile["holding_bucket"] = pd.Categorical(profile["holding_bucket"], categories=HOLDING_BUCKET_ORDER, ordered=True)
    return (
        profile[columns]
        .sort_values(["trade_date", "first_executed_ts", "code"], kind="stable")
        .reset_index(drop=True)
    )


def _bucket_holding_days(days: float | int | None) -> str:
    if days is None or pd.isna(days):
        return "11日以上"
    value = float(days)
    if value < 2:
        return "1日"
    if value < 4:
        return "2-3日"
    if value < 6:
        return "4-5日"
    if value < 11:
        return "6-10日"
    return "11日以上"


def _bucket_position_size(market_value_abs: float | int | None) -> str:
    if market_value_abs is None or pd.isna(market_value_abs):
        return "50万円未満"
    value = float(market_value_abs)
    if value < 500_000:
        return "50万円未満"
    if value < 1_000_000:
        return "50-100万円"
    if value < 3_000_000:
        return "100-300万円"
    if value < 5_000_000:
        return "300-500万円"
    return "500万円以上"


def build_overnight_hold_profile(snapshots_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "entry_date",
        "exit_date",
        "code",
        "name",
        "direction",
        "holding_days",
        "observed_days",
        "total_tr_pl",
        "total_realized_pl",
        "avg_daily_tr_pl",
        "final_unrealized_pl",
        "entry_market_value_abs",
        "avg_market_value_abs",
        "max_market_value_abs",
        "position_size_bucket",
        "size_efficiency_pct",
        "daily_efficiency_pct",
        "status",
        "close_reason",
        "outcome",
        "holding_day_bucket",
    ]
    if snapshots_df.empty:
        return pd.DataFrame(columns=columns)

    working = snapshots_df.copy()
    if "snapshot_date" not in working.columns:
        return pd.DataFrame(columns=columns)

    for column in ["code", "name", "account_category", "product_type", "strategy_key"]:
        if column not in working.columns:
            working[column] = ""
        working[column] = working[column].fillna("").astype(str).str.strip()

    if "id_name" not in working.columns:
        working["id_name"] = (
            working["code"].fillna("").astype(str).str.strip()
            + "_"
            + working["name"].fillna("").astype(str).str.strip()
        )

    working["snapshot_date"] = pd.to_datetime(working["snapshot_date"], errors="coerce")
    working = working.dropna(subset=["snapshot_date"]).copy()
    if working.empty:
        return pd.DataFrame(columns=columns)

    working = _fill_numeric(
        working,
        ["net_qty", "tr_pl", "realized_pl", "unrealized_pl", "position_market_value_jpy"],
    )
    working["position_sign"] = working["net_qty"].apply(lambda value: 1 if value > 0 else (-1 if value < 0 else 0))
    group_cols = [column for column in COMPARE_KEYS if column in working.columns]
    if not group_cols:
        group_cols = ["code", "name"]
    working = working.sort_values(group_cols + ["snapshot_date"], kind="stable").reset_index(drop=True)

    records: list[dict[str, object]] = []

    def _finalize_episode(episode: dict[str, object], exit_date: pd.Timestamp, status: str, close_reason: str) -> None:
        holding_days = max(int((exit_date - episode["entry_date"]).days), 0)
        if holding_days < 1:
            return
        total_tr_pl = float(episode["total_tr_pl"])
        observed_days = int(episode["observed_days"])
        avg_market_value_abs = float(episode["market_value_abs_sum"]) / observed_days if observed_days else 0.0
        size_efficiency_pct = total_tr_pl / avg_market_value_abs * 100 if avg_market_value_abs > 0 else 0.0
        daily_efficiency_pct = (
            total_tr_pl / (avg_market_value_abs * observed_days) * 100
            if avg_market_value_abs > 0 and observed_days > 0 else 0.0
        )
        records.append(
            {
                "entry_date": episode["entry_date"],
                "exit_date": exit_date,
                "code": episode["code"],
                "name": episode["name"],
                "direction": episode["direction"],
                "holding_days": holding_days,
                "observed_days": observed_days,
                "total_tr_pl": total_tr_pl,
                "total_realized_pl": float(episode["total_realized_pl"]),
                "avg_daily_tr_pl": total_tr_pl / observed_days if observed_days else 0.0,
                "final_unrealized_pl": float(episode["final_unrealized_pl"]),
                "entry_market_value_abs": float(episode["entry_market_value_abs"]),
                "avg_market_value_abs": avg_market_value_abs,
                "max_market_value_abs": float(episode["max_market_value_abs"]),
                "position_size_bucket": _bucket_position_size(avg_market_value_abs),
                "size_efficiency_pct": size_efficiency_pct,
                "daily_efficiency_pct": daily_efficiency_pct,
                "status": status,
                "close_reason": close_reason,
                "outcome": "Win" if total_tr_pl > 0 else ("Lose" if total_tr_pl < 0 else "Even"),
                "holding_day_bucket": _bucket_holding_days(holding_days),
            }
        )

    for _, group in working.groupby(group_cols, dropna=False, sort=False):
        group = group.sort_values("snapshot_date", kind="stable").reset_index(drop=True)
        episode: dict[str, object] | None = None

        for row in group.itertuples():
            sign = int(getattr(row, "position_sign", 0))
            if episode is None:
                if sign == 0:
                    continue
                episode = {
                    "entry_date": row.snapshot_date,
                    "code": row.code,
                    "name": row.name,
                    "direction": "買い" if sign > 0 else "売り",
                    "sign": sign,
                    "observed_days": 1,
                    "total_tr_pl": float(row.tr_pl),
                    "total_realized_pl": float(row.realized_pl),
                    "final_unrealized_pl": float(row.unrealized_pl),
                    "entry_market_value_abs": abs(float(row.position_market_value_jpy)),
                    "market_value_abs_sum": abs(float(row.position_market_value_jpy)),
                    "max_market_value_abs": abs(float(row.position_market_value_jpy)),
                    "last_observed_date": row.snapshot_date,
                }
                continue

            if sign == int(episode["sign"]):
                episode["observed_days"] = int(episode["observed_days"]) + 1
                episode["total_tr_pl"] = float(episode["total_tr_pl"]) + float(row.tr_pl)
                episode["total_realized_pl"] = float(episode["total_realized_pl"]) + float(row.realized_pl)
                episode["final_unrealized_pl"] = float(row.unrealized_pl)
                episode["market_value_abs_sum"] = float(episode["market_value_abs_sum"]) + abs(float(row.position_market_value_jpy))
                episode["max_market_value_abs"] = max(
                    float(episode["max_market_value_abs"]),
                    abs(float(row.position_market_value_jpy)),
                )
                episode["last_observed_date"] = row.snapshot_date
                continue

            if sign == 0:
                episode["total_tr_pl"] = float(episode["total_tr_pl"]) + float(row.tr_pl)
                episode["total_realized_pl"] = float(episode["total_realized_pl"]) + float(row.realized_pl)
                episode["final_unrealized_pl"] = float(row.unrealized_pl)
                _finalize_episode(episode, row.snapshot_date, "クローズ", "フラット")
                episode = None
                continue

            _finalize_episode(episode, row.snapshot_date, "ドテン", "方向転換")
            episode = {
                "entry_date": row.snapshot_date,
                "code": row.code,
                "name": row.name,
                "direction": "買い" if sign > 0 else "売り",
                "sign": sign,
                "observed_days": 1,
                "total_tr_pl": float(row.tr_pl),
                "total_realized_pl": float(row.realized_pl),
                "final_unrealized_pl": float(row.unrealized_pl),
                "entry_market_value_abs": abs(float(row.position_market_value_jpy)),
                "market_value_abs_sum": abs(float(row.position_market_value_jpy)),
                "max_market_value_abs": abs(float(row.position_market_value_jpy)),
                "last_observed_date": row.snapshot_date,
            }

        if episode is not None:
            _finalize_episode(
                episode,
                pd.Timestamp(episode["last_observed_date"]),
                "継続中",
                "観測末尾",
            )

    if not records:
        return pd.DataFrame(columns=columns)

    result = pd.DataFrame.from_records(records, columns=columns)
    result["entry_date"] = pd.to_datetime(result["entry_date"], errors="coerce")
    result["exit_date"] = pd.to_datetime(result["exit_date"], errors="coerce")
    result["holding_day_bucket"] = pd.Categorical(
        result["holding_day_bucket"],
        categories=OVERNIGHT_HOLDING_DAY_ORDER,
        ordered=True,
    )
    result["position_size_bucket"] = pd.Categorical(
        result["position_size_bucket"],
        categories=OVERNIGHT_POSITION_SIZE_ORDER,
        ordered=True,
    )
    return result.sort_values(["exit_date", "entry_date", "code"], kind="stable").reset_index(drop=True)


def summarize_by_direction(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["方向", "件数", "ネット数量", "評価額(円貨)", "簿価総額ネット", "TR損益", "実現損益", "評価損益", "損益"]
        )

    working = _fill_numeric(
        df,
        ["net_qty", "position_market_value_jpy", "book_value_net", "tr_pl", "realized_pl", "unrealized_pl", "net_pl"],
    )
    return (
        working.groupby("direction", dropna=False)
        .agg(
            件数=("code", "count"),
            ネット数量=("net_qty", "sum"),
            **{
                "評価額(円貨)": ("position_market_value_jpy", "sum"),
                "簿価総額ネット": ("book_value_net", "sum"),
                "TR損益": ("tr_pl", "sum"),
                "実現損益": ("realized_pl", "sum"),
                "評価損益": ("unrealized_pl", "sum"),
                "損益": ("net_pl", "sum"),
            },
        )
        .reset_index()
        .rename(columns={"direction": "方向"})
        .sort_values("方向")
    )


def summarize_by_account_category(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["口座区分", "件数", "買数量", "売数量", "ネット数量", "評価額(円貨)", "損益"])

    working = _fill_numeric(df, ["buy_qty", "sell_qty", "net_qty", "position_market_value_jpy", "net_pl"])
    return (
        working.groupby("account_category", dropna=False)
        .agg(
            件数=("code", "count"),
            買数量=("buy_qty", "sum"),
            売数量=("sell_qty", "sum"),
            ネット数量=("net_qty", "sum"),
            **{"評価額(円貨)": ("position_market_value_jpy", "sum"), "損益": ("net_pl", "sum")},
        )
        .reset_index()
        .rename(columns={"account_category": "口座区分"})
        .sort_values("口座区分")
    )


def _classify_action(
    prev_qty: float,
    curr_qty: float,
    buy_qty: float = 0,
    sell_qty: float = 0,
    realized_pl: float = 0,
) -> str:
    # デイトレ判定: 前日も当日もポジション0だが、売買と実現損益がある
    if prev_qty == 0 and curr_qty == 0:
        if buy_qty > 0 and sell_qty > 0:
            return "デイトレ"
        if realized_pl != 0 and (buy_qty > 0 or sell_qty > 0):
            return "デイトレ"
        return "変化なし"
    if prev_qty == 0 and curr_qty > 0:
        return "新規買い"
    if prev_qty == 0 and curr_qty < 0:
        return "新規売り"
    if prev_qty > 0 and curr_qty == 0:
        return "買い解消"
    if prev_qty < 0 and curr_qty == 0:
        return "売り解消"
    if prev_qty > 0 and curr_qty < 0:
        return "ドテン売り"
    if prev_qty < 0 and curr_qty > 0:
        return "ドテン買い"
    if prev_qty > 0 and curr_qty > prev_qty:
        return "買い増し"
    if prev_qty > 0 and 0 < curr_qty < prev_qty:
        return "買い返済"
    if prev_qty < 0 and abs(curr_qty) > abs(prev_qty):
        return "売り増し"
    if prev_qty < 0 and abs(curr_qty) < abs(prev_qty):
        return "売り返済"
    return "変化なし"


def compare_snapshots(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    if current_df.empty and previous_df.empty:
        return pd.DataFrame()

    if current_df.empty:
        current_df = pd.DataFrame(columns=previous_df.columns)
    if previous_df.empty:
        previous_df = pd.DataFrame(columns=current_df.columns)

    current = current_df.copy().rename(
        columns={
            "direction": "direction_curr",
            "net_qty": "net_qty_curr",
            "buy_qty": "buy_qty_curr",
            "sell_qty": "sell_qty_curr",
            "position_market_value_jpy": "market_value_curr",
            "net_pl": "net_pl_curr",
            "unrealized_pl": "unrealized_pl_curr",
            "realized_pl": "realized_pl_curr",
            "tr_pl": "tr_pl_curr",
            "book_price": "book_price_curr",
            "last_price": "last_price_curr",
        }
    )
    previous = previous_df.copy().rename(
        columns={
            "direction": "direction_prev",
            "net_qty": "net_qty_prev",
            "buy_qty": "buy_qty_prev",
            "sell_qty": "sell_qty_prev",
            "position_market_value_jpy": "market_value_prev",
            "net_pl": "net_pl_prev",
            "unrealized_pl": "unrealized_pl_prev",
            "realized_pl": "realized_pl_prev",
            "tr_pl": "tr_pl_prev",
            "book_price": "book_price_prev",
            "last_price": "last_price_prev",
        }
    )
    merged = current.merge(previous, on=COMPARE_KEYS, how="outer")
    for column in [
        "net_qty_curr", "buy_qty_curr", "sell_qty_curr",
        "market_value_curr",
        "net_pl_curr",
        "unrealized_pl_curr",
        "realized_pl_curr",
        "tr_pl_curr",
        "book_price_curr",
        "last_price_curr",
        "net_qty_prev", "buy_qty_prev", "sell_qty_prev",
        "market_value_prev",
        "net_pl_prev",
        "unrealized_pl_prev",
        "realized_pl_prev",
        "tr_pl_prev",
        "book_price_prev",
        "last_price_prev",
    ]:
        if column not in merged.columns:
            merged[column] = 0
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0)

    merged["action_type"] = merged.apply(
        lambda row: _classify_action(
            row["net_qty_prev"], row["net_qty_curr"],
            buy_qty=row.get("buy_qty_curr", 0),
            sell_qty=row.get("sell_qty_curr", 0),
            realized_pl=row.get("realized_pl_curr", 0),
        ),
        axis=1,
    )
    merged["qty_diff"] = merged["net_qty_curr"] - merged["net_qty_prev"]
    merged["market_value_diff"] = merged["market_value_curr"] - merged["market_value_prev"]
    merged["net_pl_diff"] = merged["net_pl_curr"] - merged["net_pl_prev"]
    merged["unrealized_pl_diff"] = merged["unrealized_pl_curr"] - merged["unrealized_pl_prev"]
    merged["realized_pl_diff"] = merged["realized_pl_curr"] - merged["realized_pl_prev"]
    merged["tr_pl_diff"] = merged["tr_pl_curr"] - merged["tr_pl_prev"]
    merged["direction"] = merged["direction_curr"].fillna(merged["direction_prev"]).fillna("フラット")
    merged["quantity_change"] = merged["net_qty_prev"].astype(int).astype(str) + " -> " + merged["net_qty_curr"].astype(int).astype(str)

    # アクション分の損益を推定
    def _action_pl(row):
        action = row["action_type"]
        realized = row["realized_pl_curr"]
        qty_diff = row["qty_diff"]
        book_curr = row["book_price_curr"]
        last_curr = row["last_price_curr"]

        # ロング/ショートで符号が逆
        # ロング: 含み益 = (時価 - 簿価) × 数量  (値上がりで利益)
        # ショート: 含み益 = (簿価 - 時価) × 数量  (値下がりで利益)
        def _unrealized(book, last, qty):
            if not book or not last or qty == 0:
                return 0
            if qty > 0:  # ロング
                return (last - book) * abs(qty)
            else:  # ショート
                return (book - last) * abs(qty)

        # デイトレ・解消: 実現損益がそのままアクションの結果
        if action in ("デイトレ", "買い解消", "売り解消"):
            return realized

        # 新規: 含み損益 (方向を考慮)
        if action in ("新規買い", "新規売り"):
            curr_qty = row["net_qty_curr"]
            return _unrealized(book_curr, last_curr, curr_qty) + realized

        # 増し: 新規分のエントリー価格を逆算して含み損益を計算
        if action in ("買い増し", "売り増し"):
            book_prev = row["book_price_prev"]
            qty_prev = row["net_qty_prev"]
            qty_curr = row["net_qty_curr"]
            # 新規分エントリー = (当日簿価×当日数量 - 前日簿価×前日数量) / 差分数量
            if book_prev and book_curr and qty_diff != 0:
                new_entry = (book_curr * abs(qty_curr) - book_prev * abs(qty_prev)) / abs(qty_diff)
                return _unrealized(new_entry, last_curr, qty_diff) + realized
            return _unrealized(book_curr, last_curr, qty_diff) + realized

        # 返済: 実現損益
        if action in ("買い返済", "売り返済"):
            return realized

        # ドテン: 実現損益 + 新ポジションの含み損益
        if action in ("ドテン買い", "ドテン売り"):
            curr_qty = row["net_qty_curr"]
            return realized + _unrealized(book_curr, last_curr, curr_qty)

        return row["tr_pl_diff"]

    merged["action_pl"] = merged.apply(_action_pl, axis=1).round(0).astype(int)

    # TR損益を既存分 / 新規分に分解
    merged["tr_pl_new"] = merged["action_pl"]
    merged["tr_pl_existing"] = merged.apply(
        lambda r: 0 if r["action_type"] in ("デイトレ", "新規買い", "新規売り") else int(round(r["tr_pl_diff"] - r["action_pl"])),
        axis=1,
    )

    # 勝敗判定: アクション分損益で判定
    def _judge(row):
        pl = row["action_pl"]
        if pl > 0:
            return "Win"
        elif pl < 0:
            return "Lose"
        return "Even"

    merged["result"] = merged.apply(_judge, axis=1)

    cols = [
        "action_type", "result", "action_pl",
        "id_name", "code", "name", "account_category", "product_type",
        "direction", "quantity_change",
        "net_qty_prev", "net_qty_curr", "qty_diff",
        "book_price_prev", "book_price_curr",
        "last_price_prev", "last_price_curr",
        "market_value_prev", "market_value_curr", "market_value_diff",
        "tr_pl_curr", "tr_pl_diff", "tr_pl_new", "tr_pl_existing",
        "realized_pl_diff", "unrealized_pl_diff",
        "net_pl_prev", "net_pl_curr", "net_pl_diff",
    ]
    available = [c for c in cols if c in merged.columns]

    return (
        merged[available]
        .rename(
            columns={
                "action_type": "当日アクション",
                "result": "勝敗",
                "action_pl": "アクション損益",
                "id_name": "ID名",
                "code": "コード",
                "name": "銘柄名",
                "account_category": "口座区分",
                "product_type": "商品区分",
                "direction": "方向",
                "quantity_change": "数量変化",
                "net_qty_prev": "前日数量",
                "net_qty_curr": "当日数量",
                "qty_diff": "数量差分",
                "book_price_prev": "前日簿価",
                "book_price_curr": "当日簿価",
                "last_price_prev": "前日時価",
                "last_price_curr": "当日時価",
                "market_value_prev": "前日評価額",
                "market_value_curr": "当日評価額",
                "market_value_diff": "評価額差分",
                "tr_pl_curr": "TR損益",
                "tr_pl_diff": "TR損益差分",
                "tr_pl_new": "TR(新規分)",
                "tr_pl_existing": "TR(既存分)",
                "realized_pl_diff": "実現損益差分",
                "unrealized_pl_diff": "評価損益差分",
                "net_pl_prev": "前日損益",
                "net_pl_curr": "当日損益",
                "net_pl_diff": "損益差分",
            }
        )
        .sort_values(["当日アクション", "ID名", "コード"], kind="stable")
        .reset_index(drop=True)
    )


def build_action_summary(compared_df: pd.DataFrame) -> pd.DataFrame:
    if compared_df.empty:
        return pd.DataFrame(columns=["当日アクション", "件数"])

    agg_dict = {
        "件数": ("コード", "count"),
        "Win": ("勝敗", lambda x: (x == "Win").sum()),
        "Lose": ("勝敗", lambda x: (x == "Lose").sum()),
        "Even": ("勝敗", lambda x: (x == "Even").sum()),
    }
    if "アクション損益" in compared_df.columns:
        agg_dict["アクション損益合計"] = ("アクション損益", "sum")
    if "実現損益差分" in compared_df.columns:
        agg_dict["実現損益合計"] = ("実現損益差分", "sum")

    summary = (
        compared_df.groupby("当日アクション", dropna=False)
        .agg(**agg_dict)
        .reset_index()
    )
    summary["勝率"] = summary.apply(
        lambda r: f"{r['Win'] / (r['Win'] + r['Lose']) * 100:.0f}%" if (r["Win"] + r["Lose"]) > 0 else "-",
        axis=1,
    )
    return summary.sort_values(["件数", "当日アクション"], ascending=[False, True], kind="stable")


def build_daily_trend(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["snapshot_date", "TR損益", "実現損益", "評価損益", "損益", "評価額(円貨)", "件数"])

    working = _fill_numeric(df, ["tr_pl", "realized_pl", "unrealized_pl", "net_pl", "position_market_value_jpy"])
    result = (
        working.groupby("snapshot_date", dropna=False)
        .agg(
            **{
                "TR損益": ("tr_pl", "sum"),
                "実現損益": ("realized_pl", "sum"),
                "評価損益": ("unrealized_pl", "sum"),
                "損益": ("net_pl", "sum"),
                "評価額(円貨)": ("position_market_value_jpy", "sum"),
                "件数": ("code", "count"),
            }
        )
        .reset_index()
    )
    result["snapshot_date"] = pd.to_datetime(result["snapshot_date"])
    return result.sort_values("snapshot_date").reset_index(drop=True)


def build_daily_trend_by_direction(df: pd.DataFrame) -> pd.DataFrame:
    """日次×方向(買い/売り/フラット)で損益を集計した long-form DataFrame を返す。"""
    columns = ["snapshot_date", "方向", "TR損益", "実現損益", "評価損益", "損益", "評価額(円貨)", "件数"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    working = _fill_numeric(df, ["tr_pl", "realized_pl", "unrealized_pl", "net_pl", "position_market_value_jpy"])
    if "direction" not in working.columns:
        working = working.copy()
        working["direction"] = "フラット"
    working["direction"] = working["direction"].fillna("フラット").replace("", "フラット")

    result = (
        working.groupby(["snapshot_date", "direction"], dropna=False)
        .agg(
            **{
                "TR損益": ("tr_pl", "sum"),
                "実現損益": ("realized_pl", "sum"),
                "評価損益": ("unrealized_pl", "sum"),
                "損益": ("net_pl", "sum"),
                "評価額(円貨)": ("position_market_value_jpy", "sum"),
                "件数": ("code", "count"),
            }
        )
        .reset_index()
        .rename(columns={"direction": "方向"})
    )
    result["snapshot_date"] = pd.to_datetime(result["snapshot_date"])
    return result.sort_values(["snapshot_date", "方向"]).reset_index(drop=True)


def build_daily_exposure(df: pd.DataFrame) -> pd.DataFrame:
    """日次のロング/ショート評価額・グロス/ネット・傾き(ネット÷グロス)を集計。

    符号は position_market_value_jpy の符号で分類(市場タブの既存集計と同一基準)。
    """
    columns = ["snapshot_date", "ロング評価額", "ショート評価額", "グロス評価額", "ネット評価額", "傾き"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    work = df.copy()
    mv = pd.to_numeric(work.get("position_market_value_jpy", 0), errors="coerce").fillna(0)
    work["_long_mv"] = mv.clip(lower=0)
    work["_short_mv"] = mv.clip(upper=0)
    result = (
        work.groupby("snapshot_date", dropna=False)
        .agg(
            **{
                "ロング評価額": ("_long_mv", "sum"),
                "ショート評価額": ("_short_mv", "sum"),
            }
        )
        .reset_index()
    )
    result["グロス評価額"] = result["ロング評価額"] - result["ショート評価額"]
    result["ネット評価額"] = result["ロング評価額"] + result["ショート評価額"]
    result["傾き"] = result.apply(
        lambda r: r["ネット評価額"] / r["グロス評価額"] if r["グロス評価額"] > 0 else 0.0,
        axis=1,
    )
    result["snapshot_date"] = pd.to_datetime(result["snapshot_date"])
    return result.sort_values("snapshot_date").reset_index(drop=True)


def build_monthly_pnl(df: pd.DataFrame, month: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        empty_daily = pd.DataFrame(columns=["snapshot_date", "TR損益", "実現損益", "評価損益", "損益", "評価額(円貨)", "件数"])
        empty_contrib = pd.DataFrame(columns=["コード", "銘柄名", "TR損益", "実現損益", "評価損益", "損益", "平均ネット数量"])
        return empty_daily, empty_contrib

    working = df.copy()
    working["snapshot_date"] = pd.to_datetime(working["snapshot_date"])
    month_period = pd.Period(month, freq="M")
    month_df = working[working["snapshot_date"].dt.to_period("M") == month_period]
    if month_df.empty:
        return build_daily_trend(month_df), pd.DataFrame(columns=["コード", "銘柄名", "TR損益", "実現損益", "評価損益", "損益", "平均ネット数量"])

    daily = build_daily_trend(month_df)
    monthly = _fill_numeric(month_df, ["tr_pl", "realized_pl", "unrealized_pl", "net_pl", "net_qty"])
    contribution = (
        monthly.groupby(["code", "name"], dropna=False)
        .agg(
            **{
                "TR損益": ("tr_pl", "sum"),
                "実現損益": ("realized_pl", "sum"),
                "評価損益": ("unrealized_pl", "sum"),
                "損益": ("net_pl", "sum"),
                "平均ネット数量": ("net_qty", "mean"),
            }
        )
        .reset_index()
        .rename(columns={"code": "コード", "name": "銘柄名"})
        .sort_values(["損益", "コード"], ascending=[False, True], kind="stable")
        .reset_index(drop=True)
    )
    return daily, contribution


def build_instrument_timeline(df: pd.DataFrame, code: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["snapshot_date", "コード", "銘柄名", "ネット数量", "買数量", "売数量", "評価額(円貨)", "評価損益", "実現損益", "TR損益", "損益", "方向"]
        )

    target = df[df["code"] == code].copy()
    if target.empty:
        return pd.DataFrame(
            columns=["snapshot_date", "コード", "銘柄名", "ネット数量", "買数量", "売数量", "評価額(円貨)", "評価損益", "実現損益", "TR損益", "損益", "方向"]
        )

    target["snapshot_date"] = pd.to_datetime(target["snapshot_date"])
    target = _fill_numeric(
        target,
        ["net_qty", "buy_qty", "sell_qty", "position_market_value_jpy", "unrealized_pl", "realized_pl", "tr_pl", "net_pl"],
    )
    timeline = (
        target.groupby("snapshot_date", dropna=False)
        .agg(
            コード=("code", "first"),
            銘柄名=("name", "first"),
            ネット数量=("net_qty", "sum"),
            買数量=("buy_qty", "sum"),
            売数量=("sell_qty", "sum"),
            **{
                "評価額(円貨)": ("position_market_value_jpy", "sum"),
                "評価損益": ("unrealized_pl", "sum"),
                "実現損益": ("realized_pl", "sum"),
                "TR損益": ("tr_pl", "sum"),
                "損益": ("net_pl", "sum"),
            },
        )
        .reset_index()
        .sort_values("snapshot_date")
        .reset_index(drop=True)
    )
    timeline["方向"] = timeline["ネット数量"].apply(lambda qty: "買い" if qty > 0 else ("売り" if qty < 0 else "フラット"))
    return timeline


def build_instrument_timeline_by_direction(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """銘柄の日次×方向別タイムライン。両建てや日中ドテンを分離して表示できる。"""
    columns = [
        "snapshot_date", "コード", "銘柄名", "方向",
        "ネット数量", "買数量", "売数量",
        "評価額(円貨)", "評価損益", "実現損益", "TR損益", "損益",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)

    target = df[df["code"] == code].copy()
    if target.empty:
        return pd.DataFrame(columns=columns)

    target["snapshot_date"] = pd.to_datetime(target["snapshot_date"])
    target = _fill_numeric(
        target,
        ["net_qty", "buy_qty", "sell_qty", "position_market_value_jpy", "unrealized_pl", "realized_pl", "tr_pl", "net_pl"],
    )
    if "direction" not in target.columns:
        target["direction"] = "フラット"
    target["direction"] = target["direction"].fillna("フラット").replace("", "フラット")

    timeline = (
        target.groupby(["snapshot_date", "direction"], dropna=False)
        .agg(
            コード=("code", "first"),
            銘柄名=("name", "first"),
            ネット数量=("net_qty", "sum"),
            買数量=("buy_qty", "sum"),
            売数量=("sell_qty", "sum"),
            **{
                "評価額(円貨)": ("position_market_value_jpy", "sum"),
                "評価損益": ("unrealized_pl", "sum"),
                "実現損益": ("realized_pl", "sum"),
                "TR損益": ("tr_pl", "sum"),
                "損益": ("net_pl", "sum"),
            },
        )
        .reset_index()
        .rename(columns={"direction": "方向"})
        .sort_values(["snapshot_date", "方向"])
        .reset_index(drop=True)
    )
    return timeline[columns]
