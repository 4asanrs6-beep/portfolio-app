from __future__ import annotations

import pandas as pd


COMPARE_KEYS = ["id_name", "code", "name", "account_category", "product_type", "strategy_key"]


def _fill_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = 0
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0)
    return result


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

        # 増し: 差分数量の含み損益 + 実現損益
        if action in ("買い増し", "売り増し"):
            return _unrealized(book_curr, last_curr, qty_diff) + realized

        # 返済: 実現損益
        if action in ("買い返済", "売り返済"):
            return realized

        # ドテン: 実現損益 + 新ポジションの含み損益
        if action in ("ドテン買い", "ドテン売り"):
            curr_qty = row["net_qty_curr"]
            return realized + _unrealized(book_curr, last_curr, curr_qty)

        return row["tr_pl_diff"]

    merged["action_pl"] = merged.apply(_action_pl, axis=1)

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
        "tr_pl_curr", "tr_pl_diff",
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
