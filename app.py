from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st
from pandas.api.types import is_numeric_dtype

from portfolio_app.analytics import (
    build_action_summary,
    build_daily_trend,
    build_instrument_timeline,
    build_monthly_pnl,
    compare_snapshots,
    summarize_by_account_category,
    summarize_by_direction,
)
from portfolio_app.db import (
    init_db,
    list_snapshot_dates,
    list_snapshot_months,
    load_all_snapshots,
    load_instrument_history,
    load_previous_snapshot,
    load_snapshot,
    load_snapshots_by_month,
    replace_snapshot,
)
from portfolio_app.parser import parse_positions, split_blocks


st.set_page_config(page_title="日本株ポジション管理", layout="wide")
init_db()

DISPLAY_RENAME = {
    "id_name": "ID名",
    "account_category": "口座区分",
    "code": "コード",
    "name": "銘柄名",
    "product_type": "商品区分",
    "index_flag": "INDEX",
    "currency": "通貨区分",
    "strategy_key": "戦略キー",
    "direction": "方向",
    "terminal_id": "ID",
    "book_price": "簿価",
    "last_price": "時価",
    "change_pct": "騰落率",
    "tr_pl": "TR損益",
    "realized_pl": "実現損益",
    "unrealized_pl": "評価損益",
    "net_qty": "ネット数量",
    "position_market_value": "ポジション時価総額",
    "net_pl_rate": "ネット損益率",
    "book_value_net": "簿価総額ネット",
    "buy_qty": "買数量",
    "sell_qty": "売数量",
    "net_pl": "損益",
    "strike_price": "行使価格",
    "contract_month": "限月",
    "call_put": "CALL/PUT",
    "sell_price": "売価格",
    "buy_price": "買価格",
    "delta_qty": "デルタ数量",
    "buy_effective_qty": "買有効数量",
    "sell_effective_qty": "売有効数量",
    "buy_limit_amount": "買指値金額",
    "sell_limit_amount": "売指値金額",
    "buy_fill_amount": "買約定金額",
    "sell_fill_amount": "売約定金額",
    "buy_to_cover_required_qty": "買戻必要数量",
    "cancel_required_qty": "取消必要数量",
    "pl_rate_exceeded": "損益率超過",
    "margin_new_sell_fill_amount": "信用新規売約定金額",
    "margin_new_sell_limit_amount": "信用新規売指値金額",
    "prev_day_margin_new_sell_amount": "前日信用新規売金額",
    "margin_new_sell_amount_total": "信用新規売金額合計",
    "delta_value": "デルタ値",
    "gamma_value": "ガンマ値",
    "cash_position_amount": "現物ポジション金額",
    "sell_pos_count": "売POS数",
    "buy_pos_count": "買POS数",
    "sell_fill_count": "売約定数",
    "buy_fill_count": "買約定数",
    "prev_day_diff": "前日比",
    "today_sell_price": "当日売価格",
    "today_buy_price": "当日買価格",
    "today_margin_new_sell_qty": "当日信用新規売数量",
    "today_tr_pl_ds": "当日TR損益(DS)",
    "today_tr_pl_ev": "当日TR損益(EV)",
    "today_tr_pl_jpy": "当日TR損益(円貨)",
    "today_tr_pl_foreign": "当日TR損益(外貨)",
    "position_market_value_jpy": "ポジション時価総額(円貨)",
    "position_market_value_foreign": "ポジション時価総額(外貨)",
    "base_fx_rate": "基準為替レート",
    "live_fx_rate": "リアル為替レート",
    "fx_book_rate": "為替簿価",
    "margin_book_value_total": "信用簿価総額",
    "margin_short_open_qty": "信用売建玉残数",
    "margin_long_open_qty": "信用買建玉残数",
    "board_set_label": "板セット",
    "send_label": "送信",
}

PREVIEW_COLUMNS = [
    "ID名",
    "コード",
    "銘柄名",
    "簿価",
    "時価",
    "騰落率",
    "TR損益",
    "実現損益",
    "評価損益",
    "ネット数量",
    "ポジション時価総額",
    "ネット損益率",
    "簿価総額ネット",
    "買数量",
    "売数量",
    "損益",
    "商品区分",
    "売価格",
    "買価格",
    "ID",
    "前日比",
    "当日売価格",
    "当日買価格",
    "通貨区分",
]

PERCENT_HINTS = ("率",)

CSS = """
<style>
:root {
  --bg-main: #f4efe6;
  --bg-panel: rgba(255,255,255,0.86);
  --bg-panel-strong: rgba(255,255,255,0.96);
  --ink: #1f2937;
  --muted: #6b7280;
  --line: rgba(148, 163, 184, 0.28);
  --accent: #b45309;
  --accent-soft: #fde6bf;
  --accent-deep: #7c2d12;
  --shadow: 0 24px 60px rgba(120, 53, 15, 0.12);
}

.stApp {
  background:
    radial-gradient(circle at top left, rgba(250, 204, 21, 0.18), transparent 28%),
    radial-gradient(circle at top right, rgba(217, 119, 6, 0.16), transparent 24%),
    linear-gradient(180deg, #fbf7f0 0%, var(--bg-main) 45%, #efe7da 100%);
  color: var(--ink);
}

[data-testid="stHeader"] {
  background: transparent;
}

[data-testid="stSidebar"] {
  background: linear-gradient(180deg, rgba(124,45,18,0.94), rgba(68,64,60,0.96));
}

.block-container {
  padding-top: 2rem;
  padding-bottom: 3rem;
  max-width: 1400px;
}

.hero-shell {
  background:
    linear-gradient(135deg, rgba(255,255,255,0.95), rgba(255,248,235,0.88)),
    linear-gradient(135deg, rgba(180,83,9,0.08), rgba(124,45,18,0.08));
  border: 1px solid rgba(180,83,9,0.14);
  border-radius: 28px;
  padding: 1.5rem 1.6rem;
  box-shadow: var(--shadow);
  margin-bottom: 1.25rem;
}

.hero-kicker {
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.16em;
  color: var(--accent);
  font-weight: 700;
  margin-bottom: 0.5rem;
}

.hero-title {
  font-size: 2.2rem;
  line-height: 1.1;
  font-weight: 800;
  color: var(--accent-deep);
  margin-bottom: 0.55rem;
}

.hero-copy {
  font-size: 1rem;
  color: var(--muted);
  max-width: 860px;
}

.section-note {
  padding: 0.95rem 1rem;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.72);
  border: 1px solid rgba(180,83,9,0.14);
  color: #57534e;
  margin-bottom: 1rem;
}

[data-testid="stMetric"] {
  background: linear-gradient(180deg, rgba(255,255,255,0.94), rgba(255,250,243,0.86));
  border: 1px solid rgba(180,83,9,0.12);
  border-radius: 22px;
  padding: 1rem 1rem 0.9rem;
  box-shadow: 0 18px 40px rgba(120,53,15,0.08);
}

[data-testid="stMetricLabel"] {
  color: var(--muted);
  font-weight: 600;
}

[data-testid="stMetricValue"] {
  color: var(--accent-deep);
}

.stTabs [data-baseweb="tab-list"] {
  gap: 0.45rem;
  background: rgba(255,255,255,0.55);
  border: 1px solid rgba(180,83,9,0.12);
  border-radius: 22px;
  padding: 0.45rem;
  margin-bottom: 1rem;
}

.stTabs [data-baseweb="tab"] {
  background: transparent;
  border-radius: 16px;
  color: #6b7280;
  font-weight: 700;
  min-height: 42px;
  padding: 0.65rem 0.95rem;
}

.stTabs [aria-selected="true"] {
  background: linear-gradient(135deg, #c2410c, #9a3412);
  color: #fff;
}

.stTextArea textarea,
.stTextInput input,
.stDateInput input,
.stSelectbox [data-baseweb="select"] > div {
  background: rgba(255,255,255,0.9);
  border-radius: 16px;
}

.stButton button,
.stDownloadButton button {
  border-radius: 16px;
  border: 1px solid rgba(180,83,9,0.16);
  background: linear-gradient(135deg, #c2410c, #9a3412);
  color: #fff;
  font-weight: 700;
  box-shadow: 0 16px 32px rgba(154,52,18,0.18);
}

.stDataFrame, div[data-testid="stDataFrame"] {
  background: rgba(255,255,255,0.84);
  border: 1px solid rgba(180,83,9,0.12);
  border-radius: 22px;
  overflow: hidden;
}
</style>
"""


def inject_theme() -> None:
    st.markdown(CSS, unsafe_allow_html=True)
    st.markdown(
        """
        <section class="hero-shell">
          <div class="hero-kicker">Daily Position Console</div>
          <div class="hero-title">日本株ポジション管理</div>
          <div class="hero-copy">
            毎日のポジション保存を土台に、差分・推移・月次PL・銘柄別タイムラインまで一つの画面で追えるようにしています。
          </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def prepare_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.rename(columns=DISPLAY_RENAME).copy()


def metric_sum(df: pd.DataFrame, column: str) -> str:
    if column not in df.columns:
        return "0"
    return format_number(pd.to_numeric(df[column], errors="coerce").fillna(0).sum(), column)


def format_number(value: float | int, column_name: str = "") -> str:
    if pd.isna(value):
        return ""
    if any(hint in column_name for hint in PERCENT_HINTS):
        return f"{value:,.2f}"
    if float(value).is_integer():
        return f"{int(round(value)):,}"
    return f"{value:,.2f}"


def format_display_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    formatted = df.copy()
    for column in formatted.columns:
        if is_numeric_dtype(formatted[column]):
            formatted[column] = formatted[column].apply(lambda value: format_number(value, column))
    return formatted


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def render_download(label: str, df: pd.DataFrame, filename: str) -> None:
    st.download_button(label, data=to_csv_bytes(df), file_name=filename, mime="text/csv", disabled=df.empty)


def render_table(df: pd.DataFrame, filename: str | None = None, download_label: str | None = None) -> None:
    display_df = format_display_table(df)
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    if filename and download_label:
        render_download(download_label, df, filename)


inject_theme()

tab_import, tab_summary, tab_trend, tab_actions, tab_compare, tab_symbol, tab_monthly, tab_history = st.tabs(
    ["取込", "日次サマリ", "推移", "当日アクション", "前日比較", "銘柄分析", "月次", "履歴"]
)

with tab_import:
    st.markdown('<div class="section-note">表形式の貼り付けにも対応しています。保存前にプレビューで件数と主要項目を確認できます。</div>', unsafe_allow_html=True)
    snapshot_date = st.date_input("対象日", value=date.today())
    note = st.text_input("メモ", placeholder="任意")
    raw_text = st.text_area("ポジション一覧をそのまま貼り付け", height=420)
    positions = parse_positions(raw_text) if raw_text.strip() else []
    detected_blocks = split_blocks(raw_text) if raw_text.strip() else []
    preview_df = prepare_display_df(pd.DataFrame([row.as_dict() for row in positions]))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("検出件数", format_number(len(positions)))
    c2.metric("ブロック数", format_number(len(detected_blocks)))
    c3.metric("銘柄数", format_number(preview_df["コード"].nunique()) if not preview_df.empty and "コード" in preview_df.columns else "0")
    c4.metric("保存可否", "可" if positions else "不可")

    if not preview_df.empty:
        shown = [column for column in PREVIEW_COLUMNS if column in preview_df.columns]
        render_table(preview_df[shown], f"preview_{snapshot_date.isoformat()}.csv", "プレビューCSVをダウンロード")
    elif raw_text.strip():
        st.warning("ポジションを検出できていません。表のヘッダー行を含めて貼り付けているか確認してください。")
        preview_lines = raw_text.splitlines()[:20]
        if preview_lines:
            st.code("\n".join(preview_lines), language="text")

    if st.button("保存する", type="primary", disabled=not positions):
        replace_snapshot(snapshot_date.isoformat(), raw_text, positions, note)
        st.success(f"{snapshot_date.isoformat()} のデータを保存しました。")

all_df = load_all_snapshots()
snapshot_dates = list_snapshot_dates()
snapshot_months = list_snapshot_months()

with tab_summary:
    if not snapshot_dates:
        st.info("まだ保存データがありません。")
    else:
        selected_date = st.selectbox("対象日", snapshot_dates, key="summary_date")
        snapshot_df = load_snapshot(selected_date)
        display_df = prepare_display_df(snapshot_df)
        direction_summary = summarize_by_direction(snapshot_df)
        category_summary = summarize_by_account_category(snapshot_df)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("TR損益", metric_sum(display_df, "TR損益"))
        c2.metric("実現損益", metric_sum(display_df, "実現損益"))
        c3.metric("評価損益", metric_sum(display_df, "評価損益"))
        c4.metric("時価総額(円貨)", metric_sum(display_df, "ポジション時価総額(円貨)"))

        left, right = st.columns(2)
        with left:
            render_table(direction_summary, f"summary_direction_{selected_date}.csv", "方向別サマリCSV")
        with right:
            render_table(category_summary, f"summary_account_{selected_date}.csv", "口座別サマリCSV")

        shown = [column for column in PREVIEW_COLUMNS if column in display_df.columns]
        render_table(display_df[shown], f"snapshot_{selected_date}.csv", "日次一覧CSV")

with tab_trend:
    trend_df = build_daily_trend(all_df)
    if trend_df.empty:
        st.info("推移を表示するデータがありません。")
    else:
        range_label = st.selectbox("期間", ["全期間", "過去30日", "今月"], key="trend_range")
        view_df = trend_df.copy()
        if range_label == "過去30日":
            cutoff = view_df["snapshot_date"].max() - pd.Timedelta(days=29)
            view_df = view_df[view_df["snapshot_date"] >= cutoff]
        elif range_label == "今月":
            month_period = view_df["snapshot_date"].max().to_period("M")
            view_df = view_df[view_df["snapshot_date"].dt.to_period("M") == month_period]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("最新TR損益", format_number(view_df["TR損益"].iloc[-1], "TR損益"))
        c2.metric("最新実現損益", format_number(view_df["実現損益"].iloc[-1], "実現損益"))
        c3.metric("最新評価損益", format_number(view_df["評価損益"].iloc[-1], "評価損益"))
        c4.metric("最新時価総額(円貨)", format_number(view_df["時価総額(円貨)"].iloc[-1], "時価総額(円貨)"))

        st.line_chart(view_df.set_index("snapshot_date")[["TR損益", "実現損益", "評価損益", "損益"]], use_container_width=True)
        st.line_chart(view_df.set_index("snapshot_date")[["時価総額(円貨)"]], use_container_width=True)

        display_trend = view_df.copy()
        display_trend["snapshot_date"] = display_trend["snapshot_date"].dt.strftime("%Y-%m-%d")
        render_table(display_trend, "daily_trend.csv", "推移CSV")

with tab_actions:
    if not snapshot_dates:
        st.info("比較対象データがありません。")
    else:
        selected_date = st.selectbox("対象日", snapshot_dates, key="action_date")
        current_df = load_snapshot(selected_date)
        previous_df = load_previous_snapshot(selected_date)
        if previous_df.empty:
            st.info("前日データがないため当日アクションを判定できません。")
        else:
            actions_df = compare_snapshots(current_df, previous_df)
            summary_df = build_action_summary(actions_df)
            left, right = st.columns([1, 2])
            with left:
                render_table(summary_df, f"actions_summary_{selected_date}.csv", "当日アクション集計CSV")
            with right:
                options = ["すべて"] + actions_df["当日アクション"].drop_duplicates().tolist()
                focus_action = st.selectbox("絞り込み", options, key="action_filter")
                filtered = actions_df if focus_action == "すべて" else actions_df[actions_df["当日アクション"] == focus_action]
                render_table(filtered, f"actions_{selected_date}.csv", "当日アクションCSV")

with tab_compare:
    if not snapshot_dates:
        st.info("比較対象データがありません。")
    else:
        selected_date = st.selectbox("対象日", snapshot_dates, key="compare_date")
        current_df = load_snapshot(selected_date)
        previous_df = load_previous_snapshot(selected_date)
        if previous_df.empty:
            st.info("前日データがないため比較できません。")
        else:
            compared = compare_snapshots(current_df, previous_df)
            render_table(compared, f"compare_{selected_date}.csv", "前日比較CSV")

with tab_symbol:
    if all_df.empty:
        st.info("銘柄分析に使うデータがありません。")
    else:
        codes = (
            all_df[["code", "name"]]
            .drop_duplicates()
            .sort_values(["code", "name"], kind="stable")
            .assign(label=lambda x: x["code"] + " " + x["name"])
        )
        selected_label = st.selectbox("銘柄", codes["label"].tolist(), key="symbol_code")
        selected_code = selected_label.split(" ", 1)[0]
        instrument_df = load_instrument_history(selected_code)
        timeline_df = build_instrument_timeline(instrument_df, selected_code)
        if timeline_df.empty:
            st.info("この銘柄の履歴はありません。")
        else:
            st.line_chart(timeline_df.set_index("snapshot_date")[["評価損益", "実現損益", "TR損益", "損益"]], use_container_width=True)
            st.line_chart(timeline_df.set_index("snapshot_date")[["ネット数量", "時価総額(円貨)"]], use_container_width=True)
            display_timeline = timeline_df.copy()
            display_timeline["snapshot_date"] = display_timeline["snapshot_date"].dt.strftime("%Y-%m-%d")
            render_table(display_timeline, f"timeline_{selected_code}.csv", "銘柄時系列CSV")

with tab_monthly:
    if not snapshot_months:
        st.info("月次集計に使うデータがありません。")
    else:
        selected_month = st.selectbox("対象月", snapshot_months, key="monthly_month")
        month_df = load_snapshots_by_month(selected_month)
        month_daily, contribution_df = build_monthly_pnl(month_df, selected_month)
        if month_daily.empty:
            st.info("この月のデータはありません。")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("当月TR損益", format_number(month_daily["TR損益"].sum(), "TR損益"))
            c2.metric("当月実現損益", format_number(month_daily["実現損益"].sum(), "実現損益"))
            c3.metric("月末評価損益", format_number(month_daily["評価損益"].iloc[-1], "評価損益"))
            c4.metric("月末時価総額", format_number(month_daily["時価総額(円貨)"].iloc[-1], "時価総額(円貨)"))

            st.line_chart(month_daily.set_index("snapshot_date")[["TR損益", "実現損益", "評価損益", "損益"]], use_container_width=True)

            left, right = st.columns(2)
            with left:
                display_month_daily = month_daily.copy()
                display_month_daily["snapshot_date"] = display_month_daily["snapshot_date"].dt.strftime("%Y-%m-%d")
                render_table(display_month_daily, f"monthly_daily_{selected_month}.csv", "月次日別CSV")
            with right:
                render_table(contribution_df, f"monthly_contribution_{selected_month}.csv", "月次寄与CSV")

with tab_history:
    if not snapshot_dates:
        st.info("保存済みデータはありません。")
    else:
        selected_date = st.selectbox("閲覧日", snapshot_dates, key="history_date")
        history_df = prepare_display_df(load_snapshot(selected_date))
        shown = [column for column in PREVIEW_COLUMNS if column in history_df.columns]
        render_table(history_df[shown], f"history_{selected_date}.csv", "履歴CSV")
