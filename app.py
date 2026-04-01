from __future__ import annotations

import os
from datetime import date

import pandas as pd
import plotly.graph_objects as go
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
    list_risk_limit_months,
    list_snapshot_dates,
    list_snapshot_months,
    load_all_snapshots,
    load_instrument_history,
    load_latest_risk_limits,
    load_previous_snapshot,
    load_risk_limits,
    load_snapshot,
    load_snapshots_by_month,
    replace_snapshot,
    save_risk_limits,
)
from portfolio_app.market_data import (
    BENCHMARK_LABELS,
    JQuantsClient,
    compute_multi_period_metrics,
    compute_portfolio_all,
    compute_portfolio_weights,
    compute_price_chart_data,
    compute_rolling_beta,
    compute_sector_breakdown,
    compute_price_changes,
    enrich_portfolio_with_market_info,
    fetch_portfolio_stock_info,
    is_equity_code,
)
from portfolio_app.parser import parse_positions, split_blocks

# .env ファイルがあれば読み込む
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


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
    "position_market_value": "ポジション評価額",
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
    "position_market_value_jpy": "ポジション評価額(円貨)",
    "position_market_value_foreign": "ポジション評価額(外貨)",
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
    "ポジション評価額",
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

/* ---- Dashboard Table ---- */
.dash-table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(255,250,243,0.90));
  border: 1px solid rgba(180,83,9,0.12);
  border-radius: 20px;
  overflow: hidden;
  box-shadow: 0 12px 32px rgba(120,53,15,0.07);
  margin-bottom: 1.2rem;
  font-size: 0.92rem;
}
.dash-table caption {
  caption-side: top;
  text-align: left;
  font-weight: 800;
  font-size: 1.05rem;
  color: var(--accent-deep);
  padding: 1rem 1.2rem 0.5rem;
  letter-spacing: 0.02em;
}
.dash-table th {
  background: rgba(180,83,9,0.07);
  color: var(--muted);
  font-weight: 700;
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  padding: 0.6rem 1rem;
  text-align: right;
  border-bottom: 1px solid rgba(180,83,9,0.10);
}
.dash-table th:first-child { text-align: left; }
.dash-table td {
  padding: 0.65rem 1rem;
  text-align: right;
  color: var(--ink);
  border-bottom: 1px solid rgba(148,163,184,0.12);
  font-variant-numeric: tabular-nums;
}
.dash-table td:first-child {
  text-align: left;
  font-weight: 700;
  color: var(--accent-deep);
}
.dash-table tr:last-child td { border-bottom: none; }
.dash-table tr:hover td { background: rgba(250,204,21,0.06); }
.dash-table .val-pos { color: #15803d; }
.dash-table .val-neg { color: #dc2626; }
.dash-table .val-muted { color: var(--muted); font-size: 0.82rem; }

.dash-kpi-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 0.8rem;
  margin-bottom: 1.2rem;
}
.dash-kpi {
  background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(255,250,243,0.88));
  border: 1px solid rgba(180,83,9,0.12);
  border-radius: 18px;
  padding: 0.9rem 1rem;
  box-shadow: 0 8px 20px rgba(120,53,15,0.06);
}
.dash-kpi-label {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  font-weight: 700;
  margin-bottom: 0.2rem;
}
.dash-kpi-value {
  font-size: 1.4rem;
  font-weight: 800;
  color: var(--accent-deep);
  font-variant-numeric: tabular-nums;
}
.dash-kpi-sub {
  font-size: 0.76rem;
  color: var(--muted);
  margin-top: 0.15rem;
}

/* ---- Trend Dashboard ---- */
.trend-period-bar {
  display: flex;
  gap: 0.35rem;
  margin-bottom: 1.2rem;
}
.trend-period-btn {
  padding: 0.45rem 1.1rem;
  border-radius: 12px;
  border: 1px solid rgba(180,83,9,0.14);
  background: rgba(255,255,255,0.7);
  color: var(--muted);
  font-weight: 700;
  font-size: 0.82rem;
  cursor: pointer;
  transition: all 0.15s;
}
.trend-period-btn.active,
.trend-period-btn:hover {
  background: linear-gradient(135deg, #c2410c, #9a3412);
  color: #fff;
  border-color: transparent;
}

.trend-kpi-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 0.9rem;
  margin-bottom: 1.4rem;
}
.trend-kpi {
  background: linear-gradient(180deg, rgba(255,255,255,0.97), rgba(255,250,243,0.92));
  border: 1px solid rgba(180,83,9,0.10);
  border-radius: 20px;
  padding: 1.1rem 1.2rem 1rem;
  box-shadow: 0 10px 28px rgba(120,53,15,0.06);
  position: relative;
  overflow: hidden;
}
.trend-kpi::after {
  content: "";
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 3px;
  border-radius: 20px 20px 0 0;
}
.trend-kpi.accent-pl::after   { background: linear-gradient(90deg, #c2410c, #ea580c); }
.trend-kpi.accent-real::after { background: linear-gradient(90deg, #0284c7, #0ea5e9); }
.trend-kpi.accent-eval::after { background: linear-gradient(90deg, #059669, #10b981); }
.trend-kpi.accent-val::after  { background: linear-gradient(90deg, #7c3aed, #8b5cf6); }

.trend-kpi-label {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  font-weight: 700;
  margin-bottom: 0.35rem;
}
.trend-kpi-value {
  font-size: 1.55rem;
  font-weight: 800;
  color: var(--ink);
  font-variant-numeric: tabular-nums;
  line-height: 1.15;
}
.trend-kpi-delta {
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
  font-size: 0.78rem;
  font-weight: 700;
  margin-top: 0.3rem;
  padding: 0.15rem 0.5rem;
  border-radius: 8px;
}
.trend-kpi-delta.up   { color: #15803d; background: rgba(5,150,105,0.08); }
.trend-kpi-delta.down { color: #dc2626; background: rgba(220,38,38,0.08); }
.trend-kpi-delta.flat { color: var(--muted); background: rgba(107,114,128,0.06); }
.trend-kpi-sub {
  font-size: 0.72rem;
  color: var(--muted);
  margin-top: 0.25rem;
}

.trend-section-title {
  font-size: 0.82rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--accent);
  font-weight: 700;
  margin: 1.6rem 0 0.7rem;
  padding-bottom: 0.4rem;
  border-bottom: 1px solid rgba(180,83,9,0.12);
}
</style>
"""


def _v(value, suffix: str = "", fallback: str = "-") -> str:
    """ダッシュボード表示用フォーマッタ。"""
    if value is None or value == "-":
        return fallback
    return f"{value}{suffix}"


def _colored(value, suffix: str = "") -> str:
    """正負で色分けした HTML span を返す。"""
    if value is None or value == "-":
        return '<span class="val-muted">-</span>'
    try:
        v = float(value)
    except (ValueError, TypeError):
        return f"{value}{suffix}"
    cls = "val-pos" if v > 0 else "val-neg" if v < 0 else ""
    return f'<span class="{cls}">{format_number(v)}{suffix}</span>'


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
    st.dataframe(display_df, width="stretch", hide_index=True)
    if filename and download_label:
        render_download(download_label, df, filename)


inject_theme()

tab_import, tab_summary, tab_trend, tab_actions, tab_compare, tab_symbol, tab_market, tab_limits, tab_monthly, tab_history = st.tabs(
    ["取込", "日次サマリ", "推移", "当日アクション", "前日比較", "銘柄分析", "マーケット指標", "リスク枠", "月次", "履歴"]
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
        c4.metric("評価額(円貨)", metric_sum(display_df, "ポジション評価額(円貨)"))

        left, right = st.columns(2)
        with left:
            render_table(direction_summary, f"summary_direction_{selected_date}.csv", "方向別サマリCSV")
        with right:
            render_table(category_summary, f"summary_account_{selected_date}.csv", "口座別サマリCSV")

        # --- リスク枠消化状況 ---
        summary_month = selected_date[:7]
        summary_rl = load_latest_risk_limits(summary_month)
        if summary_rl and not snapshot_df.empty:
            mv_series = pd.to_numeric(snapshot_df["position_market_value_jpy"], errors="coerce").fillna(0)
            s_gross = mv_series.abs().sum()
            s_net = mv_series.sum()

            st.markdown("##### リスク枠消化状況")
            sl1, sl2, sl3, sl4 = st.columns(4)
            sl1.metric("グロス", f"{format_number(s_gross)}円", delta=f"上限 {format_number(summary_rl['gross_limit'] or 0)}円")
            sl2.metric("ネット", f"{format_number(abs(s_net))}円", delta=f"上限 {format_number(summary_rl['net_limit'] or 0)}円")
            sl3.metric("先物枠", f"{format_number(summary_rl['futures_limit'] or 0)}円")
            sl4.metric("損失限度", f"{format_number(summary_rl['monthly_loss_limit'] or 0)}円")

            for s_label, s_actual, s_limit in [("グロス", s_gross, summary_rl["gross_limit"]), ("ネット", abs(s_net), summary_rl["net_limit"])]:
                if s_limit and s_limit > 0:
                    s_ratio = min(s_actual / s_limit, 1.0)
                    st.progress(s_ratio, text=f"{s_label}: {format_number(s_actual)} / {format_number(s_limit)} ({s_ratio * 100:.1f}%)")

        shown = [column for column in PREVIEW_COLUMNS if column in display_df.columns]
        render_table(display_df[shown], f"snapshot_{selected_date}.csv", "日次一覧CSV")

with tab_trend:
    trend_df = build_daily_trend(all_df)
    if trend_df.empty:
        st.info("推移を表示するデータがありません。")
    else:
        # ---- 期間セレクタ ----
        period_options = ["1D", "5D", "1M", "3M", "YTD", "全期間"]
        selected_period = st.radio(
            "期間", period_options, index=4, horizontal=True, key="trend_period",
            label_visibility="collapsed",
        )

        view_df = trend_df.copy()
        max_date = view_df["snapshot_date"].max()
        if selected_period == "1D":
            view_df = view_df[view_df["snapshot_date"] == max_date]
        elif selected_period == "5D":
            view_df = view_df[view_df["snapshot_date"] >= max_date - pd.Timedelta(days=6)]
        elif selected_period == "1M":
            view_df = view_df[view_df["snapshot_date"] >= max_date - pd.Timedelta(days=30)]
        elif selected_period == "3M":
            view_df = view_df[view_df["snapshot_date"] >= max_date - pd.Timedelta(days=90)]
        elif selected_period == "YTD":
            view_df = view_df[view_df["snapshot_date"] >= pd.Timestamp(f"{max_date.year}-01-01")]

        if view_df.empty:
            st.info("選択期間にデータがありません。")
        else:
            # TR損益・実現損益は日次値 → 期間累計に変換
            view_df = view_df.copy()
            view_df["TR損益(累計)"] = view_df["TR損益"].cumsum()
            view_df["実現損益(累計)"] = view_df["実現損益"].cumsum()

            latest = view_df.iloc[-1]
            prev = view_df.iloc[-2] if len(view_df) >= 2 else latest
            first = view_df.iloc[0]

            # 前日比・期間変化
            def _delta_info(col: str) -> tuple[str, str, str]:
                curr = float(latest[col])
                prv = float(prev[col])
                diff = curr - prv
                sign = "+" if diff > 0 else ""
                cls = "up" if diff > 0 else "down" if diff < 0 else "flat"
                return f"{sign}{format_number(diff)}", cls, f"前日比"

            def _period_info(col: str) -> str:
                curr = float(latest[col])
                fst = float(first[col])
                diff = curr - fst
                sign = "+" if diff > 0 else ""
                return f"期間変化: {sign}{format_number(diff)}"

            d_tr, cls_tr, lbl_tr = _delta_info("TR損益(累計)")
            d_real, cls_real, lbl_real = _delta_info("実現損益(累計)")
            d_eval, cls_eval, lbl_eval = _delta_info("評価損益")
            d_val, cls_val, lbl_val = _delta_info("評価額(円貨)")

            # ---- KPI カード ----
            st.markdown(f"""
            <div class="trend-kpi-row">
              <div class="trend-kpi accent-pl">
                <div class="trend-kpi-label">TR損益 (累計)</div>
                <div class="trend-kpi-value">{format_number(latest["TR損益(累計)"])}</div>
                <div class="trend-kpi-delta {cls_tr}">{d_tr} {lbl_tr}</div>
                <div class="trend-kpi-sub">当日: {format_number(latest["TR損益"])}</div>
              </div>
              <div class="trend-kpi accent-real">
                <div class="trend-kpi-label">実現損益 (累計)</div>
                <div class="trend-kpi-value">{format_number(latest["実現損益(累計)"])}</div>
                <div class="trend-kpi-delta {cls_real}">{d_real} {lbl_real}</div>
                <div class="trend-kpi-sub">当日: {format_number(latest["実現損益"])}</div>
              </div>
              <div class="trend-kpi accent-eval">
                <div class="trend-kpi-label">評価損益</div>
                <div class="trend-kpi-value">{format_number(latest["評価損益"])}</div>
                <div class="trend-kpi-delta {cls_eval}">{d_eval} {lbl_eval}</div>
                <div class="trend-kpi-sub">{_period_info("評価損益")}</div>
              </div>
              <div class="trend-kpi accent-val">
                <div class="trend-kpi-label">ポジション評価額</div>
                <div class="trend-kpi-value">{format_number(latest["評価額(円貨)"])}</div>
                <div class="trend-kpi-delta {cls_val}">{d_val} {lbl_val}</div>
                <div class="trend-kpi-sub">銘柄数: {int(latest["件数"])}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            # ---- Plotly チャート共通レイアウト ----
            n_points = len(view_df)
            # データ点が少ないときはマーカーを大きく、線を太く
            _marker_size = 8 if n_points <= 10 else 5 if n_points <= 30 else 3
            _line_main = 3.5 if n_points <= 15 else 2.8
            _line_sub = 2 if n_points <= 15 else 1.5
            _show_markers = n_points <= 40

            _chart_layout = dict(
                template="none",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(255,255,255,0.35)",
                font=dict(family="system-ui, -apple-system, sans-serif", size=12, color="#374151"),
                margin=dict(l=12, r=12, t=8, b=48),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0,
                    font=dict(size=11, color="#6b7280"), bgcolor="rgba(0,0,0,0)",
                    itemsizing="constant",
                ),
                xaxis=dict(
                    showgrid=False, linecolor="rgba(180,83,9,0.10)",
                    tickformat="%m/%d", tickangle=-30,
                    tickfont=dict(size=11, color="#9ca3af"),
                    nticks=min(n_points, 15),
                ),
                yaxis=dict(
                    showgrid=True, gridcolor="rgba(148,163,184,0.08)", gridwidth=1,
                    zeroline=True, zerolinecolor="rgba(194,65,12,0.18)", zerolinewidth=1.5,
                    linecolor="rgba(180,83,9,0.10)",
                    tickformat=",", tickfont=dict(size=11, color="#9ca3af"),
                ),
                hovermode="x unified",
                hoverlabel=dict(bgcolor="white", font_size=12, bordercolor="#e5e7eb"),
                height=400,
            )

            # ---- 損益推移チャート (主役) ----
            _latest_pl = format_number(latest["TR損益(累計)"])
            st.markdown(
                f'<div class="trend-section-title">損益推移 (累計) &nbsp;<span style="font-size:1.1em;color:var(--ink)">TR {_latest_pl}</span></div>',
                unsafe_allow_html=True,
            )

            fig_pl = go.Figure()

            # 実現損益(累計) — エリア (薄め背景)
            fig_pl.add_trace(go.Scatter(
                x=view_df["snapshot_date"], y=view_df["実現損益(累計)"],
                name="実現損益(累計)",
                mode="lines",
                line=dict(color="rgba(2,132,199,0.5)", width=_line_sub),
                fill="tozeroy", fillcolor="rgba(2,132,199,0.06)",
                hovertemplate="%{y:,.0f}",
            ))

            # 評価損益 — エリア (日次値: ポジション含み益はその時点の値)
            fig_pl.add_trace(go.Scatter(
                x=view_df["snapshot_date"], y=view_df["評価損益"],
                name="評価損益",
                mode="lines",
                line=dict(color="rgba(5,150,105,0.5)", width=_line_sub),
                fill="tozeroy", fillcolor="rgba(5,150,105,0.06)",
                hovertemplate="%{y:,.0f}",
            ))

            # TR損益(累計) — 主役の太い折れ線
            fig_pl.add_trace(go.Scatter(
                x=view_df["snapshot_date"], y=view_df["TR損益(累計)"],
                name="TR損益(累計)",
                mode="lines+markers" if _show_markers else "lines",
                line=dict(color="#c2410c", width=_line_main),
                marker=dict(size=_marker_size, color="#c2410c", line=dict(width=1, color="white")) if _show_markers else None,
                hovertemplate="%{y:,.0f}",
            ))

            # 損益合計 — 補助線
            fig_pl.add_trace(go.Scatter(
                x=view_df["snapshot_date"], y=view_df["損益"],
                name="損益合計(日次)",
                mode="lines",
                line=dict(color="#78716c", width=1.5, dash="dash"),
                hovertemplate="%{y:,.0f}",
            ))

            fig_pl.update_layout(**_chart_layout)
            st.plotly_chart(fig_pl, use_container_width=True, config={"displayModeBar": False})

            # ---- 評価額推移チャート ----
            _latest_val = format_number(latest["評価額(円貨)"])
            _latest_cnt = int(latest["件数"])
            st.markdown(
                f'<div class="trend-section-title">ポジション評価額 &nbsp;<span style="font-size:1.1em;color:var(--ink)">{_latest_val}円</span>'
                f' &nbsp;<span style="font-size:0.85em;color:var(--muted)">({_latest_cnt}銘柄)</span></div>',
                unsafe_allow_html=True,
            )

            fig_val = go.Figure()
            fig_val.add_trace(go.Scatter(
                x=view_df["snapshot_date"], y=view_df["評価額(円貨)"],
                name="評価額",
                mode="lines+markers" if _show_markers else "lines",
                fill="tozeroy",
                line=dict(color="#7c3aed", width=_line_main),
                fillcolor="rgba(124,58,237,0.06)",
                marker=dict(size=_marker_size, color="#7c3aed", line=dict(width=1, color="white")) if _show_markers else None,
                hovertemplate="%{y:,.0f}円",
            ))

            val_layout = {**_chart_layout, "height": 300}
            fig_val.update_layout(**val_layout)
            st.plotly_chart(fig_val, use_container_width=True, config={"displayModeBar": False})

            # ---- 日次テーブル (差分付き) ----
            st.markdown('<div class="trend-section-title">日次データ</div>', unsafe_allow_html=True)

            table_df = view_df.copy()
            table_df["snapshot_date"] = table_df["snapshot_date"].dt.strftime("%Y-%m-%d")

            # 差分列を追加
            for col in ["評価損益", "評価額(円貨)", "件数"]:
                table_df[f"{col}_diff"] = view_df[col].diff()

            display_cols = ["snapshot_date", "TR損益", "TR損益(累計)", "実現損益", "実現損益(累計)", "評価損益"]
            for col in ["評価損益", "評価額(円貨)", "件数"]:
                diff_col = f"{col}_diff"
                if col not in display_cols:
                    display_cols.append(col)
                if diff_col in table_df.columns:
                    display_cols.append(diff_col)

            table_display = table_df[[c for c in display_cols if c in table_df.columns]].rename(columns={
                "snapshot_date": "日付",
                "TR損益(累計)": "TR累計",
                "実現損益(累計)": "実現累計",
                "評価損益_diff": "評価(前日比)",
                "評価額(円貨)_diff": "評価額(前日比)",
                "件数_diff": "件数(増減)",
            })
            # 最新が上
            table_display = table_display.iloc[::-1].reset_index(drop=True)
            st.dataframe(format_display_table(table_display), width="stretch", hide_index=True)

            st.download_button(
                "推移CSVダウンロード", data=to_csv_bytes(table_display),
                file_name="daily_trend.csv", mime="text/csv",
            )

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
            # 変化なしを除外した実アクション
            real_actions = actions_df[actions_df["当日アクション"] != "変化なし"]
            summary_df = build_action_summary(real_actions)

            # ---- KPI ----
            total_actions = len(real_actions)
            wins = int((real_actions["勝敗"] == "Win").sum()) if "勝敗" in real_actions.columns else 0
            losses = int((real_actions["勝敗"] == "Lose").sum()) if "勝敗" in real_actions.columns else 0
            win_rate = f"{wins / (wins + losses) * 100:.0f}%" if (wins + losses) > 0 else "-"
            action_pl_total = real_actions["アクション損益"].sum() if "アクション損益" in real_actions.columns else 0
            real_total = real_actions["実現損益差分"].sum() if "実現損益差分" in real_actions.columns else 0

            st.markdown(f"""
            <div class="trend-kpi-row">
              <div class="trend-kpi accent-pl">
                <div class="trend-kpi-label">アクション数</div>
                <div class="trend-kpi-value">{total_actions}</div>
                <div class="trend-kpi-sub">新規/増減/解消等</div>
              </div>
              <div class="trend-kpi accent-real">
                <div class="trend-kpi-label">勝敗</div>
                <div class="trend-kpi-value">{wins}W - {losses}L</div>
                <div class="trend-kpi-sub">勝率: {win_rate}</div>
              </div>
              <div class="trend-kpi accent-eval">
                <div class="trend-kpi-label">アクション損益</div>
                <div class="trend-kpi-value">{_colored(action_pl_total)}</div>
                <div class="trend-kpi-sub">新規/増減分のみ</div>
              </div>
              <div class="trend-kpi accent-val">
                <div class="trend-kpi-label">実現損益 (当日アクション分)</div>
                <div class="trend-kpi-value">{_colored(real_total)}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            # ---- アクション集計テーブル ----
            st.markdown('<div class="trend-section-title">アクション別集計</div>', unsafe_allow_html=True)
            st.dataframe(summary_df, width="stretch", hide_index=True)

            # ---- アクション一覧 ----
            st.markdown('<div class="trend-section-title">アクション詳細</div>', unsafe_allow_html=True)

            filter_options = ["すべて"] + real_actions["当日アクション"].drop_duplicates().tolist()
            focus_action = st.selectbox("絞り込み", filter_options, key="action_filter")
            filtered = real_actions if focus_action == "すべて" else real_actions[real_actions["当日アクション"] == focus_action]

            # 表示列を絞って見やすく
            display_cols = [c for c in [
                "当日アクション", "勝敗", "アクション損益", "コード", "銘柄名", "方向",
                "数量変化", "前日簿価", "当日簿価", "前日時価", "当日時価",
                "TR損益差分", "実現損益差分", "評価損益差分", "評価額差分",
            ] if c in filtered.columns]
            st.dataframe(format_display_table(filtered[display_cols]), width="stretch", hide_index=True)

            # ---- 変化なし (折りたたみ) ----
            unchanged = actions_df[actions_df["当日アクション"] == "変化なし"]
            if not unchanged.empty:
                with st.expander(f"変化なし ({len(unchanged)}件)", expanded=False):
                    unch_cols = [c for c in [
                        "コード", "銘柄名", "方向", "当日数量", "TR損益", "評価損益差分",
                    ] if c in unchanged.columns]
                    st.dataframe(format_display_table(unchanged[unch_cols]), width="stretch", hide_index=True)

            # CSV
            render_download("アクション詳細CSVダウンロード", filtered, f"actions_{selected_date}.csv")

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
            st.line_chart(timeline_df.set_index("snapshot_date")[["評価損益", "実現損益", "TR損益", "損益"]], width="stretch")
            st.line_chart(timeline_df.set_index("snapshot_date")[["ネット数量", "評価額(円貨)"]], width="stretch")
            display_timeline = timeline_df.copy()
            display_timeline["snapshot_date"] = display_timeline["snapshot_date"].dt.strftime("%Y-%m-%d")
            render_table(display_timeline, f"timeline_{selected_code}.csv", "銘柄時系列CSV")

with tab_market:
    jquants_api_key = os.getenv("JQUANTS_API_KEY", "")
    with st.expander("J-Quants API 設定", expanded=not bool(jquants_api_key)):
        api_key_input = st.text_input(
            "API Key",
            value=jquants_api_key,
            type="password",
            help="https://jpx-jquants.com/ で取得した API キーを入力してください。環境変数 JQUANTS_API_KEY でも設定できます。",
        )
        if api_key_input:
            jquants_api_key = api_key_input

    if not jquants_api_key:
        st.info("J-Quants API キーを設定するとポートフォリオ銘柄のヒストリカルベータ等を表示できます。")
    elif not snapshot_dates:
        st.info("ポジションデータがありません。先に「取込」タブからデータを保存してください。")
    else:
        # session_state にクライアントを保持してキャッシュを活かす
        if "jq_client" not in st.session_state or st.session_state.get("jq_api_key") != jquants_api_key:
            st.session_state["jq_client"] = JQuantsClient(api_key=jquants_api_key)
            st.session_state["jq_api_key"] = jquants_api_key
        jq_client = st.session_state["jq_client"]

        # --- 設定 ---
        col_date, col_period = st.columns(2)
        with col_date:
            market_date = st.selectbox("対象日", snapshot_dates, key="market_date")
        with col_period:
            beta_period_label = st.selectbox(
                "指標の算出期間",
                ["3ヶ月", "6ヶ月", "1年", "2年"],
                index=2,
                key="beta_period",
            )
        beta_days = {"3ヶ月": 90, "6ヶ月": 180, "1年": 365, "2年": 730}[beta_period_label]

        market_snapshot = load_snapshot(market_date)

        # --- セクター情報付与 & ウェイト計算 ---
        with st.spinner("銘柄マスタ・ウェイト計算中..."):
            enriched = enrich_portfolio_with_market_info(market_snapshot, jq_client)
            weighted = compute_portfolio_weights(enriched)

        # ================================================================
        # ポートフォリオ全体ビュー
        # ================================================================
        st.markdown("## ポートフォリオ全体")

        if not weighted.empty:
            # ポジション金額 (API 不要) — 株式 / 先物を分離
            w = weighted.copy()
            w["_is_eq"] = w.apply(lambda r: is_equity_code(r["code"], r.get("product_type")), axis=1)
            mv = pd.to_numeric(w["position_market_value_jpy"], errors="coerce").fillna(0)
            bv = pd.to_numeric(w["book_value_net"], errors="coerce").fillna(0)

            # 全体 (時価)
            gross_actual = mv.abs().sum()
            net_actual = mv.sum()
            long_mv = mv[mv > 0].sum()
            short_mv = mv[mv < 0].sum()
            long_count = int((mv > 0).sum())
            short_count = int((mv < 0).sum())

            # 全体 (簿価)
            bv_gross = bv.abs().sum()
            bv_net = bv.sum()
            bv_long = bv[bv > 0].sum()
            bv_short = bv[bv < 0].sum()

            # 株式のみ (時価)
            eq_mask = w["_is_eq"]
            eq_mv = mv[eq_mask]
            eq_gross = eq_mv.abs().sum()
            eq_net = eq_mv.sum()
            eq_long = eq_mv[eq_mv > 0].sum()
            eq_short = eq_mv[eq_mv < 0].sum()
            eq_long_cnt = int((eq_mv > 0).sum())
            eq_short_cnt = int((eq_mv < 0).sum())

            # 株式のみ (簿価)
            eq_bv = bv[eq_mask]
            eq_bv_gross = eq_bv.abs().sum()
            eq_bv_net = eq_bv.sum()

            # 先物のみ
            fut_mv = mv[~eq_mask]
            fut_gross = fut_mv.abs().sum()
            fut_net = fut_mv.sum()
            fut_count = int((~eq_mask).sum())
            fut_bv = bv[~eq_mask]
            fut_bv_gross = fut_bv.abs().sum()
            fut_bv_net = fut_bv.sum()

            with st.spinner(f"全銘柄のベータ・指標を取得中 (TOPIX & 日経平均 / {beta_period_label})..."):
                try:
                    pr = compute_portfolio_all(jq_client, weighted, days=beta_days)
                except Exception as e:
                    pr = {}
                    st.error(f"ポートフォリオ指標取得エラー: {e}")

            # ---- ポジション概要テーブル ----
            _fut_long = fut_mv[fut_mv > 0].sum() if len(fut_mv[fut_mv > 0]) else 0
            _fut_short = fut_mv[fut_mv < 0].sum() if len(fut_mv[fut_mv < 0]) else 0

            st.markdown(f"""
            <table class="dash-table">
              <caption>ポジション概要 (時価ベース)</caption>
              <tr>
                <th></th><th>ロング</th><th>ショート</th><th>ネット</th><th>グロス</th><th>銘柄数</th>
              </tr>
              <tr>
                <td>株式</td>
                <td>{_colored(eq_long, "円")}</td>
                <td>{_colored(eq_short, "円")}</td>
                <td>{_colored(eq_net, "円")}</td>
                <td>{format_number(eq_gross)}円</td>
                <td>{eq_long_cnt + eq_short_cnt} (L{eq_long_cnt} / S{eq_short_cnt})</td>
              </tr>
              <tr>
                <td>先物</td>
                <td>{_colored(_fut_long, "円")}</td>
                <td>{_colored(_fut_short, "円")}</td>
                <td>{_colored(fut_net, "円")}</td>
                <td>{format_number(fut_gross)}円</td>
                <td>{fut_count}</td>
              </tr>
              <tr style="font-weight:800; border-top:2px solid rgba(180,83,9,0.2)">
                <td>合計</td>
                <td>{_colored(long_mv, "円")}</td>
                <td>{_colored(short_mv, "円")}</td>
                <td>{_colored(net_actual, "円")}</td>
                <td>{format_number(gross_actual)}円</td>
                <td>{long_count + short_count}</td>
              </tr>
            </table>
            """, unsafe_allow_html=True)

            st.markdown(f"""
            <table class="dash-table">
              <caption>ポジション概要 (簿価ベース)</caption>
              <tr>
                <th></th><th>ロング</th><th>ショート</th><th>ネット</th><th>グロス</th>
              </tr>
              <tr>
                <td>株式</td>
                <td>{_colored(eq_bv[eq_bv > 0].sum() if len(eq_bv[eq_bv > 0]) else 0, "円")}</td>
                <td>{_colored(eq_bv[eq_bv < 0].sum() if len(eq_bv[eq_bv < 0]) else 0, "円")}</td>
                <td>{_colored(eq_bv_net, "円")}</td>
                <td>{format_number(eq_bv_gross)}円</td>
              </tr>
              <tr>
                <td>先物</td>
                <td>{_colored(fut_bv[fut_bv > 0].sum() if len(fut_bv[fut_bv > 0]) else 0, "円")}</td>
                <td>{_colored(fut_bv[fut_bv < 0].sum() if len(fut_bv[fut_bv < 0]) else 0, "円")}</td>
                <td>{_colored(fut_bv_net, "円")}</td>
                <td>{format_number(fut_bv_gross)}円</td>
              </tr>
              <tr style="font-weight:800; border-top:2px solid rgba(180,83,9,0.2)">
                <td>合計</td>
                <td>{_colored(bv_long, "円")}</td>
                <td>{_colored(bv_short, "円")}</td>
                <td>{_colored(bv_net, "円")}</td>
                <td>{format_number(bv_gross)}円</td>
              </tr>
            </table>
            """, unsafe_allow_html=True)

            # ---- 銘柄情報 (時価総額・バリュエーション・出来高) ----
            unique_codes = weighted["code"].unique().tolist()
            with st.spinner("yfinance から銘柄情報を取得中..."):
                stock_info_df = fetch_portfolio_stock_info(jq_client, unique_codes)
            if not stock_info_df.empty:
                if "時価総額" in stock_info_df.columns:
                    stock_info_df["時価総額(億円)"] = stock_info_df["時価総額"].apply(
                        lambda x: f"{x / 1e8:,.0f}" if x else "-"
                    )
                display_cols = [c for c in [
                    "コード", "時価総額(億円)", "株価", "β(Yahoo)",
                    "PER(実績)", "PER(予想)", "PBR", "配当利回り(%)",
                    "出来高", "平均出来高(3M)", "出来高倍率",
                    "機関投資家保有率(%)", "内部者保有率(%)",
                    "52週高値", "52週安値",
                ] if c in stock_info_df.columns]
                st.markdown("### 銘柄情報")
                st.dataframe(stock_info_df[display_cols], width="stretch", hide_index=True)

            # ---- ベータ & リスク指標テーブル ----
            if pr:
                st.markdown(f"""
                <table class="dash-table">
                  <caption>加重ベータ (3M / 6M / 12M)</caption>
                  <tr>
                    <th></th><th colspan="3">先物込み</th><th colspan="3">株式のみ</th><th>L/S ({beta_period_label})</th>
                  </tr>
                  <tr>
                    <th></th><th>3M</th><th>6M</th><th>12M</th><th>3M</th><th>6M</th><th>12M</th><th></th>
                  </tr>
                  <tr>
                    <td>β (TOPIX)</td>
                    <td><b>{_v(pr.get('topix_beta_3M'))}</b></td>
                    <td><b>{_v(pr.get('topix_beta_6M'))}</b></td>
                    <td><b>{_v(pr.get('topix_beta_12M'))}</b></td>
                    <td>{_v(pr.get('topix_beta_3M_eq'))}</td>
                    <td>{_v(pr.get('topix_beta_6M_eq'))}</td>
                    <td>{_v(pr.get('topix_beta_12M_eq'))}</td>
                    <td class="val-muted">L {_v(pr.get('topix_long_beta'))} / S {_v(pr.get('topix_short_beta'))}</td>
                  </tr>
                  <tr>
                    <td>β (日経平均)</td>
                    <td><b>{_v(pr.get('nikkei_beta_3M'))}</b></td>
                    <td><b>{_v(pr.get('nikkei_beta_6M'))}</b></td>
                    <td><b>{_v(pr.get('nikkei_beta_12M'))}</b></td>
                    <td>{_v(pr.get('nikkei_beta_3M_eq'))}</td>
                    <td>{_v(pr.get('nikkei_beta_6M_eq'))}</td>
                    <td>{_v(pr.get('nikkei_beta_12M_eq'))}</td>
                    <td class="val-muted">L {_v(pr.get('nikkei_long_beta'))} / S {_v(pr.get('nikkei_short_beta'))}</td>
                  </tr>
                </table>
                """, unsafe_allow_html=True)

                st.markdown(f"""
                <div class="dash-kpi-grid">
                  <div class="dash-kpi">
                    <div class="dash-kpi-label">ボラティリティ (年率)</div>
                    <div class="dash-kpi-value">{_v(pr.get('weighted_vol'), '%')}</div>
                  </div>
                  <div class="dash-kpi">
                    <div class="dash-kpi-label">シャープレシオ</div>
                    <div class="dash-kpi-value">{_v(pr.get('weighted_sharpe'))}</div>
                  </div>
                  <div class="dash-kpi">
                    <div class="dash-kpi-label">加重リターン</div>
                    <div class="dash-kpi-value">{_v(pr.get('weighted_return'), '%')}</div>
                  </div>
                  <div class="dash-kpi">
                    <div class="dash-kpi-label">上位3銘柄集中度</div>
                    <div class="dash-kpi-value">{_v(pr.get('concentration_top3'), '%')}</div>
                  </div>
                  <div class="dash-kpi">
                    <div class="dash-kpi-label">ベスト銘柄</div>
                    <div class="dash-kpi-value" style="font-size:0.85rem">{pr.get('best_stock', '-')}</div>
                  </div>
                  <div class="dash-kpi">
                    <div class="dash-kpi-label">ワースト銘柄</div>
                    <div class="dash-kpi-value" style="font-size:0.85rem">{pr.get('worst_stock', '-')}</div>
                  </div>
                </div>
                """, unsafe_allow_html=True)

                st.caption(f"算出期間: {beta_period_label} / 銘柄数: {pr.get('stock_count', 0)} / ベータはTOPIX・日経平均の両方で算出")

            # ---- リスク枠消化状況 ----
            current_month = market_date[:7]
            rl = load_latest_risk_limits(current_month)

            def _usage(actual: float, limit: float | None) -> str:
                if limit is None or limit == 0:
                    return "-"
                return f"{actual / limit * 100:.1f}%"

            if rl:
                g_lim = rl["gross_limit"] or 0
                n_lim = rl["net_limit"] or 0
                f_lim = rl["futures_limit"] or 0
                l_lim = rl["monthly_loss_limit"] or 0

                st.markdown(f"""
                <table class="dash-table">
                  <caption>リスク枠消化状況 ({rl['month']})</caption>
                  <tr>
                    <th></th><th>上限</th><th>実績</th><th>消化率</th><th>残余</th>
                  </tr>
                  <tr>
                    <td>グロス金額</td>
                    <td>{format_number(g_lim)}円</td>
                    <td>{format_number(gross_actual)}円</td>
                    <td>{_usage(gross_actual, rl['gross_limit'])}</td>
                    <td>{format_number(g_lim - gross_actual)}円</td>
                  </tr>
                  <tr>
                    <td>ネット上限</td>
                    <td>{format_number(n_lim)}円</td>
                    <td>{format_number(abs(net_actual))}円</td>
                    <td>{_usage(abs(net_actual), rl['net_limit'])}</td>
                    <td>{format_number(n_lim - abs(net_actual))}円</td>
                  </tr>
                  <tr>
                    <td>先物枠</td>
                    <td>{format_number(f_lim)}円</td>
                    <td>{format_number(fut_gross)}円</td>
                    <td>{_usage(fut_gross, rl['futures_limit'])}</td>
                    <td>{format_number(f_lim - fut_gross)}円</td>
                  </tr>
                  <tr>
                    <td>月間損失限度</td>
                    <td>{format_number(l_lim)}円</td>
                    <td class="val-muted">-</td>
                    <td class="val-muted">-</td>
                    <td class="val-muted">-</td>
                  </tr>
                </table>
                """, unsafe_allow_html=True)

                for label, actual_val, limit_val in [
                    ("グロス消化率", gross_actual, rl["gross_limit"]),
                    ("ネット消化率", abs(net_actual), rl["net_limit"]),
                    ("先物枠消化率", fut_gross, rl["futures_limit"]),
                ]:
                    if limit_val and limit_val > 0:
                        ratio = min(actual_val / limit_val, 1.0)
                        st.progress(ratio, text=f"{label}: {format_number(actual_val)} / {format_number(limit_val)} ({ratio * 100:.1f}%)")

            # ---- コピー用テキスト ----
            copy_lines = [
                f"ポートフォリオ概要 ({market_date} / {beta_period_label})",
                "",
                f"[株式・時価] L: {format_number(eq_long)}円({eq_long_cnt}) / S: {format_number(eq_short)}円({eq_short_cnt}) / Net: {format_number(eq_net)}円 / Gross: {format_number(eq_gross)}円",
                f"[株式・簿価] Net: {format_number(eq_bv_net)}円 / Gross: {format_number(eq_bv_gross)}円",
                f"[先物] Net: {format_number(fut_net)}円 / Gross: {format_number(fut_gross)}円 ({fut_count}銘柄)",
                f"[合計・時価] L: {format_number(long_mv)}円 / S: {format_number(short_mv)}円 / Net: {format_number(net_actual)}円 / Gross: {format_number(gross_actual)}円",
                f"[合計・簿価] Net: {format_number(bv_net)}円 / Gross: {format_number(bv_gross)}円",
            ]
            if pr:
                copy_lines += [
                    "",
                    f"β(TOPIX) 先物込: 3M {pr.get('topix_beta_3M', '-')} / 6M {pr.get('topix_beta_6M', '-')} / 12M {pr.get('topix_beta_12M', '-')}  株のみ: 3M {pr.get('topix_beta_3M_eq', '-')} / 6M {pr.get('topix_beta_6M_eq', '-')} / 12M {pr.get('topix_beta_12M_eq', '-')}",
                    f"β(日経)  先物込: 3M {pr.get('nikkei_beta_3M', '-')} / 6M {pr.get('nikkei_beta_6M', '-')} / 12M {pr.get('nikkei_beta_12M', '-')}  株のみ: 3M {pr.get('nikkei_beta_3M_eq', '-')} / 6M {pr.get('nikkei_beta_6M_eq', '-')} / 12M {pr.get('nikkei_beta_12M_eq', '-')}",
                    f"ボラティリティ(年率): {pr.get('weighted_vol', '-')}%  シャープ: {pr.get('weighted_sharpe', '-')}  リターン: {pr.get('weighted_return', '-')}%",
                ]
            if rl:
                copy_lines += [
                    "",
                    f"--- リスク枠 ({rl['month']}) ---",
                    f"グロス: {format_number(gross_actual)} / {format_number(g_lim)}円 ({_usage(gross_actual, rl['gross_limit'])})",
                    f"ネット: {format_number(abs(net_actual))} / {format_number(n_lim)}円 ({_usage(abs(net_actual), rl['net_limit'])})",
                    f"先物枠: {format_number(fut_gross)} / {format_number(f_lim)}円 ({_usage(fut_gross, rl['futures_limit'])})",
                    f"損失限度: {format_number(l_lim)}円",
                ]
            copy_text = "\n".join(copy_lines)
            st.text_area("コピー用サマリ", value=copy_text, height=280, key="portfolio_copy")

            # --- セクター構成 ---
            sector_df = compute_sector_breakdown(weighted)
            if not sector_df.empty:
                st.markdown("### セクター構成")
                left_sec, right_sec = st.columns([2, 3])
                with left_sec:
                    st.dataframe(sector_df, width="stretch", hide_index=True)
                with right_sec:
                    chart_data = sector_df.set_index("セクター")[["ウェイト"]]
                    st.bar_chart(chart_data, width="stretch")

            # --- 銘柄一覧 (統合テーブル) ---
            st.markdown("### 銘柄一覧")

            # ベース: ウェイト
            unified = weighted[["code", "name", "direction", "net_qty", "position_market_value_jpy", "book_value_net", "weight_pct"]].copy()
            unified = unified.rename(columns={
                "code": "コード", "name": "銘柄名", "direction": "方向",
                "net_qty": "数量", "position_market_value_jpy": "評価額(円)",
                "book_value_net": "簿価(円)", "weight_pct": "ウェイト(%)",
            })

            # 時価総額・バリュエーション (stock_info_df は上で取得済み)
            if not stock_info_df.empty:
                info_cols = stock_info_df[["コード"]].copy()
                if "時価総額(億円)" in stock_info_df.columns:
                    info_cols["時価総額(億円)"] = stock_info_df["時価総額(億円)"]
                for c in ["PER(予想)", "PBR", "配当利回り(%)", "出来高倍率"]:
                    if c in stock_info_df.columns:
                        info_cols[c] = stock_info_df[c]
                unified = unified.merge(info_cols, on="コード", how="left")

            # product_type マップ (code → product_type)
            _pt_map = dict(zip(weighted["code"], weighted.get("product_type", pd.Series(dtype=str))))

            # 騰落率
            with st.spinner("全銘柄の騰落率を計算中..."):
                chg_rows = []
                for code in weighted["code"].unique():
                    if not is_equity_code(code, _pt_map.get(code)):
                        continue
                    chg = compute_price_changes(jq_client, code)
                    if chg:
                        chg_rows.append({
                            "コード": code[:4] if len(code) == 5 and code.isdigit() else code,
                            "前日比(%)": chg.get("前日比"),
                            "1W(%)": chg.get("1W"),
                            "1M(%)": chg.get("1M"),
                            "3M(%)": chg.get("3M"),
                            "YTD(%)": chg.get("YTD"),
                        })
                if chg_rows:
                    chg_df = pd.DataFrame(chg_rows)
                    unified = unified.merge(chg_df, on="コード", how="left")

            # 需給 (貸借倍率のみ)
            with st.spinner("全銘柄の信用残を取得中..."):
                margin_rows = []
                for code in weighted["code"].unique():
                    if not is_equity_code(code, _pt_map.get(code)):
                        continue
                    try:
                        mdf = jq_client.get_margin_balance(code, weeks=4)
                        if not mdf.empty:
                            lat = mdf.iloc[-1]
                            margin_rows.append({
                                "コード": code[:4] if len(code) == 5 and code.isdigit() else code,
                                "貸借倍率": lat.get("貸借倍率"),
                                "買残増減(%)": lat.get("買残増減率(%)"),
                            })
                    except Exception:
                        pass
                if margin_rows:
                    margin_summary = pd.DataFrame(margin_rows)
                    unified = unified.merge(margin_summary, on="コード", how="left")

            # ベータ (stock_metrics から主要列をマージ)
            if pr and "stock_metrics" in pr:
                sm = pr["stock_metrics"]
                beta_cols = ["コード"]
                for c in ["β(T3M)", "β(T6M)", "β(T12M)", "β(N3M)", "β(N6M)", "β(N12M)",
                           "ボラティリティ(年率%)", "シャープレシオ", "期間リターン(%)", "最大DD(%)"]:
                    if c in sm.columns:
                        beta_cols.append(c)
                unified = unified.merge(sm[beta_cols], on="コード", how="left")

            st.dataframe(unified, width="stretch", hide_index=True)

            csv_bytes = unified.to_csv(index=False).encode("utf-8-sig")
            col_dl, col_cp = st.columns(2)
            with col_dl:
                st.download_button(
                    "銘柄一覧CSVダウンロード",
                    data=csv_bytes,
                    file_name=f"portfolio_all_{market_date}.csv",
                    mime="text/csv",
                )
            with col_cp:
                tsv_text = unified.to_csv(index=False, sep="\t")
                st.text_area("銘柄一覧 (コピー用TSV)", value=tsv_text, height=200, key="metrics_copy")

        # ================================================================
        # 個別銘柄ドリルダウン
        # ================================================================
        st.markdown("---")
        st.markdown("## 個別銘柄ドリルダウン")

        stock_codes = (
            market_snapshot[["code", "name"]]
            .drop_duplicates()
            .sort_values(["code", "name"], kind="stable")
            .assign(label=lambda x: x["code"] + " " + x["name"])
        )

        selected_label = st.selectbox("銘柄", stock_codes["label"].tolist(), key="market_code")
        selected_code = selected_label.split(" ", 1)[0]
        selected_name = selected_label.split(" ", 1)[1] if " " in selected_label else ""

        st.markdown(f"### {selected_code} {selected_name}")

        # ---- 銘柄情報 (yfinance) ----
        with st.spinner("銘柄情報を取得中..."):
            si = jq_client.get_stock_info(selected_code)

        if si:
            mcap = si.get("時価総額")
            mcap_str = f"{mcap / 1e8:,.0f}億円" if mcap else "-"

            st.markdown(f"""
            <table class="dash-table">
              <caption>銘柄情報</caption>
              <tr><th>時価総額</th><th>株価</th><th>β(Yahoo)</th><th>PER(実績)</th><th>PER(予想)</th><th>PBR</th><th>配当利回り</th></tr>
              <tr>
                <td>{mcap_str}</td>
                <td>{format_number(si.get('株価', 0))}円</td>
                <td>{_v(si.get('β(Yahoo)'))}</td>
                <td>{_v(si.get('PER(実績)'))}</td>
                <td>{_v(si.get('PER(予想)'))}</td>
                <td>{_v(si.get('PBR'))}</td>
                <td>{_v(si.get('配当利回り(%)'), '%')}</td>
              </tr>
            </table>
            """, unsafe_allow_html=True)

            st.markdown(f"""
            <table class="dash-table">
              <caption>出来高・保有構造</caption>
              <tr><th>出来高</th><th>平均出来高(3M)</th><th>出来高倍率</th><th>機関投資家</th><th>内部者</th><th>52週高値</th><th>52週安値</th></tr>
              <tr>
                <td>{format_number(si.get('出来高', 0))}</td>
                <td>{format_number(si.get('平均出来高(3M)', 0))}</td>
                <td>{_v(si.get('出来高倍率'), 'x')}</td>
                <td>{_v(si.get('機関投資家保有率(%)'), '%')}</td>
                <td>{_v(si.get('内部者保有率(%)'), '%')}</td>
                <td>{format_number(si.get('52週高値', 0))}円</td>
                <td>{format_number(si.get('52週安値', 0))}円</td>
              </tr>
            </table>
            """, unsafe_allow_html=True)

        # ---- 直近騰落率 ----
        with st.spinner("騰落率を計算中..."):
            price_chg = compute_price_changes(jq_client, selected_code)

        if price_chg and len(price_chg) > 1:
            current = price_chg.get("現在値", 0)
            headers = ""
            values = ""
            for k in ["前日比", "1W", "1M", "3M", "6M", "YTD", "1Y"]:
                v = price_chg.get(k)
                if v is not None:
                    headers += f"<th>{k}</th>"
                    cls = "val-pos" if v > 0 else "val-neg" if v < 0 else ""
                    sign = "+" if v > 0 else ""
                    values += f'<td class="{cls}">{sign}{v}%</td>'

            st.markdown(f"""
            <table class="dash-table">
              <caption>直近騰落率 (現在値: {format_number(current)}円)</caption>
              <tr>{headers}</tr>
              <tr>{values}</tr>
            </table>
            """, unsafe_allow_html=True)

        # ---- 信用取引残高 (J-Quants) ----
        with st.spinner("信用残データを取得中..."):
            try:
                margin_df = jq_client.get_margin_balance(selected_code)
            except Exception as e:
                margin_df = pd.DataFrame()
                st.warning(f"信用残取得エラー: {e}")

        if not margin_df.empty:
            st.markdown("#### 信用取引残高 (週次推移)")
            # 最新の数値をハイライト
            latest = margin_df.iloc[-1]
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("買残", format_number(latest.get("買残", 0)))
            m2.metric("売残", format_number(latest.get("売残", 0)))
            m3.metric("貸借倍率", f"{latest.get('貸借倍率', '-')}x")
            buy_chg = latest.get("買残増減率(%)")
            m4.metric("買残増減率", f"{buy_chg}%" if buy_chg is not None and not pd.isna(buy_chg) else "-")

            # チャート
            if len(margin_df) > 1:
                chart_margin = margin_df[["日付", "買残", "売残"]].set_index("日付")
                st.line_chart(chart_margin, width="stretch")

                if "貸借倍率" in margin_df.columns:
                    ratio_chart = margin_df[["日付", "貸借倍率"]].dropna().set_index("日付")
                    st.line_chart(ratio_chart, width="stretch")

            # テーブル
            display_margin = margin_df.copy()
            display_margin["日付"] = display_margin["日付"].dt.strftime("%Y-%m-%d")
            st.dataframe(display_margin, width="stretch", hide_index=True)

        # 複数期間指標 (TOPIX & 日経平均 両方)
        with st.spinner("期間別指標を計算中 (TOPIX & 日経平均)..."):
            metrics_topix = pd.DataFrame()
            metrics_nikkei = pd.DataFrame()
            try:
                metrics_topix = compute_multi_period_metrics(jq_client, selected_code, benchmark="TOPIX")
            except Exception as e:
                st.error(f"TOPIX指標エラー: {e}")
            try:
                metrics_nikkei = compute_multi_period_metrics(jq_client, selected_code, benchmark="日経平均")
            except Exception as e:
                st.error(f"日経平均指標エラー: {e}")

        # ハイライト (TOPIX 1Y or最長)
        if not metrics_topix.empty:
            hl_period = "1Y" if "1Y" in metrics_topix.index else metrics_topix.index[-1]
            ht = metrics_topix.loc[hl_period]
            hn = metrics_nikkei.loc[hl_period] if not metrics_nikkei.empty and hl_period in metrics_nikkei.index else {}

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("β (TOPIX)", f"{ht.get('ベータ', '-')}")
            c2.metric("β (日経)", f"{hn.get('ベータ', '-') if hn else '-'}" if isinstance(hn, dict) else f"{hn.get('ベータ', '-')}" if hasattr(hn, 'get') else "-")
            c3.metric("ボラティリティ", f"{ht.get('ボラティリティ(年率%)', '-')}%")
            c4.metric("シャープレシオ", f"{ht.get('シャープレシオ', '-')}")
            c5.metric("期間リターン", f"{ht.get('期間リターン(%)', '-')}%")
            c6.metric("最大DD", f"{ht.get('最大ドローダウン(%)', '-')}%")
            st.caption(f"上記は {hl_period} の値")

        # 対TOPIX テーブル
        if not metrics_topix.empty:
            st.markdown("#### 期間別リスク指標 (対TOPIX)")
            st.dataframe(metrics_topix, width="stretch")
        # 対日経平均 テーブル
        if not metrics_nikkei.empty:
            st.markdown("#### 期間別リスク指標 (対日経平均)")
            st.dataframe(metrics_nikkei, width="stretch")

        # CSV ダウンロード (両方結合)
        if not metrics_topix.empty or not metrics_nikkei.empty:
            dfs_to_merge = []
            if not metrics_topix.empty:
                t = metrics_topix.reset_index().copy()
                t.columns = [f"{c}(TOPIX)" if c != "期間" else c for c in t.columns]
                dfs_to_merge.append(t)
            if not metrics_nikkei.empty:
                n = metrics_nikkei.reset_index().copy()
                n.columns = [f"{c}(日経)" if c != "期間" else c for c in n.columns]
                dfs_to_merge.append(n)
            if len(dfs_to_merge) == 2:
                combined = dfs_to_merge[0].merge(dfs_to_merge[1], on="期間", how="outer")
            else:
                combined = dfs_to_merge[0]
            st.download_button(
                "指標CSVダウンロード",
                data=combined.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"metrics_{selected_code}.csv",
                mime="text/csv",
            )

        # 株価 vs TOPIX & 日経平均
        chart_period = st.selectbox(
            "チャート期間", ["6ヶ月", "1年", "2年"], index=1, key="chart_period"
        )
        chart_days = {"6ヶ月": 180, "1年": 365, "2年": 730}[chart_period]

        with st.spinner("チャートデータ取得中..."):
            try:
                chart_topix = compute_price_chart_data(jq_client, selected_code, days=chart_days, benchmark="TOPIX")
                chart_nikkei = compute_price_chart_data(jq_client, selected_code, days=chart_days, benchmark="日経平均")
            except Exception as e:
                chart_topix = pd.DataFrame()
                chart_nikkei = pd.DataFrame()
                st.error(f"チャートデータ取得エラー: {e}")

        # 両方を結合して1つのチャートに
        if not chart_topix.empty:
            merged_chart = chart_topix.rename(columns={"stock": selected_code})
            if not chart_nikkei.empty and "日経平均株価" in chart_nikkei.columns:
                nk_data = chart_nikkei[["date", "日経平均株価"]]
                merged_chart = merged_chart.merge(nk_data, on="date", how="left")
            st.markdown(f"#### 株価 vs TOPIX vs 日経平均 (正規化: 開始日=100)")
            chart_cols = [c for c in merged_chart.columns if c != "date"]
            st.line_chart(merged_chart.set_index("date")[chart_cols], width="stretch")

        # ローリングベータ (TOPIX & 日経平均)
        rolling_window = st.slider("ローリングベータ 窓幅(営業日)", 20, 120, 60, key="rolling_window")
        with st.spinner("ローリングベータ計算中..."):
            try:
                roll_topix = compute_rolling_beta(jq_client, selected_code, window=rolling_window, days=chart_days + 200, benchmark="TOPIX")
                roll_nikkei = compute_rolling_beta(jq_client, selected_code, window=rolling_window, days=chart_days + 200, benchmark="日経平均")
            except Exception as e:
                roll_topix = pd.DataFrame()
                roll_nikkei = pd.DataFrame()
                st.error(f"ローリングベータ取得エラー: {e}")

        if not roll_topix.empty or not roll_nikkei.empty:
            st.markdown(f"#### ローリングベータ ({rolling_window}日窓)")
            merged_roll = pd.DataFrame()
            if not roll_topix.empty:
                merged_roll = roll_topix.rename(columns={"rolling_beta": "β(TOPIX)"})
            if not roll_nikkei.empty:
                rn = roll_nikkei.rename(columns={"rolling_beta": "β(日経平均)"})
                if merged_roll.empty:
                    merged_roll = rn
                else:
                    merged_roll = merged_roll.merge(rn, on="date", how="outer").sort_values("date")
            if not merged_roll.empty:
                beta_cols = [c for c in merged_roll.columns if c != "date"]
                st.line_chart(merged_roll.set_index("date")[beta_cols], width="stretch")

with tab_limits:
    st.markdown('<div class="section-note">毎月の会社設定リスク枠を登録・管理します。マーケット指標タブで消化率が表示されます。</div>', unsafe_allow_html=True)

    col_input, col_history = st.columns([3, 2])

    with col_input:
        st.markdown("### リスク枠 登録・更新")
        from datetime import datetime as _dt
        default_month = _dt.now().strftime("%Y-%m")
        limit_month = st.text_input("対象月 (YYYY-MM)", value=default_month, key="limit_month")

        # 既存データがあれば初期値に
        existing = load_risk_limits(limit_month) if len(limit_month) == 7 else None

        gross_val = st.number_input(
            "グロス金額上限 (円)",
            min_value=0,
            value=int(existing["gross_limit"] or 0) if existing and existing["gross_limit"] else 0,
            step=10_000_000,
            help="1千万円単位で増減",
            key="limit_gross",
        )
        if gross_val > 0:
            st.caption(f"= {gross_val:,} 円")
        net_val = st.number_input(
            "ネット上限 (円)",
            min_value=0,
            value=int(existing["net_limit"] or 0) if existing and existing["net_limit"] else 0,
            step=10_000_000,
            help="1千万円単位で増減",
            key="limit_net",
        )
        if net_val > 0:
            st.caption(f"= {net_val:,} 円")
        futures_val = st.number_input(
            "先物枠 (円)",
            min_value=0,
            value=int(existing["futures_limit"] or 0) if existing and existing["futures_limit"] else 0,
            step=10_000_000,
            help="1千万円単位で増減",
            key="limit_futures",
        )
        if futures_val > 0:
            st.caption(f"= {futures_val:,} 円")
        loss_val = st.number_input(
            "月間損失限度額 (円)",
            min_value=0,
            value=int(existing["monthly_loss_limit"] or 0) if existing and existing["monthly_loss_limit"] else 0,
            step=1_000_000,
            help="100万円単位で増減",
            key="limit_loss",
        )
        if loss_val > 0:
            st.caption(f"= {loss_val:,} 円")
        limit_note = st.text_input("メモ", value=existing["note"] or "" if existing else "", key="limit_note")

        if st.button("保存する", type="primary", key="save_limits"):
            if len(limit_month) != 7 or limit_month[4] != "-":
                st.error("対象月は YYYY-MM 形式で入力してください。")
            else:
                save_risk_limits(
                    month=limit_month,
                    gross_limit=gross_val if gross_val > 0 else None,
                    net_limit=net_val if net_val > 0 else None,
                    futures_limit=futures_val if futures_val > 0 else None,
                    monthly_loss_limit=loss_val if loss_val > 0 else None,
                    note=limit_note,
                )
                st.success(f"{limit_month} のリスク枠を保存しました。")

    with col_history:
        st.markdown("### 登録済みリスク枠")
        limit_months = list_risk_limit_months()
        if not limit_months:
            st.info("まだリスク枠が登録されていません。")
        else:
            history_rows = []
            for m in limit_months:
                rl = load_risk_limits(m)
                if rl:
                    history_rows.append({
                        "対象月": rl["month"],
                        "グロス上限": format_number(rl["gross_limit"] or 0),
                        "ネット上限": format_number(rl["net_limit"] or 0),
                        "先物枠": format_number(rl["futures_limit"] or 0),
                        "損失限度": format_number(rl["monthly_loss_limit"] or 0),
                        "メモ": rl["note"] or "",
                        "更新日時": rl["updated_at"],
                    })
            if history_rows:
                st.dataframe(pd.DataFrame(history_rows), width="stretch", hide_index=True)

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
            c4.metric("月末評価額", format_number(month_daily["評価額(円貨)"].iloc[-1], "評価額(円貨)"))

            st.line_chart(month_daily.set_index("snapshot_date")[["TR損益", "実現損益", "評価損益", "損益"]], width="stretch")

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
