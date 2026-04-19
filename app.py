from __future__ import annotations

import html
import os
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pandas.api.types import is_numeric_dtype

from portfolio_app.analytics import (
    build_action_summary,
    build_daily_exposure,
    build_daily_trend,
    build_daily_trend_by_direction,
    build_intraday_roundtrips,
    build_overnight_hold_profile,
    build_roundtrip_profile,
    build_trade_habit_profile,
    build_trade_session_summary,
    build_instrument_timeline,
    build_instrument_timeline_by_direction,
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
    list_trade_dates,
    load_all_snapshots,
    load_all_trades,
    load_instrument_history,
    load_latest_risk_limits,
    load_previous_snapshot,
    load_risk_limits,
    load_snapshot,
    load_snapshots_by_month,
    load_trades_by_date,
    replace_snapshot,
    replace_trade_executions,
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
from portfolio_app.parser import parse_positions, parse_trade_tsv, split_blocks

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

.trade-diagnostic-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.9rem;
  margin: 0.4rem 0 1.2rem;
}
.trade-diagnostic-card {
  background: linear-gradient(180deg, rgba(255,255,255,0.97), rgba(255,250,243,0.92));
  border: 1px solid rgba(180,83,9,0.10);
  border-radius: 20px;
  padding: 1rem 1.05rem;
  box-shadow: 0 10px 28px rgba(120,53,15,0.06);
}
.trade-diagnostic-card.good { border-color: rgba(5,150,105,0.20); }
.trade-diagnostic-card.warn { border-color: rgba(217,119,6,0.20); }
.trade-diagnostic-card.bad { border-color: rgba(220,38,38,0.18); }
.trade-diagnostic-card.neutral { border-color: rgba(148,163,184,0.18); }
.trade-diagnostic-pill {
  display: inline-flex;
  align-items: center;
  padding: 0.18rem 0.55rem;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.04em;
  margin-bottom: 0.55rem;
}
.trade-diagnostic-pill.good { color: #166534; background: rgba(5,150,105,0.12); }
.trade-diagnostic-pill.warn { color: #b45309; background: rgba(217,119,6,0.12); }
.trade-diagnostic-pill.bad { color: #b91c1c; background: rgba(220,38,38,0.10); }
.trade-diagnostic-pill.neutral { color: #475569; background: rgba(148,163,184,0.12); }
.trade-diagnostic-title {
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  font-weight: 700;
}
.trade-diagnostic-main {
  font-size: 1.38rem;
  font-weight: 800;
  color: var(--accent-deep);
  line-height: 1.15;
  margin-top: 0.3rem;
}
.trade-diagnostic-sub {
  font-size: 0.82rem;
  color: #57534e;
  line-height: 1.55;
  margin-top: 0.4rem;
}
.trade-diagnostic-meta {
  font-size: 0.74rem;
  color: var(--muted);
  margin-top: 0.55rem;
}
.trade-context-note {
  padding: 0.75rem 0.9rem;
  border-radius: 16px;
  background: rgba(255,255,255,0.68);
  border: 1px solid rgba(180,83,9,0.12);
  color: #57534e;
  font-size: 0.84rem;
  line-height: 1.55;
  margin-bottom: 1rem;
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


def _escape_html(value: object) -> str:
    return html.escape("" if value is None else str(value))


def render_trade_diagnostic_cards(cards: list[dict[str, str]]) -> None:
    if not cards:
        return
    html_cards = []
    for card in cards:
        tone = card.get("tone", "neutral")
        html_cards.append(
            f'<div class="trade-diagnostic-card {tone}">'
            f'<div class="trade-diagnostic-pill {tone}">{_escape_html(card.get("label", "診断"))}</div>'
            f'<div class="trade-diagnostic-title">{_escape_html(card.get("title", ""))}</div>'
            f'<div class="trade-diagnostic-main">{_escape_html(card.get("headline", "-"))}</div>'
            f'<div class="trade-diagnostic-sub">{_escape_html(card.get("body", ""))}</div>'
            f'<div class="trade-diagnostic-meta">{_escape_html(card.get("meta", ""))}</div>'
            "</div>"
        )
    st.markdown(
        '<div class="trade-diagnostic-grid">' + "".join(html_cards) + "</div>",
        unsafe_allow_html=True,
    )


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


def format_man_yen(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value / 10000:,.1f}万円"


def make_compact_delta(current: float | int, previous: float | int) -> tuple[str, str]:
    diff = float(current) - float(previous)
    if diff > 0:
        return f"+{format_man_yen(diff)} 前日比", "up"
    if diff < 0:
        return f"{format_man_yen(diff)} 前日比", "down"
    return "±0.0万円 前日比", "flat"


def apply_man_axis(fig: go.Figure, height: int = 360) -> None:
    fig.update_layout(
        template="none",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.42)",
        font=dict(family="system-ui, -apple-system, sans-serif", size=12, color="#374151"),
        margin=dict(l=92, r=18, t=18, b=46),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=11, color="#6b7280"),
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis=dict(
            showgrid=False,
            tickformat="%m/%d",
            tickangle=-30,
            tickfont=dict(size=11, color="#94a3b8"),
            linecolor="rgba(148,163,184,0.16)",
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(148,163,184,0.12)",
            zeroline=True,
            zerolinecolor="rgba(148,163,184,0.2)",
            tickfont=dict(size=12, color="#64748b"),
            ticksuffix="万",
        ),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="white", font_size=12, bordercolor="#e5e7eb"),
        height=height,
    )


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

with st.sidebar:
    if st.button("データ再読み込み", type="primary"):
        st.session_state.clear()
        st.cache_data.clear()
        st.rerun()

tab_import, tab_summary, tab_trend, tab_actions, tab_symbol, tab_trades, tab_market, tab_limits, tab_monthly, tab_history = st.tabs(
    ["取込", "日次サマリ", "推移", "当日アクション", "銘柄分析", "約定", "マーケット指標", "リスク枠", "月次", "履歴"]
)

with tab_import:
    sub_pos, sub_trade = st.tabs(["ポジション", "約定履歴"])

    with sub_pos:
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

        if st.button("保存する", type="primary", disabled=not positions, key="save_positions"):
            replace_snapshot(snapshot_date.isoformat(), raw_text, positions, note)
            st.session_state.clear()
            st.cache_data.clear()
            st.toast(f"{snapshot_date.isoformat()} のデータを保存しました。")
            st.rerun()

    with sub_trade:
        st.markdown('<div class="section-note">約定履歴をタブ区切り(TSV)で貼り付け。同日が既にあれば置換します。</div>', unsafe_allow_html=True)
        trade_date = st.date_input("対象日 (約定日)", value=date.today(), key="trade_import_date")
        trade_note = st.text_input("メモ", placeholder="任意", key="trade_import_note")
        trade_raw = st.text_area("約定履歴をそのまま貼り付け(ヘッダー行込み)", height=420, key="trade_import_raw")
        trades = parse_trade_tsv(trade_raw, fallback_date=trade_date.isoformat()) if trade_raw.strip() else []
        trade_preview_df = pd.DataFrame([t.as_dict() for t in trades]) if trades else pd.DataFrame()

        existing_trade_dates = list_trade_dates()
        already_exists = trade_date.isoformat() in existing_trade_dates

        tc1, tc2, tc3, tc4 = st.columns(4)
        tc1.metric("検出件数", format_number(len(trades)))
        tc2.metric("銘柄数", format_number(trade_preview_df["code"].nunique()) if not trade_preview_df.empty else "0")
        if not trade_preview_df.empty:
            buy_cnt = int((trade_preview_df["side"] == "買").sum())
            sell_cnt = int((trade_preview_df["side"] == "売").sum())
        else:
            buy_cnt = sell_cnt = 0
        tc3.metric("買 / 売", f"{format_number(buy_cnt)} / {format_number(sell_cnt)}")
        tc4.metric("既存データ", "置換" if already_exists else "新規")

        # 日付不一致検出
        if trades:
            mismatched = [t for t in trades if t.trade_date and t.trade_date != trade_date.isoformat()]
            if mismatched:
                unique_dates = sorted({t.trade_date for t in mismatched if t.trade_date})
                st.warning(f"対象日 {trade_date.isoformat()} と異なる日付が {len(mismatched)}件含まれます: {', '.join(unique_dates)}")

        if not trade_preview_df.empty:
            display_trades = trade_preview_df.rename(columns={
                "executed_at": "約定日時",
                "trade_date": "約定日",
                "code": "コード",
                "name": "銘柄名",
                "market": "市場",
                "side": "売買",
                "price": "約定値段",
                "quantity": "約定数量",
                "trade_no": "約定番号",
                "receipt_no": "受付番号",
                "fill_flag": "出来",
                "internal_no": "社内処理番号",
                "price_sign": "値段符号",
            })
            preview_cols = ["約定日時", "コード", "銘柄名", "市場", "売買", "約定値段", "約定数量", "約定番号", "受付番号", "値段符号"]
            render_table(display_trades[[c for c in preview_cols if c in display_trades.columns]],
                         f"trades_preview_{trade_date.isoformat()}.csv", "プレビューCSVをダウンロード")
        elif trade_raw.strip():
            st.warning("約定履歴を検出できていません。ヘッダー行(約定時間/銘柄名/...)を含めて貼り付けてください。")
            preview_lines = trade_raw.splitlines()[:10]
            if preview_lines:
                st.code("\n".join(preview_lines), language="text")

        if st.button("保存する", type="primary", disabled=not trades, key="save_trades"):
            replace_trade_executions(trade_date.isoformat(), trade_raw, trades, trade_note)
            st.session_state.clear()
            st.cache_data.clear()
            st.toast(f"{trade_date.isoformat()} の約定履歴を保存しました({len(trades)}件)。")
            st.rerun()

all_df = load_all_snapshots()
all_trades_df = load_all_trades()
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
    trend_dir_df = build_daily_trend_by_direction(all_df)
    intraday_roundtrips_all = build_intraday_roundtrips(all_trades_df)
    exposure_all = build_daily_exposure(all_df)
    if trend_df.empty:
        st.info("推移を表示するデータがありません。")
    else:
        # ---- 期間セレクタ ----
        period_options = ["5D", "1M", "3M", "6M", "YTD", "1Y", "全期間", "カスタム"]
        selected_period = st.radio(
            "期間",
            period_options,
            index=4,
            horizontal=True,
            key="trend_period",
            label_visibility="collapsed",
        )

        full_view = trend_df.copy().sort_values("snapshot_date").reset_index(drop=True)
        full_view["snapshot_date"] = pd.to_datetime(full_view["snapshot_date"])
        data_max = full_view["snapshot_date"].max()
        data_min = full_view["snapshot_date"].min()
        period_start = None
        period_end = data_max
        if selected_period == "5D":
            period_start = data_max - pd.Timedelta(days=6)
        elif selected_period == "1M":
            period_start = data_max - pd.Timedelta(days=30)
        elif selected_period == "3M":
            period_start = data_max - pd.Timedelta(days=90)
        elif selected_period == "6M":
            period_start = data_max - pd.Timedelta(days=180)
        elif selected_period == "YTD":
            period_start = pd.Timestamp(f"{data_max.year}-01-01")
        elif selected_period == "1Y":
            period_start = data_max - pd.Timedelta(days=365)
        elif selected_period == "カスタム":
            date_range = st.date_input(
                "期間指定",
                value=(data_min.date(), data_max.date()),
                min_value=data_min.date(),
                max_value=data_max.date(),
                key="trend_custom_range",
            )
            if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
                period_start = pd.Timestamp(date_range[0])
                period_end = pd.Timestamp(date_range[1])
            else:
                period_start = data_min

        def _apply_period(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return df
            d = df.copy()
            d["snapshot_date"] = pd.to_datetime(d["snapshot_date"])
            if period_start is not None:
                d = d[d["snapshot_date"] >= period_start]
            d = d[d["snapshot_date"] <= period_end]
            return d.reset_index(drop=True)

        view_df = _apply_period(full_view)

        if view_df.empty:
            st.info("選択した期間にデータがありません。")
        else:
            view_df = view_df.copy()
            view_df["TR累計"] = view_df["TR損益"].cumsum()
            view_df["実現累計"] = view_df["実現損益"].cumsum()

            # ---- ロング/ショート別のピボット (日中決済=フラットは除外) ----
            dir_df = _apply_period(trend_dir_df) if not trend_dir_df.empty else trend_dir_df
            roundtrip_df = intraday_roundtrips_all.copy()
            if not roundtrip_df.empty:
                roundtrip_df["trade_date"] = pd.to_datetime(roundtrip_df["trade_date"], errors="coerce")
                if period_start is not None:
                    roundtrip_df = roundtrip_df[roundtrip_df["trade_date"] >= period_start]
                roundtrip_df = roundtrip_df[roundtrip_df["trade_date"] <= period_end].reset_index(drop=True)
            dates_index = view_df["snapshot_date"]

            def _pivot(metric: str, direction: str) -> pd.Series:
                if dir_df.empty:
                    return pd.Series(0.0, index=range(len(dates_index)))
                sub = dir_df[dir_df["方向"] == direction]
                if sub.empty:
                    return pd.Series(0.0, index=range(len(dates_index)))
                series = sub.set_index("snapshot_date")[metric]
                return series.reindex(dates_index, fill_value=0).reset_index(drop=True)

            def _trade_pivot(direction: str) -> pd.Series:
                if roundtrip_df.empty:
                    return pd.Series(0.0, index=range(len(dates_index)))
                sub = roundtrip_df[roundtrip_df["direction"] == direction]
                if sub.empty:
                    return pd.Series(0.0, index=range(len(dates_index)))
                series = sub.groupby("trade_date", dropna=False)["realized_pl"].sum()
                return series.reindex(dates_index, fill_value=0).reset_index(drop=True)

            long_tr_daily = _pivot("TR損益", "買い")
            short_tr_daily = _pivot("TR損益", "売り")
            long_real_daily = _pivot("実現損益", "買い")
            short_real_daily = _pivot("実現損益", "売り")
            long_unreal_daily = _pivot("評価損益", "買い")
            short_unreal_daily = _pivot("評価損益", "売り")
            intraday_tr_daily = _pivot("TR損益", "フラット")
            intraday_real_daily = _pivot("実現損益", "フラット")
            flat_available = (intraday_tr_daily.abs() > 0) | (intraday_real_daily.abs() > 0)
            roundtrip_long_daily = _trade_pivot("買い").where(flat_available, 0.0)
            roundtrip_short_daily = _trade_pivot("売り").where(flat_available, 0.0)
            roundtrip_total_daily = roundtrip_long_daily + roundtrip_short_daily

            long_tr_daily = long_tr_daily + roundtrip_long_daily
            short_tr_daily = short_tr_daily + roundtrip_short_daily
            long_real_daily = long_real_daily + roundtrip_long_daily
            short_real_daily = short_real_daily + roundtrip_short_daily
            intraday_tr_daily = (intraday_tr_daily - roundtrip_total_daily).mask(lambda s: s.abs() < 0.5, 0.0)
            intraday_real_daily = (intraday_real_daily - roundtrip_total_daily).mask(lambda s: s.abs() < 0.5, 0.0)
            roundtrip_covered_days = int((roundtrip_total_daily.abs() > 0).sum())

            view_df["ロングTR累計"] = long_tr_daily.cumsum().values
            view_df["ショートTR累計"] = short_tr_daily.cumsum().values
            view_df["ロング実現累計"] = long_real_daily.cumsum().values
            view_df["ショート実現累計"] = short_real_daily.cumsum().values
            view_df["ロング評価損益"] = long_unreal_daily.values
            view_df["ショート評価損益"] = short_unreal_daily.values
            view_df["日中TR"] = intraday_tr_daily.values
            view_df["日中実現"] = intraday_real_daily.values

            # ---- 建て玉データをマージ ----
            exp_df = _apply_period(exposure_all) if not exposure_all.empty else exposure_all
            if not exp_df.empty:
                view_df = view_df.merge(
                    exp_df[["snapshot_date", "ロング評価額", "ショート評価額", "グロス評価額", "ネット評価額", "傾き"]],
                    on="snapshot_date",
                    how="left",
                )
            else:
                view_df["ロング評価額"] = 0.0
                view_df["ショート評価額"] = 0.0
                view_df["グロス評価額"] = 0.0
                view_df["ネット評価額"] = view_df["評価額(円貨)"]
                view_df["傾き"] = 0.0

            latest = view_df.iloc[-1]

            def _sub(long_v: float, short_v: float) -> str:
                return f"L {format_man_yen(long_v)} / S {format_man_yen(short_v)}"

            # ---- KPI行 (4カード: 全体+L/S内訳) ----
            st.markdown(
                f"""
                <div class="trend-kpi-row">
                  <div class="trend-kpi accent-pl">
                    <div class="trend-kpi-label">TR累計 (ネット)</div>
                    <div class="trend-kpi-value">{format_man_yen(latest['TR累計'])}</div>
                    <div class="trend-kpi-sub">{_sub(latest['ロングTR累計'], latest['ショートTR累計'])}</div>
                  </div>
                  <div class="trend-kpi accent-real">
                    <div class="trend-kpi-label">実現累計 (ネット)</div>
                    <div class="trend-kpi-value">{format_man_yen(latest['実現累計'])}</div>
                    <div class="trend-kpi-sub">{_sub(latest['ロング実現累計'], latest['ショート実現累計'])}</div>
                  </div>
                  <div class="trend-kpi accent-eval">
                    <div class="trend-kpi-label">評価損益 (ネット)</div>
                    <div class="trend-kpi-value">{format_man_yen(latest['評価損益'])}</div>
                    <div class="trend-kpi-sub">{_sub(latest['ロング評価損益'], latest['ショート評価損益'])}</div>
                  </div>
                  <div class="trend-kpi accent-val">
                    <div class="trend-kpi-label">建て玉 (グロス)</div>
                    <div class="trend-kpi-value">{format_man_yen(latest['グロス評価額'])}</div>
                    <div class="trend-kpi-sub">ネット {format_man_yen(latest['ネット評価額'])} / 傾き {latest['傾き']:+.2f}</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # ---- チャート1: 累計損益の推移 (表示モード切替) ----
            pl_mode = st.radio(
                "損益の表示",
                ["ネット (全体)", "ロングのみ", "ショートのみ", "L vs S (TR累計)"],
                horizontal=True,
                key="trend_pl_mode",
                label_visibility="collapsed",
            )

            st.markdown('<div class="trend-section-title">累計損益の推移</div>', unsafe_allow_html=True)
            fig_pl = go.Figure()
            x = view_df["snapshot_date"]
            if pl_mode == "ネット (全体)":
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["TR累計"] / 10000, name="TR累計",
                    mode="lines+markers", line=dict(color="#c2410c", width=3),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["実現累計"] / 10000, name="実現累計",
                    mode="lines", line=dict(color="#0284c7", width=2.2),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["評価損益"] / 10000, name="評価損益",
                    mode="lines", line=dict(color="#059669", width=2.2),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
            elif pl_mode == "ロングのみ":
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["ロングTR累計"] / 10000, name="ロング TR累計",
                    mode="lines+markers", line=dict(color="#c2410c", width=3),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["ロング実現累計"] / 10000, name="ロング 実現累計",
                    mode="lines", line=dict(color="#0284c7", width=2.2),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["ロング評価損益"] / 10000, name="ロング 評価損益",
                    mode="lines", line=dict(color="#059669", width=2.2),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
            elif pl_mode == "ショートのみ":
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["ショートTR累計"] / 10000, name="ショート TR累計",
                    mode="lines+markers", line=dict(color="#c2410c", width=3),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["ショート実現累計"] / 10000, name="ショート 実現累計",
                    mode="lines", line=dict(color="#0284c7", width=2.2),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["ショート評価損益"] / 10000, name="ショート 評価損益",
                    mode="lines", line=dict(color="#059669", width=2.2),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
            else:  # L vs S (TR累計)
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["TR累計"] / 10000, name="ネット (全体)",
                    mode="lines", line=dict(color="#334155", width=2.5, dash="dot"),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["ロングTR累計"] / 10000, name="ロング",
                    mode="lines+markers", line=dict(color="#c2410c", width=2.5),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
                fig_pl.add_trace(go.Scatter(x=x, y=view_df["ショートTR累計"] / 10000, name="ショート",
                    mode="lines+markers", line=dict(color="#0284c7", width=2.5),
                    hovertemplate="%{y:,.1f}万円<extra></extra>"))
            apply_man_axis(fig_pl, height=380)
            st.plotly_chart(fig_pl, width="stretch", config={"displayModeBar": False})

            # ---- チャート2: 建て玉の推移 (ロング/ショート積み上げ + ネット線) ----
            if not exp_df.empty:
                st.markdown('<div class="trend-section-title">建て玉の推移</div>', unsafe_allow_html=True)
                fig_exp = go.Figure()
                fig_exp.add_trace(go.Bar(
                    x=exp_df["snapshot_date"], y=exp_df["ロング評価額"] / 10000,
                    name="ロング", marker_color="rgba(194,65,12,0.65)",
                    hovertemplate="%{y:,.1f}万円<extra></extra>",
                ))
                fig_exp.add_trace(go.Bar(
                    x=exp_df["snapshot_date"], y=exp_df["ショート評価額"] / 10000,
                    name="ショート", marker_color="rgba(2,132,199,0.65)",
                    hovertemplate="%{y:,.1f}万円<extra></extra>",
                ))
                fig_exp.add_trace(go.Scatter(
                    x=exp_df["snapshot_date"], y=exp_df["ネット評価額"] / 10000,
                    name="ネット", mode="lines+markers",
                    line=dict(color="#7c3aed", width=2.8),
                    marker=dict(size=5, color="#7c3aed"),
                    hovertemplate="%{y:,.1f}万円<extra></extra>",
                ))
                fig_exp.update_layout(barmode="relative", legend=dict(orientation="h", y=-0.18))
                apply_man_axis(fig_exp, height=320)
                st.plotly_chart(fig_exp, width="stretch", config={"displayModeBar": False})
                latest_exp = exp_df.iloc[-1]
                st.caption(
                    f"最新: グロス {format_man_yen(latest_exp['グロス評価額'])} / "
                    f"ネット {format_man_yen(latest_exp['ネット評価額'])} / "
                    f"傾き {latest_exp['傾き']:+.2f}"
                )

            # ---- 方向別 日次表 (ネット列つき) ----
            st.markdown('<div class="trend-section-title">方向別 日次表</div>', unsafe_allow_html=True)
            ls_table = pd.DataFrame({
                "日付": view_df["snapshot_date"].dt.strftime("%Y-%m-%d"),
                "ロングTR": view_df["ロングTR累計"].diff().fillna(view_df["ロングTR累計"]),
                "ショートTR": view_df["ショートTR累計"].diff().fillna(view_df["ショートTR累計"]),
                "日中TR": view_df["日中TR"],
                "ネットTR": view_df["TR損益"],
                "ロング実現": view_df["ロング実現累計"].diff().fillna(view_df["ロング実現累計"]),
                "ショート実現": view_df["ショート実現累計"].diff().fillna(view_df["ショート実現累計"]),
                "日中実現": view_df["日中実現"],
                "ネット実現": view_df["実現損益"],
                "ロング評価": view_df["ロング評価損益"],
                "ショート評価": view_df["ショート評価損益"],
                "ネット評価": view_df["評価損益"],
            })
            # 日中列が全部0なら削除
            if (ls_table["日中TR"].abs().sum() == 0) and (ls_table["日中実現"].abs().sum() == 0):
                ls_table = ls_table.drop(columns=["日中TR", "日中実現"])
            else:
                if roundtrip_covered_days > 0:
                    st.caption(
                        f"約定履歴がある {format_number(roundtrip_covered_days)} 日は、日中損益を買い/売りへ振り直しています。"
                        "日中列に残る値は、スナップショットとの差分または未分類分です。"
                    )
                else:
                    st.caption("「日中」はその日にエントリーして当日決済したデイトレ等(ネット数量0で残存)。ネット = ロング + ショート + 日中。")
            ls_table = ls_table.iloc[::-1].reset_index(drop=True)
            render_table(ls_table, "daily_ls.csv", "方向別日次表CSV")

            # ---- 日次データ (評価額系) ----
            with st.expander("評価額・銘柄数の日次データ", expanded=False):
                table_df = view_df.copy()
                table_df["snapshot_date"] = table_df["snapshot_date"].dt.strftime("%Y-%m-%d")
                table_df = table_df.rename(columns={"snapshot_date": "日付", "件数": "銘柄数"})
                display_columns = [
                    "日付", "ロング評価額", "ショート評価額", "ネット評価額", "グロス評価額",
                    "傾き", "銘柄数",
                ]
                table_display = table_df[[c for c in display_columns if c in table_df.columns]].iloc[::-1].reset_index(drop=True)
                render_table(table_display, "daily_trend.csv", "評価額CSV")

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
                "TR損益差分", "TR(新規分)", "TR(既存分)",
                "実現損益差分", "評価損益差分", "評価額差分",
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

            # ---- 方向別タイムライン ----
            dir_timeline_df = build_instrument_timeline_by_direction(instrument_df, selected_code)
            active_directions = [
                d for d in ["買い", "売り", "フラット"]
                if d in dir_timeline_df["方向"].unique()
                and dir_timeline_df[dir_timeline_df["方向"] == d][["TR損益", "実現損益", "評価損益"]].abs().sum().sum() > 0
            ]
            if len(active_directions) >= 2:
                st.markdown('<div class="trend-section-title">方向別 推移</div>', unsafe_allow_html=True)
                dir_color = {"買い": "#c2410c", "売り": "#0284c7", "フラット": "#94a3b8"}
                dir_sorted = dir_timeline_df.sort_values(["方向", "snapshot_date"]).reset_index(drop=True)
                dir_sorted["TR損益(累計)"] = dir_sorted.groupby("方向")["TR損益"].cumsum()
                dir_sorted["実現損益(累計)"] = dir_sorted.groupby("方向")["実現損益"].cumsum()

                sym_col_l, sym_col_r = st.columns(2)
                with sym_col_l:
                    st.markdown('<div class="trend-section-title">方向別 TR累計</div>', unsafe_allow_html=True)
                    fig_sym_tr = go.Figure()
                    for direction in active_directions:
                        sub = dir_sorted[dir_sorted["方向"] == direction]
                        fig_sym_tr.add_trace(
                            go.Scatter(
                                x=sub["snapshot_date"],
                                y=sub["TR損益(累計)"] / 10000,
                                name=direction,
                                mode="lines+markers",
                                line=dict(color=dir_color.get(direction, "#64748b"), width=2.5),
                                marker=dict(size=5),
                                hovertemplate="%{y:,.1f}万円<extra></extra>",
                            )
                        )
                    apply_man_axis(fig_sym_tr, height=280)
                    st.plotly_chart(fig_sym_tr, width="stretch", config={"displayModeBar": False})

                with sym_col_r:
                    st.markdown('<div class="trend-section-title">方向別 評価損益</div>', unsafe_allow_html=True)
                    fig_sym_eval = go.Figure()
                    for direction in active_directions:
                        sub = dir_sorted[dir_sorted["方向"] == direction]
                        fig_sym_eval.add_trace(
                            go.Scatter(
                                x=sub["snapshot_date"],
                                y=sub["評価損益"] / 10000,
                                name=direction,
                                mode="lines",
                                line=dict(color=dir_color.get(direction, "#64748b"), width=2.5),
                                hovertemplate="%{y:,.1f}万円<extra></extra>",
                            )
                        )
                    apply_man_axis(fig_sym_eval, height=280)
                    st.plotly_chart(fig_sym_eval, width="stretch", config={"displayModeBar": False})

                dir_display = dir_sorted.copy()
                dir_display["snapshot_date"] = dir_display["snapshot_date"].dt.strftime("%Y-%m-%d")
                render_table(dir_display, f"timeline_{selected_code}_by_direction.csv", "方向別時系列CSV")

            display_timeline = timeline_df.copy()
            display_timeline["snapshot_date"] = display_timeline["snapshot_date"].dt.strftime("%Y-%m-%d")
            render_table(display_timeline, f"timeline_{selected_code}.csv", "銘柄時系列CSV")

with tab_trades:
    trade_dates = list_trade_dates()
    if not trade_dates:
        st.info("約定履歴がありません。「取込」タブ → 「約定履歴」から取り込んでください。")
    else:
        period_options = ["5D", "1M", "3M", "6M", "YTD", "1Y", "全期間", "カスタム"]
        selected_trade_period = st.radio(
            "期間",
            period_options,
            index=4,
            horizontal=True,
            key="trade_period",
            label_visibility="collapsed",
        )

        trade_date_index = pd.to_datetime(pd.Series(trade_dates), errors="coerce").dropna().sort_values()
        trade_data_max = trade_date_index.max()
        trade_data_min = trade_date_index.min()
        trade_period_start = None
        trade_period_end = trade_data_max
        if selected_trade_period == "5D":
            trade_period_start = trade_data_max - pd.Timedelta(days=6)
        elif selected_trade_period == "1M":
            trade_period_start = trade_data_max - pd.Timedelta(days=30)
        elif selected_trade_period == "3M":
            trade_period_start = trade_data_max - pd.Timedelta(days=90)
        elif selected_trade_period == "6M":
            trade_period_start = trade_data_max - pd.Timedelta(days=180)
        elif selected_trade_period == "YTD":
            trade_period_start = pd.Timestamp(f"{trade_data_max.year}-01-01")
        elif selected_trade_period == "1Y":
            trade_period_start = trade_data_max - pd.Timedelta(days=365)
        elif selected_trade_period == "カスタム":
            trade_range = st.date_input(
                "約定の期間指定",
                value=(trade_data_min.date(), trade_data_max.date()),
                min_value=trade_data_min.date(),
                max_value=trade_data_max.date(),
                key="trade_custom_range",
            )
            if isinstance(trade_range, (tuple, list)) and len(trade_range) == 2:
                trade_period_start = pd.Timestamp(trade_range[0])
                trade_period_end = pd.Timestamp(trade_range[1])
            else:
                trade_period_start = trade_data_min

        filtered_all_trades = all_trades_df.copy()
        filtered_all_trades["_trade_date_ts"] = pd.to_datetime(filtered_all_trades["trade_date"], errors="coerce")
        if trade_period_start is not None:
            filtered_all_trades = filtered_all_trades[filtered_all_trades["_trade_date_ts"] >= trade_period_start]
        filtered_all_trades = filtered_all_trades[filtered_all_trades["_trade_date_ts"] <= trade_period_end]
        filtered_trade_dates = sorted(
            filtered_all_trades["_trade_date_ts"].dropna().dt.strftime("%Y-%m-%d").unique().tolist(),
            reverse=True,
        )
        filtered_all_trades = filtered_all_trades.drop(columns="_trade_date_ts").reset_index(drop=True)

        if not filtered_trade_dates or filtered_all_trades.empty:
            st.info("選択した期間に約定履歴がありません。")
        else:
            roundtrip_all_df = build_intraday_roundtrips(filtered_all_trades)
            session_summary_all = build_trade_session_summary(filtered_all_trades)
            habit_profile_all = build_trade_habit_profile(filtered_all_trades)
            roundtrip_profile_all = build_roundtrip_profile(filtered_all_trades)
            snapshot_for_overnight = all_df.copy()
            if not snapshot_for_overnight.empty:
                snapshot_for_overnight["snapshot_date"] = pd.to_datetime(
                    snapshot_for_overnight["snapshot_date"], errors="coerce"
                )
                snapshot_for_overnight = snapshot_for_overnight.dropna(subset=["snapshot_date"]).copy()
                snapshot_for_overnight = snapshot_for_overnight[
                    snapshot_for_overnight["snapshot_date"] <= trade_period_end
                ].copy()
            overnight_profile_all = build_overnight_hold_profile(snapshot_for_overnight)
            if not overnight_profile_all.empty and trade_period_start is not None:
                overnight_profile_all = overnight_profile_all[
                    overnight_profile_all["exit_date"] >= trade_period_start
                ].copy()
            overnight_profile_all = overnight_profile_all.reset_index(drop=True)
            trade_range_label = f"{filtered_trade_dates[-1]} - {filtered_trade_dates[0]}"

            active_trade_days = int(filtered_all_trades["trade_date"].nunique()) if not filtered_all_trades.empty else 0
            avg_trades_per_day = len(filtered_all_trades) / active_trade_days if active_trade_days else 0.0
            daytrade_win_rate = (
                float((roundtrip_profile_all["realized_pl"] > 0).mean() * 100)
                if not roundtrip_profile_all.empty else 0.0
            )
            median_holding_minutes = (
                float(roundtrip_profile_all["holding_minutes"].median())
                if not roundtrip_profile_all.empty else 0.0
            )
            opening_trade_count = 0
            if not session_summary_all.empty:
                opening_rows = session_summary_all[session_summary_all["session"].astype(str) == "寄付"]
                opening_trade_count = int(opening_rows["trade_count"].sum()) if not opening_rows.empty else 0
            opening_trade_ratio = opening_trade_count / len(filtered_all_trades) * 100 if len(filtered_all_trades) else 0.0
            long_start_ratio = (
                float((roundtrip_profile_all["direction"] == "買い").mean() * 100)
                if not roundtrip_profile_all.empty else 0.0
            )
            avg_roundtrip_pnl = (
                float(roundtrip_profile_all["realized_pl"].mean())
                if not roundtrip_profile_all.empty else 0.0
            )
            overnight_closed_df = overnight_profile_all[overnight_profile_all["status"] != "継続中"].copy()
            overnight_ongoing_df = overnight_profile_all[overnight_profile_all["status"] == "継続中"].copy()
            overnight_count = len(overnight_profile_all)
            ongoing_overnight_count = (
                int((overnight_profile_all["status"] == "継続中").sum())
                if not overnight_profile_all.empty else 0
            )

            overview_tab, daily_tab = st.tabs(["分析ダッシュボード", "日別の確認"])

            with overview_tab:
                st.markdown(
                    '<div class="trade-context-note">'
                    '上段は「どこを直すと効きそうか」の診断、下段はその根拠です。'
                    '損切り時間は銘柄日ベースの近似なので、細かい建玉の入れ替えは平均化されています。'
                    "</div>",
                    unsafe_allow_html=True,
                )
                st.caption(f"対象期間: {trade_range_label}")

                st.markdown('<div class="trend-section-title">トレード傾向分析</div>', unsafe_allow_html=True)
                st.markdown(
                    f'<div class="trend-kpi-row">'
                    f'<div class="trend-kpi accent-pl">'
                    f'<div class="trend-kpi-label">取引日数</div>'
                    f'<div class="trend-kpi-value">{format_number(active_trade_days)}</div>'
                    f'<div class="trend-kpi-sub">全約定 {format_number(len(filtered_all_trades))}件</div>'
                    f"</div>"
                    f'<div class="trend-kpi accent-real">'
                    f'<div class="trend-kpi-label">1日平均約定件数</div>'
                    f'<div class="trend-kpi-value">{format_number(avg_trades_per_day)}</div>'
                    f'<div class="trend-kpi-sub">寄付5分の比率 {opening_trade_ratio:.1f}%</div>'
                    f"</div>"
                    f'<div class="trend-kpi accent-eval">'
                    f'<div class="trend-kpi-label">デイトレ勝率</div>'
                    f'<div class="trend-kpi-value">{daytrade_win_rate:.1f}%</div>'
                    f'<div class="trend-kpi-sub">買い先行比率 {long_start_ratio:.1f}%</div>'
                    f"</div>"
                    f'<div class="trend-kpi accent-val">'
                    f'<div class="trend-kpi-label">中央値保有時間</div>'
                    f'<div class="trend-kpi-value">{median_holding_minutes:.1f}分</div>'
                    f'<div class="trend-kpi-sub">平均デイトレ損益 {format_number(avg_roundtrip_pnl)}円</div>'
                    f"</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                timing_cards: list[dict[str, str]] = []
                streak_summary = pd.DataFrame()
                session_edge = pd.DataFrame()
                session_edge_direction = pd.DataFrame()
                holding_outcome_summary = pd.DataFrame()
                holding_bucket_summary = pd.DataFrame()
                holding_outcome_direction_summary = pd.DataFrame()
                holding_bucket_direction_summary = pd.DataFrame()
                ongoing_overnight_display = pd.DataFrame()
                overnight_detail_display = pd.DataFrame()

                if not roundtrip_profile_all.empty:
                    winners = roundtrip_profile_all[roundtrip_profile_all["outcome"] == "Win"]
                    losers = roundtrip_profile_all[roundtrip_profile_all["outcome"] == "Lose"]
                    win_hold_median = float(winners["holding_minutes"].median()) if not winners.empty else 0.0
                    lose_hold_median = float(losers["holding_minutes"].median()) if not losers.empty else 0.0
                    if winners.empty or losers.empty:
                        timing_cards.append(
                            {
                                "label": "近似診断",
                                "title": "損切り時間感覚",
                                "headline": "判定材料がまだ少ない",
                                "body": "勝ち負けの両方が十分に揃うと、時間感覚の比較がもっと安定します。",
                                "meta": f"勝ち {len(winners)}件 / 負け {len(losers)}件",
                                "tone": "neutral",
                            }
                        )
                    elif lose_hold_median >= win_hold_median * 1.5 and (lose_hold_median - win_hold_median) >= 10:
                        timing_cards.append(
                            {
                                "label": "改善余地",
                                "title": "損切り時間感覚",
                                "headline": "負けを長く抱えがち",
                                "body": f"負けの中央値が {lose_hold_median:.1f}分で、勝ちの {win_hold_median:.1f}分より長めです。",
                                "meta": "引っ張るほど損失が膨らんでいないか確認したい局面です。",
                                "tone": "bad",
                            }
                        )
                    elif win_hold_median > 0 and lose_hold_median <= win_hold_median * 0.7:
                        timing_cards.append(
                            {
                                "label": "比較的良好",
                                "title": "損切り時間感覚",
                                "headline": "負けは早めに切れている",
                                "body": f"負けの中央値 {lose_hold_median:.1f}分に対して、勝ちは {win_hold_median:.1f}分まで持てています。",
                                "meta": "次は利確を伸ばせる場面を探す段階です。",
                                "tone": "good",
                            }
                        )
                    else:
                        timing_cards.append(
                            {
                                "label": "中立",
                                "title": "損切り時間感覚",
                                "headline": "勝ち負けの保有差は小さめ",
                                "body": f"勝ち {win_hold_median:.1f}分 / 負け {lose_hold_median:.1f}分で、大きな偏りはまだ見えていません。",
                                "meta": "保有時間より銘柄選びや時間帯の影響が大きい可能性があります。",
                                "tone": "neutral",
                            }
                        )

                    ordered_profile = roundtrip_profile_all.sort_values(
                        ["trade_date", "first_executed_ts", "code"], kind="stable"
                    ).reset_index(drop=True)
                    prior_loss_streaks: list[int] = []
                    loss_streak = 0
                    for outcome in ordered_profile["outcome"]:
                        prior_loss_streaks.append(loss_streak)
                        if outcome == "Lose":
                            loss_streak += 1
                        else:
                            loss_streak = 0
                    ordered_profile["prior_loss_streak"] = prior_loss_streaks
                    ordered_profile["streak_bucket"] = ordered_profile["prior_loss_streak"].apply(
                        lambda value: "通常" if value == 0 else ("1連敗後" if value == 1 else "2連敗以上")
                    )
                    streak_summary = (
                        ordered_profile.groupby("streak_bucket", dropna=False)
                        .agg(
                            件数=("code", "count"),
                            勝率=("realized_pl", lambda s: (s > 0).mean() * 100),
                            平均損益=("realized_pl", "mean"),
                            実現損益合計=("realized_pl", "sum"),
                            平均売買代金=("total_notional", "mean"),
                        )
                        .reset_index()
                    )
                    streak_sort = {"通常": 0, "1連敗後": 1, "2連敗以上": 2}
                    streak_summary["_sort"] = streak_summary["streak_bucket"].map(streak_sort).fillna(99)
                    streak_summary = streak_summary.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

                    base_row = streak_summary[streak_summary["streak_bucket"] == "通常"]
                    stress_label = "2連敗以上" if not streak_summary[streak_summary["streak_bucket"] == "2連敗以上"].empty else "1連敗後"
                    stress_row = streak_summary[streak_summary["streak_bucket"] == stress_label]
                    if base_row.empty or stress_row.empty:
                        timing_cards.append(
                            {
                                "label": "観測不足",
                                "title": "連敗後のサイズ",
                                "headline": "連敗局面のサンプルが少ない",
                                "body": "連敗後のサイズ変化は、もう少しデータが増えると安定して見えてきます。",
                                "meta": "通常時との比較を準備中です。",
                                "tone": "neutral",
                            }
                        )
                    else:
                        base_notional = float(base_row.iloc[0]["平均売買代金"])
                        stress_notional = float(stress_row.iloc[0]["平均売買代金"])
                        base_mean_pl = float(base_row.iloc[0]["平均損益"])
                        stress_mean_pl = float(stress_row.iloc[0]["平均損益"])
                        if base_notional > 0 and stress_notional >= base_notional * 1.15 and stress_mean_pl < base_mean_pl:
                            timing_cards.append(
                                {
                                    "label": "改善余地",
                                    "title": "連敗後のサイズ",
                                    "headline": "連敗後にサイズが膨らみがち",
                                    "body": f"通常時 {format_man_yen(base_notional)} に対して、{stress_label} は {format_man_yen(stress_notional)} です。",
                                    "meta": "負けの後に取り返そうとしてロットが増えていないか見直し候補です。",
                                    "tone": "bad",
                                }
                            )
                        elif base_notional > 0 and stress_notional <= base_notional * 0.9:
                            timing_cards.append(
                                {
                                    "label": "比較的良好",
                                    "title": "連敗後のサイズ",
                                    "headline": "連敗後はサイズを抑えられている",
                                    "body": f"通常時 {format_man_yen(base_notional)} に対して、{stress_label} は {format_man_yen(stress_notional)} です。",
                                    "meta": "感情でサイズが暴れていないのは強みです。",
                                    "tone": "good",
                                }
                            )
                        else:
                            timing_cards.append(
                                {
                                    "label": "中立",
                                    "title": "連敗後のサイズ",
                                    "headline": "サイズ変化は大きくない",
                                    "body": f"通常時 {format_man_yen(base_notional)} / {stress_label} {format_man_yen(stress_notional)} で、極端な増減は見えません。",
                                    "meta": "ロットより銘柄選びや時間帯の影響を先に見た方がよさそうです。",
                                    "tone": "neutral",
                                }
                            )

                    session_edge = (
                        ordered_profile.groupby("session_group", dropna=False, observed=False)
                        .agg(
                            件数=("code", "count"),
                            勝率=("realized_pl", lambda s: (s > 0).mean() * 100),
                            実現損益合計=("realized_pl", "sum"),
                            平均損益=("realized_pl", "mean"),
                            中央保有時間=("holding_minutes", "median"),
                        )
                        .reset_index()
                    )
                    session_edge = session_edge[session_edge["件数"] > 0].copy()
                    session_edge["session_group"] = session_edge["session_group"].astype(str)
                    session_sort = {"前場": 0, "後場": 1, "引け・時間外": 2, "時間不明": 3}
                    session_edge["_sort"] = session_edge["session_group"].map(session_sort).fillna(99)
                    session_edge = session_edge.sort_values("_sort", kind="stable").drop(columns="_sort").reset_index(drop=True)

                    session_edge_direction = (
                        ordered_profile.groupby(["session_group", "direction"], dropna=False, observed=False)
                        .agg(
                            件数=("code", "count"),
                            勝率=("realized_pl", lambda s: (s > 0).mean() * 100),
                            実現損益合計=("realized_pl", "sum"),
                            平均損益=("realized_pl", "mean"),
                            中央保有時間=("holding_minutes", "median"),
                        )
                        .reset_index()
                        .rename(columns={"session_group": "時間帯", "direction": "方向"})
                    )
                    session_edge_direction = session_edge_direction[session_edge_direction["件数"] > 0].copy()
                    session_edge_direction["時間帯"] = session_edge_direction["時間帯"].astype(str)
                    direction_sort = {"買い": 0, "売り": 1}
                    session_edge_direction["_time_sort"] = session_edge_direction["時間帯"].map(session_sort).fillna(99)
                    session_edge_direction["_dir_sort"] = session_edge_direction["方向"].map(direction_sort).fillna(99)
                    session_edge_direction = session_edge_direction.sort_values(
                        ["_time_sort", "_dir_sort"],
                        kind="stable",
                    ).drop(columns=["_time_sort", "_dir_sort"]).reset_index(drop=True)

                    am_row = session_edge[session_edge["session_group"] == "前場"]
                    pm_row = session_edge[session_edge["session_group"] == "後場"]
                    if am_row.empty or pm_row.empty:
                        timing_cards.append(
                            {
                                "label": "観測不足",
                                "title": "前場 / 後場の相性",
                                "headline": "比較対象がまだ少ない",
                                "body": "前場と後場の両方に十分なサンプルがあると、時間帯の相性がはっきりします。",
                                "meta": "引け寄りや時間外の約定も分離して集計中です。",
                                "tone": "neutral",
                            }
                        )
                    else:
                        am_mean = float(am_row.iloc[0]["平均損益"])
                        pm_mean = float(pm_row.iloc[0]["平均損益"])
                        am_win = float(am_row.iloc[0]["勝率"])
                        pm_win = float(pm_row.iloc[0]["勝率"])
                        if pm_mean - am_mean > 1000:
                            headline = "後場の方が平均損益が良い"
                            tone = "good" if pm_mean > 0 else "warn"
                        elif am_mean - pm_mean > 1000:
                            headline = "前場の方が平均損益が良い"
                            tone = "good" if am_mean > 0 else "warn"
                        else:
                            headline = "前場と後場の差はまだ小さい"
                            tone = "neutral"
                        timing_cards.append(
                            {
                                "label": "時間帯",
                                "title": "前場 / 後場の相性",
                                "headline": headline,
                                "body": f"前場 {format_number(am_mean)}円 ({am_win:.1f}%) / 後場 {format_number(pm_mean)}円 ({pm_win:.1f}%)",
                                "meta": "勝率だけでなく、平均損益も合わせて見ています。",
                                "tone": tone,
                            }
                        )

                    holding_outcome_summary = (
                        ordered_profile.groupby("outcome", dropna=False)
                        .agg(
                            件数=("code", "count"),
                            中央保有時間=("holding_minutes", "median"),
                            平均保有時間=("holding_minutes", "mean"),
                            平均損益=("realized_pl", "mean"),
                            実現損益合計=("realized_pl", "sum"),
                        )
                        .reset_index()
                        .rename(columns={"outcome": "結果"})
                    )
                    holding_outcome_summary["結果"] = holding_outcome_summary["結果"].map(
                        {"Win": "勝ち", "Lose": "負け", "Even": "引分"}
                    ).fillna(holding_outcome_summary["結果"])
                    outcome_sort = {"勝ち": 0, "負け": 1, "引分": 2}
                    holding_outcome_summary["_sort"] = holding_outcome_summary["結果"].map(outcome_sort).fillna(99)
                    holding_outcome_summary = holding_outcome_summary.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

                    holding_outcome_direction_summary = (
                        ordered_profile.groupby(["outcome", "direction"], dropna=False)
                        .agg(
                            件数=("code", "count"),
                            中央保有時間=("holding_minutes", "median"),
                            平均保有時間=("holding_minutes", "mean"),
                            平均損益=("realized_pl", "mean"),
                            実現損益合計=("realized_pl", "sum"),
                        )
                        .reset_index()
                        .rename(columns={"outcome": "結果", "direction": "方向"})
                    )
                    holding_outcome_direction_summary["結果"] = holding_outcome_direction_summary["結果"].map(
                        {"Win": "勝ち", "Lose": "負け", "Even": "引分"}
                    ).fillna(holding_outcome_direction_summary["結果"])
                    holding_outcome_direction_summary["_outcome_sort"] = holding_outcome_direction_summary["結果"].map(
                        outcome_sort
                    ).fillna(99)
                    holding_outcome_direction_summary["_dir_sort"] = holding_outcome_direction_summary["方向"].map(
                        direction_sort
                    ).fillna(99)
                    holding_outcome_direction_summary = holding_outcome_direction_summary.sort_values(
                        ["_outcome_sort", "_dir_sort"],
                        kind="stable",
                    ).drop(columns=["_outcome_sort", "_dir_sort"]).reset_index(drop=True)

                    holding_bucket_summary = (
                        ordered_profile.groupby("holding_bucket", dropna=False, observed=False)
                        .agg(
                            件数=("code", "count"),
                            勝率=("realized_pl", lambda s: (s > 0).mean() * 100),
                            平均損益=("realized_pl", "mean"),
                            実現損益合計=("realized_pl", "sum"),
                        )
                        .reset_index()
                        .rename(columns={"holding_bucket": "保有時間帯"})
                    )
                    holding_bucket_summary = holding_bucket_summary[holding_bucket_summary["件数"] > 0].copy()
                    holding_bucket_summary["保有時間帯"] = holding_bucket_summary["保有時間帯"].astype(str)

                    holding_bucket_direction_summary = (
                        ordered_profile.groupby(["holding_bucket", "direction"], dropna=False, observed=False)
                        .agg(
                            件数=("code", "count"),
                            勝率=("realized_pl", lambda s: (s > 0).mean() * 100),
                            平均損益=("realized_pl", "mean"),
                            実現損益合計=("realized_pl", "sum"),
                            中央保有時間=("holding_minutes", "median"),
                        )
                        .reset_index()
                        .rename(columns={"holding_bucket": "保有時間帯", "direction": "方向"})
                    )
                    holding_bucket_direction_summary = holding_bucket_direction_summary[
                        holding_bucket_direction_summary["件数"] > 0
                    ].copy()
                    holding_bucket_direction_summary["保有時間帯"] = (
                        holding_bucket_direction_summary["保有時間帯"].astype(str)
                    )
                    holding_bucket_direction_summary["_bucket_sort"] = holding_bucket_direction_summary["保有時間帯"].map(
                        {"0-5分": 0, "5-30分": 1, "30-120分": 2, "120分以上": 3, "時間不明": 4}
                    ).fillna(99)
                    holding_bucket_direction_summary["_dir_sort"] = holding_bucket_direction_summary["方向"].map(
                        direction_sort
                    ).fillna(99)
                    holding_bucket_direction_summary = holding_bucket_direction_summary.sort_values(
                        ["_bucket_sort", "_dir_sort"],
                        kind="stable",
                    ).drop(columns=["_bucket_sort", "_dir_sort"]).reset_index(drop=True)

                if not overnight_profile_all.empty:
                    overnight_detail_display = overnight_profile_all.copy().sort_values(
                        ["exit_date", "entry_date", "code"],
                        ascending=[False, False, True],
                        kind="stable",
                    )
                    overnight_detail_display["entry_date"] = overnight_detail_display["entry_date"].dt.strftime("%Y-%m-%d")
                    overnight_detail_display["exit_date"] = overnight_detail_display["exit_date"].dt.strftime("%Y-%m-%d")
                    overnight_detail_display["outcome"] = overnight_detail_display["outcome"].map(
                        {"Win": "勝ち", "Lose": "負け", "Even": "引分"}
                    ).fillna(overnight_detail_display["outcome"])
                    overnight_detail_display = overnight_detail_display.rename(
                        columns={
                            "entry_date": "建玉日",
                            "exit_date": "終了/最新観測日",
                            "code": "コード",
                            "name": "銘柄名",
                            "direction": "方向",
                            "holding_days": "保有日数",
                            "observed_days": "観測営業日数",
                            "total_tr_pl": "TR損益合計",
                            "total_realized_pl": "実現損益合計",
                            "avg_daily_tr_pl": "1日平均TR",
                            "final_unrealized_pl": "最終評価損益",
                            "entry_market_value_abs": "建玉時サイズ",
                            "avg_market_value_abs": "平均サイズ",
                            "max_market_value_abs": "最大サイズ",
                            "position_size_bucket": "サイズ帯",
                            "size_efficiency_pct": "サイズ効率(%)",
                            "daily_efficiency_pct": "日次効率(%)",
                            "status": "状態",
                            "close_reason": "終了理由",
                            "outcome": "結果",
                        }
                    )

                if not overnight_detail_display.empty and "状態" in overnight_detail_display.columns:
                    ongoing_overnight_display = overnight_detail_display[
                        overnight_detail_display["状態"] == "継続中"
                    ].copy()
                if not ongoing_overnight_display.empty:
                    ongoing_overnight_display = ongoing_overnight_display[
                        [
                            "建玉日",
                            "終了/最新観測日",
                            "コード",
                            "銘柄名",
                            "方向",
                            "保有日数",
                            "観測営業日数",
                            "TR損益合計",
                            "1日平均TR",
                            "最終評価損益",
                            "平均サイズ",
                            "サイズ効率(%)",
                        ]
                    ].reset_index(drop=True)
                    ongoing_overnight_display = ongoing_overnight_display.rename(
                        columns={"終了/最新観測日": "最新観測日"}
                    )

                render_trade_diagnostic_cards(timing_cards)

                if not session_summary_all.empty:
                    session_labels = session_summary_all["session"].astype(str)
                    sc1, sc2 = st.columns(2)

                    with sc1:
                        st.markdown('<div class="trend-section-title">時間帯別 約定件数</div>', unsafe_allow_html=True)
                        fig_session_count = go.Figure()
                        fig_session_count.add_trace(
                            go.Bar(
                                x=session_labels,
                                y=session_summary_all["trade_count"],
                                name="約定件数",
                                marker_color="rgba(194,65,12,0.72)",
                                hovertemplate="%{y:,.0f}件<extra></extra>",
                            )
                        )
                        fig_session_count.update_layout(
                            template="none",
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(255,255,255,0.42)",
                            font=dict(family="system-ui, -apple-system, sans-serif", size=12, color="#374151"),
                            margin=dict(l=40, r=18, t=18, b=46),
                            height=300,
                            xaxis=dict(showgrid=False, tickangle=-25, tickfont=dict(size=11, color="#94a3b8")),
                            yaxis=dict(showgrid=True, gridcolor="rgba(148,163,184,0.12)", tickfont=dict(size=12, color="#64748b")),
                            legend=dict(orientation="h", y=1.02, x=0),
                            hovermode="x unified",
                        )
                        st.plotly_chart(fig_session_count, width="stretch", config={"displayModeBar": False})

                    with sc2:
                        st.markdown('<div class="trend-section-title">時間帯別 約定代金</div>', unsafe_allow_html=True)
                        fig_session_notional = go.Figure()
                        fig_session_notional.add_trace(
                            go.Bar(
                                x=session_labels,
                                y=session_summary_all["notional"] / 10000,
                                name="約定代金",
                                marker_color="rgba(2,132,199,0.72)",
                                hovertemplate="%{y:,.1f}万円<extra></extra>",
                            )
                        )
                        apply_man_axis(fig_session_notional, height=300)
                        st.plotly_chart(fig_session_notional, width="stretch", config={"displayModeBar": False})

                if not holding_outcome_summary.empty or not holding_bucket_summary.empty:
                    timing_left, timing_right = st.columns(2)
                    with timing_left:
                        st.markdown('<div class="trend-section-title">勝ち / 負けの保有時間</div>', unsafe_allow_html=True)
                        render_table(holding_outcome_summary, "trade_habit_timing.csv", "保有時間比較CSV")

                    with timing_right:
                        st.markdown('<div class="trend-section-title">保有時間帯別の平均損益</div>', unsafe_allow_html=True)
                        fig_holding = go.Figure()
                        colors = [
                            "rgba(5,150,105,0.72)" if value > 0 else "rgba(220,38,38,0.72)" if value < 0 else "rgba(148,163,184,0.72)"
                            for value in holding_bucket_summary["平均損益"]
                        ]
                        fig_holding.add_trace(
                            go.Bar(
                                x=holding_bucket_summary["保有時間帯"],
                                y=holding_bucket_summary["平均損益"] / 10000,
                                name="平均損益",
                                marker_color=colors,
                                hovertemplate="%{y:,.2f}万円<extra></extra>",
                            )
                        )
                        apply_man_axis(fig_holding, height=300)
                        st.plotly_chart(fig_holding, width="stretch", config={"displayModeBar": False})

                st.markdown('<div class="trend-section-title">オーバーナイト分析</div>', unsafe_allow_html=True)
                st.markdown(
                    '<div class="trade-context-note">'
                    '持越しは日次スナップショットを連結して再構成しています。'
                    'TR損益は保有期間の日次合算で、ドテン日は当日引け時点の方向側に寄せています。'
                    '1日平均TR は TR損益合計 ÷ 観測営業日数、サイズ効率 は TR損益合計 ÷ 平均サイズ です。'
                    '継続中の行は「終了/最新観測日」が実際の解消日ではなく、最新スナップショット日です。'
                    "</div>",
                    unsafe_allow_html=True,
                )
                if overnight_profile_all.empty:
                    st.caption("選択期間に1日以上持ち越したポジションはありません。")
                else:
                    include_ongoing_overnight = st.toggle(
                        "保有中も統計に含める",
                        value=False,
                        key="include_ongoing_overnight",
                        help="OFFではクローズ済みのみ、ONでは継続中もTR損益ベースで統計に含めます。",
                    )
                    overnight_stats_df = (
                        overnight_profile_all.copy()
                        if include_ongoing_overnight
                        else overnight_closed_df.copy()
                    )
                    overnight_stats_mode_label = (
                        "保有中を含む"
                        if include_ongoing_overnight
                        else "クローズ済みのみ"
                    )
                    overnight_stats_count = len(overnight_stats_df)
                    overnight_win_rate = (
                        float((overnight_stats_df["total_tr_pl"] > 0).mean() * 100)
                        if not overnight_stats_df.empty else 0.0
                    )
                    avg_overnight_tr_pl = (
                        float(overnight_stats_df["total_tr_pl"].mean())
                        if not overnight_stats_df.empty else 0.0
                    )
                    avg_overnight_daily_tr = (
                        float(overnight_stats_df["avg_daily_tr_pl"].mean())
                        if not overnight_stats_df.empty else 0.0
                    )
                    avg_overnight_size_eff = (
                        float(overnight_stats_df["size_efficiency_pct"].mean())
                        if not overnight_stats_df.empty else 0.0
                    )
                    overnight_outcome_summary = pd.DataFrame()
                    overnight_bucket_summary = pd.DataFrame()
                    overnight_bucket_direction_summary = pd.DataFrame()
                    overnight_direction_summary = pd.DataFrame()
                    overnight_size_summary = pd.DataFrame()
                    overnight_cards: list[dict[str, str]] = []
                    overnight_summary_text = ""
                    if not overnight_stats_df.empty:
                        overnight_outcome_summary = (
                            overnight_stats_df.groupby("outcome", dropna=False)
                            .agg(
                                件数=("code", "count"),
                                平均保有日数=("holding_days", "mean"),
                                中央保有日数=("holding_days", "median"),
                                **{
                                    "1持越し平均TR": ("total_tr_pl", "mean"),
                                    "1日平均TR": ("avg_daily_tr_pl", "mean"),
                                    "サイズ効率(%)": ("size_efficiency_pct", "mean"),
                                    "日次効率(%)": ("daily_efficiency_pct", "mean"),
                                },
                                TR損益合計=("total_tr_pl", "sum"),
                            )
                            .reset_index()
                            .rename(columns={"outcome": "結果"})
                        )
                        overnight_outcome_summary["結果"] = overnight_outcome_summary["結果"].map(
                            {"Win": "勝ち", "Lose": "負け", "Even": "引分"}
                        ).fillna(overnight_outcome_summary["結果"])
                        overnight_outcome_summary["_sort"] = overnight_outcome_summary["結果"].map(
                            {"勝ち": 0, "負け": 1, "引分": 2}
                        ).fillna(99)
                        overnight_outcome_summary = (
                            overnight_outcome_summary.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)
                        )

                        overnight_direction_summary = (
                            overnight_stats_df.groupby("direction", dropna=False)
                            .agg(
                                件数=("code", "count"),
                                勝率=("total_tr_pl", lambda s: (s > 0).mean() * 100),
                                平均保有日数=("holding_days", "mean"),
                                **{
                                    "1持越し平均TR": ("total_tr_pl", "mean"),
                                    "1日平均TR": ("avg_daily_tr_pl", "mean"),
                                    "サイズ効率(%)": ("size_efficiency_pct", "mean"),
                                    "日次効率(%)": ("daily_efficiency_pct", "mean"),
                                },
                                TR損益合計=("total_tr_pl", "sum"),
                            )
                            .reset_index()
                            .rename(columns={"direction": "方向"})
                            .sort_values("方向", kind="stable")
                            .reset_index(drop=True)
                        )
                        overnight_direction_summary["勝率"] = overnight_direction_summary["勝率"].map(
                            lambda value: f"{value:.1f}%"
                        )

                        overnight_bucket_summary = (
                            overnight_stats_df.groupby("holding_day_bucket", dropna=False, observed=False)
                            .agg(
                                件数=("code", "count"),
                                勝率=("total_tr_pl", lambda s: (s > 0).mean() * 100),
                                **{
                                    "1持越し平均TR": ("total_tr_pl", "mean"),
                                    "1日平均TR": ("avg_daily_tr_pl", "mean"),
                                    "サイズ効率(%)": ("size_efficiency_pct", "mean"),
                                    "日次効率(%)": ("daily_efficiency_pct", "mean"),
                                },
                                TR損益合計=("total_tr_pl", "sum"),
                            )
                            .reset_index()
                            .rename(columns={"holding_day_bucket": "保有日数帯"})
                        )
                        overnight_bucket_summary = overnight_bucket_summary[
                            overnight_bucket_summary["件数"] > 0
                        ].copy()
                        overnight_bucket_summary["保有日数帯"] = overnight_bucket_summary["保有日数帯"].astype(str)

                        overnight_bucket_direction_summary = (
                            overnight_stats_df.groupby(["holding_day_bucket", "direction"], dropna=False, observed=False)
                            .agg(
                                件数=("code", "count"),
                                勝率=("total_tr_pl", lambda s: (s > 0).mean() * 100),
                                **{
                                    "1持越し平均TR": ("total_tr_pl", "mean"),
                                    "1日平均TR": ("avg_daily_tr_pl", "mean"),
                                    "サイズ効率(%)": ("size_efficiency_pct", "mean"),
                                    "日次効率(%)": ("daily_efficiency_pct", "mean"),
                                },
                                TR損益合計=("total_tr_pl", "sum"),
                            )
                            .reset_index()
                            .rename(columns={"holding_day_bucket": "保有日数帯", "direction": "方向"})
                        )
                        overnight_bucket_direction_summary = overnight_bucket_direction_summary[
                            overnight_bucket_direction_summary["件数"] > 0
                        ].copy()
                        overnight_bucket_direction_summary["保有日数帯"] = (
                            overnight_bucket_direction_summary["保有日数帯"].astype(str)
                        )
                        holding_day_sort = {
                            "1日": 0,
                            "2-3日": 1,
                            "4-5日": 2,
                            "6-10日": 3,
                            "11日以上": 4,
                        }
                        direction_sort = {"買い": 0, "売り": 1}
                        overnight_bucket_direction_summary["_day_sort"] = overnight_bucket_direction_summary["保有日数帯"].map(
                            holding_day_sort
                        ).fillna(99)
                        overnight_bucket_direction_summary["_dir_sort"] = overnight_bucket_direction_summary["方向"].map(
                            direction_sort
                        ).fillna(99)
                        overnight_bucket_direction_summary = overnight_bucket_direction_summary.sort_values(
                            ["_day_sort", "_dir_sort"],
                            kind="stable",
                        ).drop(columns=["_day_sort", "_dir_sort"]).reset_index(drop=True)

                        overnight_size_summary = (
                            overnight_stats_df.groupby("position_size_bucket", dropna=False, observed=False)
                            .agg(
                                件数=("code", "count"),
                                勝率=("total_tr_pl", lambda s: (s > 0).mean() * 100),
                                平均保有日数=("holding_days", "mean"),
                                **{
                                    "1持越し平均TR": ("total_tr_pl", "mean"),
                                    "1日平均TR": ("avg_daily_tr_pl", "mean"),
                                    "サイズ効率(%)": ("size_efficiency_pct", "mean"),
                                    "日次効率(%)": ("daily_efficiency_pct", "mean"),
                                },
                                TR損益合計=("total_tr_pl", "sum"),
                            )
                            .reset_index()
                            .rename(columns={"position_size_bucket": "サイズ帯"})
                        )
                        overnight_size_summary = overnight_size_summary[
                            overnight_size_summary["件数"] > 0
                        ].copy()
                        overnight_size_summary["サイズ帯"] = overnight_size_summary["サイズ帯"].astype(str)
                        size_sort = {
                            "50万円未満": 0,
                            "50-100万円": 1,
                            "100-300万円": 2,
                            "300-500万円": 3,
                            "500万円以上": 4,
                        }
                        overnight_size_summary["_sort"] = overnight_size_summary["サイズ帯"].map(size_sort).fillna(99)
                        overnight_size_summary = overnight_size_summary.sort_values("_sort", kind="stable").drop(
                            columns="_sort"
                        ).reset_index(drop=True)
                        overnight_size_summary["勝率"] = overnight_size_summary["勝率"].map(
                            lambda value: f"{value:.1f}%"
                        )

                        direction_diag = (
                            overnight_stats_df.groupby("direction", dropna=False)
                            .agg(
                                件数=("code", "count"),
                                勝率=("total_tr_pl", lambda s: (s > 0).mean() * 100),
                                **{
                                    "1日平均TR": ("avg_daily_tr_pl", "mean"),
                                    "サイズ効率": ("size_efficiency_pct", "mean"),
                                },
                            )
                            .reset_index()
                        )
                        buy_diag = direction_diag[direction_diag["direction"] == "買い"]
                        sell_diag = direction_diag[direction_diag["direction"] == "売り"]
                        if not buy_diag.empty and not sell_diag.empty:
                            buy_row = buy_diag.iloc[0]
                            sell_row = sell_diag.iloc[0]
                            leader_row = buy_row if float(buy_row["1日平均TR"]) >= float(sell_row["1日平均TR"]) else sell_row
                            trailer_row = sell_row if leader_row["direction"] == "買い" else buy_row
                            leader_label = "ロング" if leader_row["direction"] == "買い" else "ショート"
                            trailer_label = "ショート" if leader_row["direction"] == "買い" else "ロング"
                            daily_gap = float(leader_row["1日平均TR"]) - float(trailer_row["1日平均TR"])
                            tone = "good" if float(leader_row["1日平均TR"]) > 0 else "warn"
                            if abs(daily_gap) < 1500:
                                overnight_cards.append(
                                    {
                                        "label": "方向",
                                        "title": "ロング / ショート",
                                        "headline": "方向差はまだ小さい",
                                        "body": f"ロング {format_number(float(buy_row['1日平均TR']))}円 / ショート {format_number(float(sell_row['1日平均TR']))}円 で近い水準です。",
                                        "meta": "方向より保有日数やサイズの影響が大きい可能性があります。",
                                        "tone": "neutral",
                                    }
                                )
                            else:
                                overnight_cards.append(
                                    {
                                        "label": "方向",
                                        "title": "ロング / ショート",
                                        "headline": f"{leader_label}持越しの方が効率が良い",
                                        "body": f"{leader_label} 1日平均TR {format_number(float(leader_row['1日平均TR']))}円 / {trailer_label} {format_number(float(trailer_row['1日平均TR']))}円。",
                                        "meta": f"勝率 {float(leader_row['勝率']):.1f}% vs {float(trailer_row['勝率']):.1f}% / サイズ効率 {float(leader_row['サイズ効率']):.2f}% vs {float(trailer_row['サイズ効率']):.2f}%。",
                                        "tone": tone,
                                    }
                                )

                        holding_diag = (
                            overnight_stats_df.groupby("holding_day_bucket", dropna=False, observed=False)
                            .agg(
                                件数=("code", "count"),
                                **{
                                    "1日平均TR": ("avg_daily_tr_pl", "mean"),
                                    "サイズ効率": ("size_efficiency_pct", "mean"),
                                },
                            )
                            .reset_index()
                            .rename(columns={"holding_day_bucket": "保有日数帯"})
                        )
                        holding_diag["保有日数帯"] = holding_diag["保有日数帯"].astype(str)
                        holding_diag = holding_diag[holding_diag["件数"] >= 3].copy()
                        if len(holding_diag) >= 2:
                            best_hold = holding_diag.sort_values("1日平均TR", ascending=False, kind="stable").iloc[0]
                            worst_hold = holding_diag.sort_values("1日平均TR", ascending=True, kind="stable").iloc[0]
                            hold_gap = float(best_hold["1日平均TR"]) - float(worst_hold["1日平均TR"])
                            if hold_gap < 1500:
                                overnight_cards.append(
                                    {
                                        "label": "保有日数",
                                        "title": "持越しの長さ",
                                        "headline": "保有日数の差はまだ小さい",
                                        "body": "どの保有日数帯でも、1日平均TRの差はまだ大きくありません。",
                                        "meta": "もう少し件数が増えると保有日数の癖が見えやすくなります。",
                                        "tone": "neutral",
                                    }
                                )
                            else:
                                tone = "good" if float(best_hold["1日平均TR"]) > 0 else "warn"
                                overnight_cards.append(
                                    {
                                        "label": "保有日数",
                                        "title": "持越しの長さ",
                                        "headline": f"{best_hold['保有日数帯']}の方が効率が良い",
                                        "body": f"{best_hold['保有日数帯']} は 1日平均TR {format_number(float(best_hold['1日平均TR']))}円、{worst_hold['保有日数帯']} は {format_number(float(worst_hold['1日平均TR']))}円です。",
                                        "meta": "短すぎる持越し、または長すぎる持越しのどちらが効率を落としているかを見る軸です。",
                                        "tone": tone,
                                    }
                                )

                        size_diag = (
                            overnight_stats_df.groupby("position_size_bucket", dropna=False, observed=False)
                            .agg(
                                件数=("code", "count"),
                                **{
                                    "サイズ効率": ("size_efficiency_pct", "mean"),
                                    "1日平均TR": ("avg_daily_tr_pl", "mean"),
                                },
                            )
                            .reset_index()
                            .rename(columns={"position_size_bucket": "サイズ帯"})
                        )
                        size_diag["サイズ帯"] = size_diag["サイズ帯"].astype(str)
                        size_diag = size_diag[size_diag["件数"] >= 3].copy()
                        if len(size_diag) >= 2:
                            best_size = size_diag.sort_values("サイズ効率", ascending=False, kind="stable").iloc[0]
                            worst_size = size_diag.sort_values("サイズ効率", ascending=True, kind="stable").iloc[0]
                            size_gap = float(best_size["サイズ効率"]) - float(worst_size["サイズ効率"])
                            if size_gap < 0.5:
                                overnight_cards.append(
                                    {
                                        "label": "サイズ",
                                        "title": "ポジションサイズ",
                                        "headline": "サイズ帯の差はまだ小さい",
                                        "body": "サイズ効率ベースでは、サイズ帯ごとの差はまだ大きくありません。",
                                        "meta": "ロットそのものより方向や保有日数の影響が大きい可能性があります。",
                                        "tone": "neutral",
                                    }
                                )
                            else:
                                tone = "good" if float(best_size["サイズ効率"]) > 0 else "warn"
                                overnight_cards.append(
                                    {
                                        "label": "サイズ",
                                        "title": "ポジションサイズ",
                                        "headline": f"{best_size['サイズ帯']}のサイズ効率が高い",
                                        "body": f"{best_size['サイズ帯']} は サイズ効率 {float(best_size['サイズ効率']):.2f}%、{worst_size['サイズ帯']} は {float(worst_size['サイズ効率']):.2f}% です。",
                                        "meta": f"1日平均TR は {format_number(float(best_size['1日平均TR']))}円 vs {format_number(float(worst_size['1日平均TR']))}円。",
                                        "tone": tone,
                                    }
                                )

                        if overnight_cards:
                            overnight_summary_text = " / ".join(
                                card["headline"] for card in overnight_cards[:3]
                            )

                    overnight_mode_caption = (
                        f"統計モード: {overnight_stats_mode_label} "
                        f"(全{format_number(overnight_count)}件 / 継続中 {format_number(ongoing_overnight_count)}件)"
                    )
                    overnight_kpi_html = (
                        f'<div class="trend-kpi-row">'
                        f'<div class="trend-kpi accent-pl">'
                        f'<div class="trend-kpi-label">勝率</div>'
                        f'<div class="trend-kpi-value">{overnight_win_rate:.1f}%</div>'
                        f'<div class="trend-kpi-sub">統計対象 {format_number(overnight_stats_count)}件</div>'
                        f"</div>"
                        f'<div class="trend-kpi accent-real">'
                        f'<div class="trend-kpi-label">1持越し平均TR</div>'
                        f'<div class="trend-kpi-value">{format_man_yen(avg_overnight_tr_pl)}</div>'
                        f'<div class="trend-kpi-sub">保有期間TR合計の平均</div>'
                        f"</div>"
                        f'<div class="trend-kpi accent-eval">'
                        f'<div class="trend-kpi-label">1日平均TR</div>'
                        f'<div class="trend-kpi-value">{format_man_yen(avg_overnight_daily_tr)}</div>'
                        f'<div class="trend-kpi-sub">持越しごとの日次TR平均</div>'
                        f"</div>"
                        f'<div class="trend-kpi accent-val">'
                        f'<div class="trend-kpi-label">サイズ効率</div>'
                        f'<div class="trend-kpi-value">{avg_overnight_size_eff:.2f}%</div>'
                        f'<div class="trend-kpi-sub">TR / 平均サイズ</div>'
                        f"</div>"
                        f"</div>"
                    )
                    (
                        overnight_overview_tab,
                        overnight_holding_tab,
                        overnight_style_tab,
                        overnight_detail_tab,
                    ) = st.tabs(["要約", "保有日数", "サイズ・方向", "継続中 / 明細"])

                    with overnight_overview_tab:
                        st.caption(overnight_mode_caption)
                        if overnight_summary_text:
                            st.markdown(
                                f'<div class="trade-context-note">要約: {_escape_html(overnight_summary_text)}</div>',
                                unsafe_allow_html=True,
                            )
                        st.markdown(overnight_kpi_html, unsafe_allow_html=True)
                        if overnight_cards:
                            render_trade_diagnostic_cards(overnight_cards)
                        else:
                            st.caption("件数が少ないため、傾向の自動要約はまだ表示していません。")

                        st.markdown('<div class="trend-section-title">持越しの勝ち / 負け</div>', unsafe_allow_html=True)
                        if overnight_outcome_summary.empty:
                            st.caption("このモードで集計できる持越しがまだありません。")
                        else:
                            render_table(overnight_outcome_summary, "overnight_outcome_summary.csv", "持越し勝敗CSV")

                    with overnight_holding_tab:
                        st.caption("保有日数ごとの効率差と、ロング / ショートでのズレを確認できます。")
                        holding_top_left, holding_top_right = st.columns(2)
                        with holding_top_left:
                            st.markdown('<div class="trend-section-title">保有日数別の1日平均TR</div>', unsafe_allow_html=True)
                            if overnight_bucket_summary.empty:
                                st.caption("このモードで保有日数別に集計できる持越しがまだありません。")
                            else:
                                fig_overnight_bucket = go.Figure()
                                overnight_colors = [
                                    "rgba(5,150,105,0.72)" if value > 0 else "rgba(220,38,38,0.72)"
                                    if value < 0 else "rgba(148,163,184,0.72)"
                                    for value in overnight_bucket_summary["1日平均TR"]
                                ]
                                fig_overnight_bucket.add_trace(
                                    go.Bar(
                                        x=overnight_bucket_summary["保有日数帯"],
                                        y=overnight_bucket_summary["1日平均TR"] / 10000,
                                        name="1日平均TR",
                                        marker_color=overnight_colors,
                                        hovertemplate="%{y:,.2f}万円<extra></extra>",
                                    )
                                )
                                apply_man_axis(fig_overnight_bucket, height=300)
                                st.plotly_chart(fig_overnight_bucket, width="stretch", config={"displayModeBar": False})

                        with holding_top_right:
                            st.markdown('<div class="trend-section-title">保有日数別 集計</div>', unsafe_allow_html=True)
                            if overnight_bucket_summary.empty:
                                st.caption("このモードで保有日数別に集計できる持越しがまだありません。")
                            else:
                                overnight_bucket_display = overnight_bucket_summary.copy()
                                overnight_bucket_display["勝率"] = overnight_bucket_display["勝率"].map(
                                    lambda value: f"{value:.1f}%"
                                )
                                render_table(
                                    overnight_bucket_display,
                                    "overnight_holding_day_summary.csv",
                                    "保有日数別CSV",
                                )

                        holding_bottom_left, holding_bottom_right = st.columns(2)
                        with holding_bottom_left:
                            st.markdown('<div class="trend-section-title">保有日数別 × ロング/ショート の1日平均TR</div>', unsafe_allow_html=True)
                            if overnight_bucket_direction_summary.empty:
                                st.caption("このモードで方向別に保有日数を比較できる持越しがまだありません。")
                            else:
                                fig_overnight_bucket_dir = go.Figure()
                                direction_colors = {"買い": "rgba(194,65,12,0.72)", "売り": "rgba(2,132,199,0.72)"}
                                for direction in ["買い", "売り"]:
                                    sub = overnight_bucket_direction_summary[
                                        overnight_bucket_direction_summary["方向"] == direction
                                    ]
                                    if sub.empty:
                                        continue
                                    fig_overnight_bucket_dir.add_trace(
                                        go.Bar(
                                            x=sub["保有日数帯"],
                                            y=sub["1日平均TR"] / 10000,
                                            name=direction,
                                            marker_color=direction_colors.get(direction, "rgba(148,163,184,0.72)"),
                                            hovertemplate="%{y:,.2f}万円<extra></extra>",
                                        )
                                    )
                                fig_overnight_bucket_dir.update_layout(barmode="group")
                                apply_man_axis(fig_overnight_bucket_dir, height=300)
                                st.plotly_chart(
                                    fig_overnight_bucket_dir,
                                    width="stretch",
                                    config={"displayModeBar": False},
                                )

                        with holding_bottom_right:
                            st.markdown('<div class="trend-section-title">保有日数別 × ロング/ショート 集計</div>', unsafe_allow_html=True)
                            if overnight_bucket_direction_summary.empty:
                                st.caption("このモードで方向別に保有日数を集計できる持越しがまだありません。")
                            else:
                                overnight_bucket_direction_display = overnight_bucket_direction_summary.copy()
                                overnight_bucket_direction_display["勝率"] = overnight_bucket_direction_display["勝率"].map(
                                    lambda value: f"{value:.1f}%"
                                )
                                render_table(
                                    overnight_bucket_direction_display,
                                    "overnight_holding_day_direction_summary.csv",
                                    "保有日数×方向CSV",
                                )

                    with overnight_style_tab:
                        st.caption("サイズ感と方向の相性を並べて見て、どこで効率が落ちるかを確認できます。")
                        style_top_left, style_top_right = st.columns(2)
                        with style_top_left:
                            st.markdown('<div class="trend-section-title">ポジションサイズ別のサイズ効率</div>', unsafe_allow_html=True)
                            if overnight_size_summary.empty:
                                st.caption("このモードでサイズ別に集計できる持越しがまだありません。")
                            else:
                                fig_overnight_size = go.Figure()
                                overnight_size_colors = [
                                    "rgba(5,150,105,0.72)" if value > 0 else "rgba(220,38,38,0.72)"
                                    if value < 0 else "rgba(148,163,184,0.72)"
                                    for value in pd.to_numeric(overnight_size_summary["サイズ効率(%)"], errors="coerce").fillna(0)
                                ]
                                fig_overnight_size.add_trace(
                                    go.Bar(
                                        x=overnight_size_summary["サイズ帯"],
                                        y=pd.to_numeric(overnight_size_summary["サイズ効率(%)"], errors="coerce").fillna(0),
                                        name="サイズ効率",
                                        marker_color=overnight_size_colors,
                                        hovertemplate="%{y:,.2f}%<extra></extra>",
                                    )
                                )
                                fig_overnight_size.update_layout(
                                    template="none",
                                    paper_bgcolor="rgba(0,0,0,0)",
                                    plot_bgcolor="rgba(255,255,255,0.42)",
                                    font=dict(family="system-ui, -apple-system, sans-serif", size=12, color="#374151"),
                                    margin=dict(l=72, r=18, t=18, b=46),
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="left",
                                        x=0,
                                        font=dict(size=11, color="#6b7280"),
                                        bgcolor="rgba(0,0,0,0)",
                                    ),
                                    xaxis=dict(
                                        showgrid=False,
                                        tickangle=-25,
                                        tickfont=dict(size=11, color="#94a3b8"),
                                        linecolor="rgba(148,163,184,0.16)",
                                    ),
                                    yaxis=dict(
                                        showgrid=True,
                                        gridcolor="rgba(148,163,184,0.12)",
                                        zeroline=True,
                                        zerolinecolor="rgba(148,163,184,0.2)",
                                        tickfont=dict(size=12, color="#64748b"),
                                        ticksuffix="%",
                                    ),
                                    hovermode="x unified",
                                    hoverlabel=dict(bgcolor="white", font_size=12, bordercolor="#e5e7eb"),
                                    height=300,
                                )
                                st.plotly_chart(fig_overnight_size, width="stretch", config={"displayModeBar": False})

                        with style_top_right:
                            st.markdown('<div class="trend-section-title">方向別 オーバーナイト傾向</div>', unsafe_allow_html=True)
                            if overnight_direction_summary.empty:
                                st.caption("このモードで方向別に集計できる持越しがまだありません。")
                            else:
                                render_table(overnight_direction_summary, "overnight_direction_summary.csv", "方向別持越しCSV")

                        st.markdown('<div class="trend-section-title">ポジションサイズ別 集計</div>', unsafe_allow_html=True)
                        if overnight_size_summary.empty:
                            st.caption("このモードでサイズ別に集計できる持越しがまだありません。")
                        else:
                            render_table(overnight_size_summary, "overnight_size_summary.csv", "サイズ別持越しCSV")

                    with overnight_detail_tab:
                        st.caption("保有中の建玉と、全持越しの明細をここでまとめて確認できます。")
                        st.markdown('<div class="trend-section-title">継続中の持越し</div>', unsafe_allow_html=True)
                        if ongoing_overnight_display.empty:
                            st.caption("現在継続中の持越しはありません。")
                        else:
                            render_table(ongoing_overnight_display, "ongoing_overnight_positions.csv", "継続中持越しCSV")

                        st.markdown('<div class="trend-section-title">持越し明細</div>', unsafe_allow_html=True)
                        render_table(
                            overnight_detail_display[
                                [
                                    "建玉日",
                                    "終了/最新観測日",
                                    "コード",
                                    "銘柄名",
                                    "方向",
                                    "状態",
                                    "保有日数",
                                    "観測営業日数",
                                    "TR損益合計",
                                    "実現損益合計",
                                    "1日平均TR",
                                    "平均サイズ",
                                    "最大サイズ",
                                    "サイズ帯",
                                    "最終評価損益",
                                    "結果",
                                    "終了理由",
                                ]
                            ],
                            "overnight_hold_detail.csv",
                            "持越し明細CSV",
                        )

                streak_left, streak_right = st.columns(2)
                with streak_left:
                    st.markdown('<div class="trend-section-title">連敗後のサイズ変化</div>', unsafe_allow_html=True)
                    if streak_summary.empty:
                        st.caption("連敗後の分析に使えるデイトレ集計がまだありません。")
                    else:
                        streak_display = streak_summary.rename(columns={"streak_bucket": "状態"})
                        streak_display["勝率"] = streak_display["勝率"].map(lambda value: f"{value:.1f}%")
                        render_table(streak_display, "trade_habit_streak.csv", "連敗後分析CSV")

                with streak_right:
                    st.markdown('<div class="trend-section-title">前場 / 後場の収益差</div>', unsafe_allow_html=True)
                    if session_edge.empty:
                        st.caption("前場 / 後場比較に使えるデイトレ集計がまだありません。")
                    else:
                        fig_session_edge = go.Figure()
                        colors = [
                            "rgba(5,150,105,0.72)" if value > 0 else "rgba(220,38,38,0.72)" if value < 0 else "rgba(148,163,184,0.72)"
                            for value in session_edge["平均損益"]
                        ]
                        fig_session_edge.add_trace(
                            go.Bar(
                                x=session_edge["session_group"],
                                y=session_edge["平均損益"] / 10000,
                                name="平均損益",
                                marker_color=colors,
                                hovertemplate="%{y:,.2f}万円<extra></extra>",
                            )
                        )
                        apply_man_axis(fig_session_edge, height=300)
                        st.plotly_chart(fig_session_edge, width="stretch", config={"displayModeBar": False})
                        edge_display = session_edge.rename(columns={"session_group": "時間帯"})
                        edge_display["勝率"] = edge_display["勝率"].map(lambda value: f"{value:.1f}%")
                        render_table(edge_display, "trade_habit_session_edge.csv", "前場後場分析CSV")

                if not session_edge_direction.empty or not holding_bucket_direction_summary.empty:
                    st.markdown('<div class="trend-section-title">L/Sで見る時間分析</div>', unsafe_allow_html=True)
                    st.caption("前場 / 後場と保有時間帯を、買い先行と売り先行で切り分けて見られます。")
                    ls_session_tab, ls_holding_tab = st.tabs(["前場 / 後場 × L/S", "保有時間 × L/S"])

                    with ls_session_tab:
                        session_ls_left, session_ls_right = st.columns(2)
                        with session_ls_left:
                            st.markdown('<div class="trend-section-title">前場 / 後場 × ロング/ショート の平均損益</div>', unsafe_allow_html=True)
                            if session_edge_direction.empty:
                                st.caption("時間帯をL/Sで分けられるデイトレ集計がまだありません。")
                            else:
                                fig_session_edge_dir = go.Figure()
                                direction_colors = {"買い": "rgba(194,65,12,0.72)", "売り": "rgba(2,132,199,0.72)"}
                                for direction in ["買い", "売り"]:
                                    sub = session_edge_direction[session_edge_direction["方向"] == direction]
                                    if sub.empty:
                                        continue
                                    fig_session_edge_dir.add_trace(
                                        go.Bar(
                                            x=sub["時間帯"],
                                            y=sub["平均損益"] / 10000,
                                            name=direction,
                                            marker_color=direction_colors.get(direction, "rgba(148,163,184,0.72)"),
                                            hovertemplate="%{y:,.2f}万円<extra></extra>",
                                        )
                                    )
                                fig_session_edge_dir.update_layout(barmode="group")
                                apply_man_axis(fig_session_edge_dir, height=300)
                                st.plotly_chart(
                                    fig_session_edge_dir,
                                    width="stretch",
                                    config={"displayModeBar": False},
                                )

                        with session_ls_right:
                            st.markdown('<div class="trend-section-title">前場 / 後場 × L/S 集計</div>', unsafe_allow_html=True)
                            if session_edge_direction.empty:
                                st.caption("時間帯をL/Sで分けられるデイトレ集計がまだありません。")
                            else:
                                session_edge_direction_display = session_edge_direction.copy()
                                session_edge_direction_display["勝率"] = session_edge_direction_display["勝率"].map(
                                    lambda value: f"{value:.1f}%"
                                )
                                render_table(
                                    session_edge_direction_display,
                                    "trade_habit_session_edge_direction.csv",
                                    "前場後場L/S分析CSV",
                                )

                    with ls_holding_tab:
                        holding_ls_top_left, holding_ls_top_right = st.columns(2)
                        with holding_ls_top_left:
                            st.markdown('<div class="trend-section-title">保有時間帯 × ロング/ショート の平均損益</div>', unsafe_allow_html=True)
                            if holding_bucket_direction_summary.empty:
                                st.caption("保有時間をL/Sで分けられるデイトレ集計がまだありません。")
                            else:
                                fig_holding_dir = go.Figure()
                                direction_colors = {"買い": "rgba(194,65,12,0.72)", "売り": "rgba(2,132,199,0.72)"}
                                for direction in ["買い", "売り"]:
                                    sub = holding_bucket_direction_summary[
                                        holding_bucket_direction_summary["方向"] == direction
                                    ]
                                    if sub.empty:
                                        continue
                                    fig_holding_dir.add_trace(
                                        go.Bar(
                                            x=sub["保有時間帯"],
                                            y=sub["平均損益"] / 10000,
                                            name=direction,
                                            marker_color=direction_colors.get(direction, "rgba(148,163,184,0.72)"),
                                            hovertemplate="%{y:,.2f}万円<extra></extra>",
                                        )
                                    )
                                fig_holding_dir.update_layout(barmode="group")
                                apply_man_axis(fig_holding_dir, height=300)
                                st.plotly_chart(fig_holding_dir, width="stretch", config={"displayModeBar": False})

                        with holding_ls_top_right:
                            st.markdown('<div class="trend-section-title">保有時間帯 × L/S 集計</div>', unsafe_allow_html=True)
                            if holding_bucket_direction_summary.empty:
                                st.caption("保有時間をL/Sで分けられるデイトレ集計がまだありません。")
                            else:
                                holding_bucket_direction_display = holding_bucket_direction_summary.copy()
                                holding_bucket_direction_display["勝率"] = holding_bucket_direction_display["勝率"].map(
                                    lambda value: f"{value:.1f}%"
                                )
                                render_table(
                                    holding_bucket_direction_display,
                                    "trade_habit_holding_bucket_direction.csv",
                                    "保有時間L/S分析CSV",
                                )

                        st.markdown('<div class="trend-section-title">勝ち / 負けの保有時間 × L/S</div>', unsafe_allow_html=True)
                        if holding_outcome_direction_summary.empty:
                            st.caption("勝ち負けをL/Sで分けられるデイトレ集計がまだありません。")
                        else:
                            render_table(
                                holding_outcome_direction_summary,
                                "trade_habit_holding_outcome_direction.csv",
                                "勝敗保有時間L/S分析CSV",
                            )

                summary_left, summary_right = st.columns(2)
                with summary_left:
                    st.markdown('<div class="trend-section-title">L/S デイトレ傾向</div>', unsafe_allow_html=True)
                    if roundtrip_all_df.empty:
                        st.caption("デイトレとして集計できる約定はまだありません。")
                    else:
                        ls_summary = (
                            roundtrip_all_df.groupby("direction", dropna=False)
                            .agg(
                                件数=("code", "count"),
                                勝率=("realized_pl", lambda s: (s > 0).mean() * 100),
                                実現損益合計=("realized_pl", "sum"),
                                平均損益=("realized_pl", "mean"),
                                平均デイトレ量=("daytrade_qty", "mean"),
                                平均回転数=("turnover", "mean"),
                            )
                            .reset_index()
                            .rename(columns={"direction": "方向"})
                            .sort_values("方向")
                            .reset_index(drop=True)
                        )
                        ls_summary["勝率"] = ls_summary["勝率"].map(lambda value: f"{value:.1f}%")
                        render_table(ls_summary, "trade_habit_ls.csv", "L/S傾向CSV")

                with summary_right:
                    st.markdown('<div class="trend-section-title">執行スタイル傾向</div>', unsafe_allow_html=True)
                    if habit_profile_all.empty:
                        st.caption("約定から執行スタイルを集計できていません。")
                    else:
                        style_order = {"単発": 0, "分割": 1, "多段": 2}
                        style_summary = (
                            habit_profile_all.groupby("execution_style", dropna=False)
                            .agg(
                                銘柄日数=("code", "count"),
                                平均約定数=("executions", "mean"),
                                中央保有時間=("holding_minutes", "median"),
                                平均売買代金=("total_notional", "mean"),
                            )
                            .reset_index()
                            .rename(columns={"execution_style": "執行スタイル"})
                        )
                        style_summary["_sort"] = style_summary["執行スタイル"].map(style_order).fillna(99)
                        style_summary = style_summary.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)
                        render_table(style_summary, "trade_habit_style.csv", "執行スタイルCSV")

                st.markdown('<div class="trend-section-title">よく触る銘柄</div>', unsafe_allow_html=True)
                if roundtrip_all_df.empty:
                    fallback_symbol_df = filtered_all_trades.copy()
                    fallback_symbol_df["notional"] = fallback_symbol_df["price"] * fallback_symbol_df["quantity"]
                    symbol_bias = (
                        fallback_symbol_df.groupby(["code", "name"], dropna=False)
                        .agg(約定件数=("id", "count"), 約定代金=("notional", "sum"))
                        .reset_index()
                        .rename(columns={"code": "コード", "name": "銘柄名"})
                        .sort_values("約定件数", ascending=False, kind="stable")
                        .head(12)
                        .reset_index(drop=True)
                    )
                else:
                    symbol_bias = (
                        roundtrip_all_df.groupby(["code", "name"], dropna=False)
                        .agg(
                            デイトレ回数=("trade_date", "count"),
                            実現損益合計=("realized_pl", "sum"),
                            平均損益=("realized_pl", "mean"),
                        )
                        .reset_index()
                        .rename(columns={"code": "コード", "name": "銘柄名"})
                        .sort_values(["デイトレ回数", "実現損益合計"], ascending=[False, False], kind="stable")
                        .head(12)
                        .reset_index(drop=True)
                    )
                render_table(symbol_bias, "trade_habit_symbols.csv", "頻出銘柄CSV")

            with daily_tab:
                st.markdown(
                    '<div class="trade-context-note">'
                    'ここでは選んだ1日を掘り下げます。'
                    '上のダッシュボードで気になった日を選んで、銘柄別とデイトレ集計に降りる使い方がおすすめです。'
                    "</div>",
                    unsafe_allow_html=True,
                )
                st.caption(f"対象期間: {trade_range_label}")
                selected_trade_date = st.selectbox("対象日", filtered_trade_dates, key="view_trade_date")
                trades_df = load_trades_by_date(selected_trade_date)
                if trades_df.empty:
                    st.info("この日の約定履歴はありません。")
                else:
                    total_trades = len(trades_df)
                    buy_df = trades_df[trades_df["side"] == "買"]
                    sell_df = trades_df[trades_df["side"] == "売"]
                    buy_notional = (buy_df["price"] * buy_df["quantity"]).sum()
                    sell_notional = (sell_df["price"] * sell_df["quantity"]).sum()
                    symbol_count = trades_df["code"].nunique()
                    roundtrip_df = build_intraday_roundtrips(trades_df)

                    k1, k2, k3, k4 = st.columns(4)
                    k1.metric("約定件数", format_number(total_trades))
                    k2.metric("銘柄数", format_number(symbol_count))
                    k3.metric("買 約定代金", format_man_yen(buy_notional))
                    k4.metric("売 約定代金", format_man_yen(sell_notional))

                    st.markdown('<div class="trend-section-title">銘柄別 集計</div>', unsafe_allow_html=True)
                    trades_df["notional"] = trades_df["price"] * trades_df["quantity"]
                    by_symbol = (
                        trades_df.groupby(["code", "name", "market", "side"], dropna=False)
                        .agg(件数=("id", "count"), 数量=("quantity", "sum"), 約定代金=("notional", "sum"),
                             平均値段=("price", "mean"))
                        .reset_index()
                        .rename(columns={"code": "コード", "name": "銘柄名", "market": "市場", "side": "売買"})
                    )
                    by_symbol["平均値段"] = by_symbol["平均値段"].round(2)
                    by_symbol = by_symbol.sort_values(["コード", "売買"], kind="stable").reset_index(drop=True)
                    render_table(by_symbol, f"trades_by_symbol_{selected_trade_date}.csv", "銘柄別集計CSV")

                    st.markdown('<div class="trend-section-title">デイトレ集計</div>', unsafe_allow_html=True)
                    if roundtrip_df.empty:
                        st.caption("同日・同銘柄で買いと売りが両方ある約定はありません。")
                    else:
                        daytrade_qty_total = int(roundtrip_df["daytrade_qty"].sum())
                        daytrade_pl_total = float(roundtrip_df["realized_pl"].sum())
                        daytrade_turnover_total = int(roundtrip_df["turnover"].sum())
                        daytrade_symbol_count = int(roundtrip_df["code"].nunique())

                        d1, d2, d3, d4 = st.columns(4)
                        d1.metric("デイトレ銘柄数", format_number(daytrade_symbol_count))
                        d2.metric("デイトレ数量", format_number(daytrade_qty_total))
                        d3.metric("デイトレ実現損益", format_man_yen(daytrade_pl_total))
                        d4.metric("回転数", format_number(daytrade_turnover_total))

                        roundtrip_display = roundtrip_df.rename(
                            columns={
                                "trade_date": "約定日",
                                "code": "コード",
                                "name": "銘柄名",
                                "direction": "方向",
                                "daytrade_qty": "デイトレ量",
                                "realized_pl": "実現損益",
                                "avg_buy_price": "平均買単価",
                                "avg_sell_price": "平均売単価",
                                "turnover": "回転数",
                            }
                        )
                        roundtrip_display["方向"] = roundtrip_display["方向"].map({"買い": "L", "売り": "S"}).fillna(roundtrip_display["方向"])
                        render_table(
                            roundtrip_display[["約定日", "コード", "銘柄名", "方向", "デイトレ量", "実現損益", "平均買単価", "平均売単価", "回転数"]],
                            f"roundtrips_{selected_trade_date}.csv",
                            "デイトレ集計CSV",
                        )
                        st.caption("方向は当日の最初の約定を基準に分類しています。")

                    st.markdown('<div class="trend-section-title">約定明細</div>', unsafe_allow_html=True)
                    detail = trades_df.rename(columns={
                        "executed_at": "約定日時",
                        "code": "コード",
                        "name": "銘柄名",
                        "market": "市場",
                        "side": "売買",
                        "price": "約定値段",
                        "quantity": "約定数量",
                        "notional": "約定代金",
                        "trade_no": "約定番号",
                        "receipt_no": "受付番号",
                        "fill_flag": "出来",
                        "price_sign": "値段符号",
                    })
                    detail_cols = ["約定日時", "コード", "銘柄名", "市場", "売買", "約定値段",
                                   "約定数量", "約定代金", "約定番号", "受付番号", "値段符号"]
                    render_table(detail[[c for c in detail_cols if c in detail.columns]],
                                 f"trades_{selected_trade_date}.csv", "明細CSV")

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
            st.text_area("コピー用サマリ", value=copy_text, height=280)

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
                st.text_area("銘柄一覧 (コピー用TSV)", value=tsv_text, height=200)

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

