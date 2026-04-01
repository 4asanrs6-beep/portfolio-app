from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from portfolio_app.parser import ParsedPosition


DB_PATH = Path("portfolio.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    raw_text TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    note TEXT
);

CREATE TABLE IF NOT EXISTS position_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_id INTEGER NOT NULL,
    snapshot_date TEXT NOT NULL,
    id_name TEXT NOT NULL,
    account_category TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    product_type TEXT,
    index_flag TEXT,
    currency TEXT,
    strategy_key TEXT NOT NULL,
    direction TEXT NOT NULL,
    terminal_id TEXT,
    book_price REAL,
    last_price REAL,
    change_pct REAL,
    tr_pl REAL,
    realized_pl REAL,
    unrealized_pl REAL,
    net_qty INTEGER NOT NULL DEFAULT 0,
    position_market_value REAL,
    net_pl_rate REAL,
    book_value_net REAL,
    offset_trade REAL,
    buy_qty INTEGER NOT NULL DEFAULT 0,
    sell_qty INTEGER NOT NULL DEFAULT 0,
    net_pl REAL,
    strike_price REAL,
    contract_month TEXT,
    call_put TEXT,
    sell_price REAL,
    buy_price REAL,
    delta_qty INTEGER NOT NULL DEFAULT 0,
    buy_effective_qty INTEGER NOT NULL DEFAULT 0,
    sell_effective_qty INTEGER NOT NULL DEFAULT 0,
    buy_limit_amount REAL,
    sell_limit_amount REAL,
    buy_fill_amount REAL,
    sell_fill_amount REAL,
    buy_to_cover_required_qty INTEGER NOT NULL DEFAULT 0,
    cancel_required_qty INTEGER NOT NULL DEFAULT 0,
    pl_rate_exceeded REAL,
    margin_new_sell_fill_amount REAL,
    margin_new_sell_limit_amount REAL,
    prev_day_margin_new_sell_amount REAL,
    margin_new_sell_amount_total REAL,
    delta_value REAL,
    gamma_value REAL,
    cash_position_amount REAL,
    sell_pos_count INTEGER NOT NULL DEFAULT 0,
    buy_pos_count INTEGER NOT NULL DEFAULT 0,
    sell_fill_count INTEGER NOT NULL DEFAULT 0,
    buy_fill_count INTEGER NOT NULL DEFAULT 0,
    prev_day_diff REAL,
    today_sell_price REAL,
    today_buy_price REAL,
    today_margin_new_sell_qty INTEGER NOT NULL DEFAULT 0,
    today_tr_pl_ds REAL,
    today_tr_pl_ev REAL,
    today_tr_pl_jpy REAL,
    today_tr_pl_foreign REAL,
    position_market_value_jpy REAL,
    position_market_value_foreign REAL,
    base_fx_rate REAL,
    live_fx_rate REAL,
    fx_book_rate REAL,
    margin_book_value_total REAL,
    margin_short_open_qty INTEGER NOT NULL DEFAULT 0,
    margin_long_open_qty INTEGER NOT NULL DEFAULT 0,
    raw_values TEXT NOT NULL,
    source_block TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (import_id) REFERENCES imports (id)
);

CREATE INDEX IF NOT EXISTS idx_position_snapshot_date
ON position_snapshots (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_position_compare
ON position_snapshots (
    snapshot_date,
    id_name,
    code,
    account_category,
    product_type,
    strategy_key
);

CREATE TABLE IF NOT EXISTS risk_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    month TEXT NOT NULL UNIQUE,
    gross_limit REAL,
    net_limit REAL,
    futures_limit REAL,
    monthly_loss_limit REAL,
    note TEXT,
    updated_at TEXT NOT NULL
);
"""

REQUIRED_COLUMNS = {
    "id_name",
    "account_category",
    "code",
    "name",
    "product_type",
    "currency",
    "strategy_key",
    "direction",
    "book_price",
    "last_price",
    "tr_pl",
    "realized_pl",
    "unrealized_pl",
    "net_qty",
    "position_market_value_jpy",
    "raw_values",
    "source_block",
}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _current_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}


def init_db() -> None:
    with get_connection() as conn:
        recreate = False
        if _table_exists(conn, "position_snapshots"):
            existing = _current_columns(conn, "position_snapshots")
            recreate = not REQUIRED_COLUMNS.issubset(existing)
        if recreate:
            conn.executescript(
                """
                DROP TABLE IF EXISTS position_snapshots;
                DROP TABLE IF EXISTS imports;
                """
            )
        conn.executescript(SCHEMA_SQL)


def replace_snapshot(snapshot_date: str, raw_text: str, positions: list[ParsedPosition], note: str = "") -> int:
    imported_at = datetime.now().isoformat(timespec="seconds")
    with get_connection() as conn:
        conn.execute("DELETE FROM position_snapshots WHERE snapshot_date = ?", (snapshot_date,))
        conn.execute("DELETE FROM imports WHERE snapshot_date = ?", (snapshot_date,))
        cursor = conn.execute(
            """
            INSERT INTO imports (snapshot_date, imported_at, raw_text, record_count, note)
            VALUES (?, ?, ?, ?, ?)
            """,
            (snapshot_date, imported_at, raw_text, len(positions), note),
        )
        import_id = int(cursor.lastrowid)
        conn.executemany(
            """
            INSERT INTO position_snapshots (
                import_id, snapshot_date, id_name, account_category, code, name, product_type, index_flag,
                currency, strategy_key, direction, terminal_id, book_price, last_price, change_pct, tr_pl,
                realized_pl, unrealized_pl, net_qty, position_market_value, net_pl_rate, book_value_net,
                offset_trade, buy_qty, sell_qty, net_pl, strike_price, contract_month, call_put, sell_price,
                buy_price, delta_qty, buy_effective_qty, sell_effective_qty, buy_limit_amount,
                sell_limit_amount, buy_fill_amount, sell_fill_amount, buy_to_cover_required_qty,
                cancel_required_qty, pl_rate_exceeded, margin_new_sell_fill_amount,
                margin_new_sell_limit_amount, prev_day_margin_new_sell_amount, margin_new_sell_amount_total,
                delta_value, gamma_value, cash_position_amount, sell_pos_count, buy_pos_count,
                sell_fill_count, buy_fill_count, prev_day_diff, today_sell_price, today_buy_price,
                today_margin_new_sell_qty, today_tr_pl_ds, today_tr_pl_ev, today_tr_pl_jpy,
                today_tr_pl_foreign, position_market_value_jpy, position_market_value_foreign,
                base_fx_rate, live_fx_rate, fx_book_rate, margin_book_value_total,
                margin_short_open_qty, margin_long_open_qty, raw_values, source_block, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    import_id,
                    snapshot_date,
                    p.id_name,
                    p.account_category,
                    p.code,
                    p.name,
                    p.product_type,
                    p.index_flag,
                    p.currency,
                    p.strategy_key,
                    p.direction,
                    p.terminal_id,
                    p.book_price,
                    p.last_price,
                    p.change_pct,
                    p.tr_pl,
                    p.realized_pl,
                    p.unrealized_pl,
                    p.net_qty,
                    p.position_market_value,
                    p.net_pl_rate,
                    p.book_value_net,
                    p.offset_trade,
                    p.buy_qty,
                    p.sell_qty,
                    p.net_pl,
                    p.strike_price,
                    p.contract_month,
                    p.call_put,
                    p.sell_price,
                    p.buy_price,
                    p.delta_qty,
                    p.buy_effective_qty,
                    p.sell_effective_qty,
                    p.buy_limit_amount,
                    p.sell_limit_amount,
                    p.buy_fill_amount,
                    p.sell_fill_amount,
                    p.buy_to_cover_required_qty,
                    p.cancel_required_qty,
                    p.pl_rate_exceeded,
                    p.margin_new_sell_fill_amount,
                    p.margin_new_sell_limit_amount,
                    p.prev_day_margin_new_sell_amount,
                    p.margin_new_sell_amount_total,
                    p.delta_value,
                    p.gamma_value,
                    p.cash_position_amount,
                    p.sell_pos_count,
                    p.buy_pos_count,
                    p.sell_fill_count,
                    p.buy_fill_count,
                    p.prev_day_diff,
                    p.today_sell_price,
                    p.today_buy_price,
                    p.today_margin_new_sell_qty,
                    p.today_tr_pl_ds,
                    p.today_tr_pl_ev,
                    p.today_tr_pl_jpy,
                    p.today_tr_pl_foreign,
                    p.position_market_value_jpy,
                    p.position_market_value_foreign,
                    p.base_fx_rate,
                    p.live_fx_rate,
                    p.fx_book_rate,
                    p.margin_book_value_total,
                    p.margin_short_open_qty,
                    p.margin_long_open_qty,
                    p.raw_values,
                    p.source_block,
                    imported_at,
                )
                for p in positions
            ],
        )
    return import_id


def list_snapshot_dates() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT snapshot_date FROM position_snapshots ORDER BY snapshot_date DESC"
        ).fetchall()
    return [row["snapshot_date"] for row in rows]


def list_snapshot_months() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT substr(snapshot_date, 1, 7) AS month FROM position_snapshots ORDER BY month DESC"
        ).fetchall()
    return [row["month"] for row in rows]


def load_snapshot(snapshot_date: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            "SELECT * FROM position_snapshots WHERE snapshot_date = ? ORDER BY id_name, code, strategy_key",
            conn,
            params=(snapshot_date,),
        )


def load_all_snapshots() -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            "SELECT * FROM position_snapshots ORDER BY snapshot_date, id_name, code, strategy_key",
            conn,
        )


def load_snapshots_by_month(month: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT * FROM position_snapshots
            WHERE substr(snapshot_date, 1, 7) = ?
            ORDER BY snapshot_date, id_name, code, strategy_key
            """,
            conn,
            params=(month,),
        )


def load_instrument_history(code: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT * FROM position_snapshots
            WHERE code = ?
            ORDER BY snapshot_date, id_name, strategy_key
            """,
            conn,
            params=(code,),
        )


def load_previous_snapshot(snapshot_date: str) -> pd.DataFrame:
    dates = list_snapshot_dates()
    try:
        index = dates.index(snapshot_date)
    except ValueError:
        return pd.DataFrame()
    if index + 1 >= len(dates):
        return pd.DataFrame()
    return load_snapshot(dates[index + 1])


# ---------------------------------------------------------------------------
# リスク枠 (risk_limits)
# ---------------------------------------------------------------------------

def save_risk_limits(
    month: str,
    gross_limit: float | None,
    net_limit: float | None,
    futures_limit: float | None,
    monthly_loss_limit: float | None,
    note: str = "",
) -> None:
    """月別リスク枠を UPSERT する。"""
    updated_at = datetime.now().isoformat(timespec="seconds")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO risk_limits (month, gross_limit, net_limit, futures_limit, monthly_loss_limit, note, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(month) DO UPDATE SET
                gross_limit = excluded.gross_limit,
                net_limit = excluded.net_limit,
                futures_limit = excluded.futures_limit,
                monthly_loss_limit = excluded.monthly_loss_limit,
                note = excluded.note,
                updated_at = excluded.updated_at
            """,
            (month, gross_limit, net_limit, futures_limit, monthly_loss_limit, note, updated_at),
        )


def load_risk_limits(month: str) -> dict | None:
    """指定月のリスク枠を返す。無ければ None。"""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM risk_limits WHERE month = ?",
            (month,),
        ).fetchone()
    if row is None:
        return None
    return dict(row)


def load_latest_risk_limits(as_of_month: str | None = None) -> dict | None:
    """as_of_month 以前で最新のリスク枠を返す。"""
    with get_connection() as conn:
        if as_of_month:
            row = conn.execute(
                "SELECT * FROM risk_limits WHERE month <= ? ORDER BY month DESC LIMIT 1",
                (as_of_month,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM risk_limits ORDER BY month DESC LIMIT 1",
            ).fetchone()
    if row is None:
        return None
    return dict(row)


def list_risk_limit_months() -> list[str]:
    """リスク枠が登録されている月の一覧。"""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT month FROM risk_limits ORDER BY month DESC"
        ).fetchall()
    return [row["month"] for row in rows]
