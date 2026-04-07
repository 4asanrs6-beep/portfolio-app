"""J-Quants API V2 を使ったマーケットデータ取得 & 指標計算モジュール"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Literal

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ベンチマーク定義
Benchmark = Literal["TOPIX", "日経平均", "グロース250"]

BENCHMARK_LABELS = {
    "TOPIX": "TOPIX",
    "日経平均": "日経平均株価",
    "グロース250": "東証グロース市場250指数",
}

# J-Quants 指数コード
INDEX_CODE_GROWTH250 = "0070"  # 東証グロース市場250指数 (旧 東証マザーズ指数)


def is_equity_code(code: str, product_type: str | None = None) -> bool:
    """個別株/ETF かどうかを判定。先物・指数等を除外。

    product_type が渡された場合はそれを優先 (「株指先」等なら False)。
    渡されない場合はコード形式で推定。
    """
    if product_type is not None:
        pt = str(product_type).strip()
        # 先物・オプション系のキーワード
        if any(k in pt for k in ["先", "OP", "オプション"]):
            return False
        if pt in ("株式", ""):
            return True
        return True  # 不明な場合は株式扱い

    # product_type なし → コード形式で判定
    c = str(code).strip()
    if not c.replace("-", "").replace(".", "").isalnum():
        return False
    # 9桁以上の数字コードは先物
    if c.isdigit() and len(c) > 5:
        return False
    return True


def classify_futures(code: str, name: str) -> str | None:
    """先物の銘柄名から種別を推定。None なら不明。

    銘柄名末尾の限月コード (例: ``-606`` / ``606``) は除去し、商品プレフィックス
    のみで判定する。``Mi`` プレフィックスは「ミニ」の意味なので剥がしてから残り
    部分でマッチさせる。
    """
    import re

    n = str(name).strip()
    # 末尾 3桁の限月 (任意で - を伴う) を除去
    n = re.sub(r"-?\d{3}$", "", n)
    # 先頭の "Mi" (ミニ) を剥がす
    if n.startswith("Mi"):
        n = n[2:]
    nu = n.upper()

    if nu.startswith("G250") or "グロース" in nu or "GROWTH" in nu:
        return "GROWTH"
    if nu.startswith("TPX") or nu.startswith("TOPIX") or nu.startswith("TOS") or "トピックス" in nu:
        return "TOPIX"
    if nu.startswith("N2") or nu.startswith("NK") or nu.startswith("NI") or "日経" in nu:
        return "NK225"
    return None


def compute_futures_cross_betas(
    client: "JQuantsClient",
    periods: list[tuple[str, int]],
) -> dict[str, dict[str, float]]:
    """日経・グロース vs TOPIX のクロスベータを各期間で実測し、先物ベータを構築する。

    Returns: {"TOPIX": {...}, "NK225": {...}, "GROWTH": {...}}
    """
    end_date = datetime.now().date()
    end_str = end_date.isoformat()

    result: dict[str, dict[str, float]] = {"TOPIX": {}, "NK225": {}, "GROWTH": {}}

    for label, days in periods:
        start = (end_date - timedelta(days=days)).isoformat()
        try:
            topix_df = client.get_benchmark_prices("TOPIX", start, end_str)
            nikkei_df = client.get_benchmark_prices("日経平均", start, end_str)
        except Exception:
            topix_df = pd.DataFrame()
            nikkei_df = pd.DataFrame()

        # グロース250 は別途取得 (失敗しても他は処理続行)
        try:
            growth_df = client.get_benchmark_prices("グロース250", start, end_str)
        except Exception as e:
            logger.warning("グロース250(%s) 取得失敗: %s", label, e)
            growth_df = pd.DataFrame()

        if topix_df.empty or nikkei_df.empty:
            continue

        # 日経の対TOPIXベータ
        m_nk_vs_tp = compute_stock_metrics(nikkei_df, topix_df)
        # TOPIXの対日経ベータ
        m_tp_vs_nk = compute_stock_metrics(topix_df, nikkei_df)

        nk_topix_beta = m_nk_vs_tp.get("ベータ")  # 日経先物の TOPIX ベータ
        tp_nikkei_beta = m_tp_vs_nk.get("ベータ")  # TOPIX先物の 日経ベータ

        # TOPIX先物: β(TOPIX)=1.0, β(日経)=実測値
        result["TOPIX"][f"β(T{label})"] = 1.0
        result["TOPIX"][f"β(N{label})"] = tp_nikkei_beta

        # 日経先物: β(TOPIX)=実測値, β(日経)=1.0
        result["NK225"][f"β(T{label})"] = nk_topix_beta
        result["NK225"][f"β(N{label})"] = 1.0

        # グロース250先物: β(TOPIX)・β(日経) ともに実測
        if not growth_df.empty:
            m_gr_vs_tp = compute_stock_metrics(growth_df, topix_df)
            m_gr_vs_nk = compute_stock_metrics(growth_df, nikkei_df)
            gr_topix_beta = m_gr_vs_tp.get("ベータ")
            gr_nikkei_beta = m_gr_vs_nk.get("ベータ")
            if gr_topix_beta is not None:
                result["GROWTH"][f"β(T{label})"] = gr_topix_beta
            if gr_nikkei_beta is not None:
                result["GROWTH"][f"β(N{label})"] = gr_nikkei_beta

    # メイン期間用のβ(TOPIX)も設定
    result["TOPIX"]["β(TOPIX)"] = 1.0
    result["NK225"]["β(TOPIX)"] = result["NK225"].get("β(T12M)") or result["NK225"].get("β(T6M)")
    result["GROWTH"]["β(TOPIX)"] = result["GROWTH"].get("β(T12M)") or result["GROWTH"].get("β(T6M)")

    return result


# ---------------------------------------------------------------------------
# J-Quants API client wrapper
# ---------------------------------------------------------------------------

class JQuantsClient:
    """jquantsapi.ClientV2 の薄いラッパー (レート制限対策 + セッションキャッシュ付き)"""

    # API呼び出し間隔 (秒) — J-Quants Free/Light プランのレート制限対策
    API_INTERVAL = 1.0
    MAX_RETRIES = 5
    RETRY_WAIT = 10  # 429 時の待機秒数

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("JQUANTS_API_KEY", "")
        self._client = None
        self._last_call: float = 0.0
        self._cache: dict[str, pd.DataFrame] = {}
        self._fail_cache: set[str] = set()

    def _throttle(self) -> None:
        """連続呼び出しを抑制。"""
        elapsed = time.time() - self._last_call
        if elapsed < self.API_INTERVAL:
            time.sleep(self.API_INTERVAL - elapsed)
        self._last_call = time.time()

    def _call_with_retry(self, func, *args, **kwargs):
        """429 エラー時にリトライする汎用ラッパー。"""
        for attempt in range(self.MAX_RETRIES):
            self._throttle()
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) and attempt < self.MAX_RETRIES - 1:
                    wait = self.RETRY_WAIT * (attempt + 1)
                    logger.info("429 レート制限 — %d秒待機後リトライ (%d/%d)", wait, attempt + 1, self.MAX_RETRIES)
                    time.sleep(wait)
                    self._last_call = time.time()
                else:
                    raise

    def _get_client(self):
        if self._client is None:
            import jquantsapi
            self._client = jquantsapi.ClientV2(api_key=self.api_key)
        return self._client

    def is_available(self) -> bool:
        if not self.api_key:
            return False
        try:
            self._get_client()
            return True
        except Exception:
            return False

    # --- 株価 -----------------------------------------------------------

    def get_stock_prices(
        self,
        code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """個別銘柄の日足株価を取得。日付は YYYY-MM-DD。先物等は空 DataFrame。"""
        # 9桁以上の数字コードは先物 → スキップ
        c = str(code).strip()
        if c.isdigit() and len(c) > 5:
            return pd.DataFrame()

        cache_key = f"stock_{code}_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        if cache_key in self._fail_cache:
            return pd.DataFrame()

        client = self._get_client()
        try:
            df = self._call_with_retry(
                client.get_eq_bars_daily,
                code=code,
                from_yyyymmdd=start_date.replace("-", ""),
                to_yyyymmdd=end_date.replace("-", ""),
            )
        except Exception:
            self._fail_cache.add(cache_key)
            raise
        column_map = {
            "Date": "date", "Code": "code",
            "O": "open", "H": "high", "L": "low", "C": "close", "Vo": "volume",
            "AdjO": "adj_open", "AdjH": "adj_high", "AdjL": "adj_low",
            "AdjC": "adj_close", "AdjVo": "adj_volume",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        self._cache[cache_key] = df
        return df

    # --- 指数 -----------------------------------------------------------

    def get_index_prices(
        self,
        index_code: str = "0000",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """指数日足を取得。デフォルトは TOPIX (0000)。"""
        cache_key = f"idx_{index_code}_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        client = self._get_client()
        kwargs: dict = {"code": index_code}
        if start_date:
            kwargs["from_yyyymmdd"] = start_date.replace("-", "")
        if end_date:
            kwargs["to_yyyymmdd"] = end_date.replace("-", "")
        df = self._call_with_retry(client.get_idx_bars_daily, **kwargs)
        column_map = {
            "Date": "date", "Code": "index_code",
            "O": "open", "H": "high", "L": "low", "C": "close",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        self._cache[cache_key] = df
        return df

    # --- 日経平均 (yfinance) ----------------------------------------------

    def get_nikkei225_prices(
        self,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """日経平均株価を yfinance 経由で取得。date / close カラムを返す。"""
        cache_key = f"nk225_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        import yfinance as yf

        df = yf.download("^N225", start=start_date, end=end_date, progress=False)
        if df.empty:
            return pd.DataFrame(columns=["date", "close"])

        # yfinance の MultiIndex 対応
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        result = df[["Close"]].reset_index()
        result.columns = ["date", "close"]
        result["date"] = pd.to_datetime(result["date"]).dt.tz_localize(None)
        self._cache[cache_key] = result
        return result

    # --- ベンチマーク統一取得 ---------------------------------------------

    def get_benchmark_prices(
        self,
        benchmark: Benchmark = "TOPIX",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """ベンチマーク種別に応じて指数データを取得。date / close を返す。"""
        if benchmark == "日経平均":
            return self.get_nikkei225_prices(
                start_date or "2020-01-01",
                end_date or datetime.now().date().isoformat(),
            )
        if benchmark == "グロース250":
            return self.get_index_prices(INDEX_CODE_GROWTH250, start_date, end_date)
        # デフォルト TOPIX
        return self.get_index_prices("0000", start_date, end_date)

    # --- 銘柄情報 -------------------------------------------------------

    def get_listed_stocks(self) -> pd.DataFrame:
        if "listed_stocks" in self._cache:
            return self._cache["listed_stocks"]
        client = self._get_client()
        df = self._call_with_retry(client.get_list)
        column_map = {
            "Code": "code", "CoName": "name",
            "S17Nm": "sector_17_name",
            "S33Nm": "sector_33_name",
            "MktNm": "market_name",
            "ScaleCat": "scale_category",
        }
        result = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
        self._cache["listed_stocks"] = result
        return result

    # --- 銘柄情報 (yfinance) ------------------------------------------------

    def get_stock_info(self, code: str) -> dict:
        """yfinance から銘柄の時価総額・需給・バリュエーション情報を取得。"""
        c = str(code).strip()
        if c.isdigit() and len(c) > 5:
            return {}

        cache_key = f"yf_info_{code}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        import yfinance as yf

        # 5桁コード (J-Quants形式: 72030) → 4桁 (7203) に変換
        c = code.strip()
        if len(c) == 5 and c.isdigit():
            c = c[:4]
        ticker = c if c.endswith(".T") else f"{c}.T"
        try:
            info = yf.Ticker(ticker).info
        except Exception as e:
            logger.warning("yfinance info 取得失敗 (%s): %s", ticker, e)
            return {}

        result = {
            "時価総額": info.get("marketCap"),
            "株価": info.get("currentPrice"),
            "β(Yahoo)": info.get("beta"),
            "出来高": info.get("volume"),
            "平均出来高(3M)": info.get("averageVolume"),
            "平均出来高(10D)": info.get("averageDailyVolume10Day"),
            "出来高倍率": (
                round(info["volume"] / info["averageVolume"], 2)
                if info.get("volume") and info.get("averageVolume") and info["averageVolume"] > 0
                else None
            ),
            "発行済株数": info.get("sharesOutstanding"),
            "浮動株数": info.get("floatShares"),
            "機関投資家保有率(%)": (
                round(info["heldPercentInstitutions"] * 100, 1)
                if info.get("heldPercentInstitutions") else None
            ),
            "内部者保有率(%)": (
                round(info["heldPercentInsiders"] * 100, 1)
                if info.get("heldPercentInsiders") else None
            ),
            "PER(実績)": info.get("trailingPE"),
            "PER(予想)": info.get("forwardPE"),
            "PBR": info.get("priceToBook"),
            "配当利回り(%)": (
                round(info["dividendYield"] * 100, 2)
                if isinstance(info.get("dividendYield"), (int, float)) and info["dividendYield"] < 1
                else round(info["dividendYield"], 2) if isinstance(info.get("dividendYield"), (int, float))
                else info.get("dividendYield")
            ),
            "52週高値": info.get("fiftyTwoWeekHigh"),
            "52週安値": info.get("fiftyTwoWeekLow"),
            "50日移動平均": info.get("fiftyDayAverage"),
            "200日移動平均": info.get("twoHundredDayAverage"),
        }
        self._cache[cache_key] = result
        return result

    # --- 信用取引残高 (J-Quants) -------------------------------------------

    def get_margin_balance(self, code: str, weeks: int = 8) -> pd.DataFrame:
        """信用残 (買残・売残・貸借倍率) の週次推移を取得。"""
        c = str(code).strip()
        if c.isdigit() and len(c) > 5:
            return pd.DataFrame()

        cache_key = f"margin_{code}_{weeks}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        if cache_key in self._fail_cache:
            return pd.DataFrame()

        end_date = datetime.now().date()
        start_date = (end_date - timedelta(weeks=weeks)).isoformat()
        end_str = end_date.isoformat()

        code5 = code if len(code) == 5 else code + "0"

        client = self._get_client()
        try:
            df = self._call_with_retry(
                client.get_mkt_margin_interest,
                code=code5,
                from_yyyymmdd=start_date.replace("-", ""),
                to_yyyymmdd=end_str.replace("-", ""),
            )
        except Exception as e:
            logger.warning("信用残取得失敗 (%s): %s", code, e)
            self._fail_cache.add(cache_key)
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        result = df.rename(columns={
            "Date": "日付", "Code": "コード",
            "LongVol": "買残", "ShrtVol": "売残",
            "LongNegVol": "買残(制度)", "ShrtNegVol": "売残(制度)",
            "LongStdVol": "買残(一般)", "ShrtStdVol": "売残(一般)",
        })
        result["日付"] = pd.to_datetime(result["日付"])

        # 貸借倍率 = 買残 / 売残
        result["貸借倍率"] = result.apply(
            lambda r: round(r["買残"] / r["売残"], 2) if r["売残"] and r["売残"] > 0 else None,
            axis=1,
        )

        # 前週比増減
        result["買残増減"] = result["買残"].diff()
        result["売残増減"] = result["売残"].diff()
        result["買残増減率(%)"] = (result["買残"].pct_change() * 100).round(1)
        result["売残増減率(%)"] = (result["売残"].pct_change() * 100).round(1)

        cols = ["日付", "買残", "売残", "貸借倍率", "買残増減", "売残増減", "買残増減率(%)", "売残増減率(%)"]
        result = result[[c for c in cols if c in result.columns]].sort_values("日付")
        self._cache[cache_key] = result
        return result


def compute_price_changes(
    client: JQuantsClient,
    code: str,
) -> dict:
    """直近の騰落率を複数期間で計算。"""
    c = str(code).strip()
    if c.isdigit() and len(c) > 5:
        return {}

    end_date = datetime.now().date()
    start_date = (end_date - timedelta(days=400)).isoformat()
    end_str = end_date.isoformat()

    try:
        df = client.get_stock_prices(code, start_date, end_str)
    except Exception:
        return {}

    if df.empty or "date" not in df.columns:
        return {}

    col = "adj_close" if "adj_close" in df.columns else "close"
    df = df[["date", col]].dropna().sort_values("date").reset_index(drop=True)
    if len(df) < 2:
        return {}

    latest_price = df[col].iloc[-1]
    latest_date = df["date"].iloc[-1]
    result: dict = {"現在値": latest_price}

    periods = [
        ("前日比", 1),
        ("1W", 5),
        ("1M", 21),
        ("3M", 63),
        ("6M", 126),
        ("YTD", None),
        ("1Y", 250),
    ]

    for label, trading_days in periods:
        if label == "YTD":
            year_start = pd.Timestamp(f"{latest_date.year}-01-01")
            past = df[df["date"] <= year_start]
            if past.empty:
                continue
            base_price = past[col].iloc[-1]
        else:
            if trading_days is None:
                continue
            idx = len(df) - 1 - trading_days
            if idx < 0:
                continue
            base_price = df[col].iloc[idx]

        if base_price and base_price != 0:
            change_pct = (latest_price / base_price - 1) * 100
            result[label] = round(change_pct, 2)

    return result


def fetch_portfolio_stock_info(
    client: JQuantsClient,
    codes: list[str],
) -> pd.DataFrame:
    """ポートフォリオ全銘柄の yfinance 情報をまとめて取得。先物等はスキップ。"""
    rows = []
    for code in codes:
        c = str(code).strip()
        if c.isdigit() and len(c) > 5:
            continue
        info = client.get_stock_info(code)
        if info:
            info["コード"] = code[:4] if len(code) == 5 and code.isdigit() else code
            rows.append(info)
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    col_order = ["コード"] + [c for c in df.columns if c != "コード"]
    return df[col_order]


# ---------------------------------------------------------------------------
# 指標計算
# ---------------------------------------------------------------------------

TRADING_DAYS_PER_YEAR = 250
RISK_FREE_RATE = 0.001  # 年率 0.1% (日本10年国債近似)


def compute_stock_metrics(
    stock_prices: pd.DataFrame,
    index_prices: pd.DataFrame,
    period_label: str = "",
) -> dict:
    """個別株と指数の日足調整後終値からリスク指標を計算して dict で返す。

    Returns keys:
        beta, alpha (年率), correlation,
        volatility (年率), index_volatility (年率),
        sharpe_ratio, information_ratio,
        max_drawdown, avg_daily_return (年率換算),
        period_return (%), index_period_return (%),
        tracking_error (年率),
        data_points (日数)
    """
    if stock_prices.empty or index_prices.empty:
        return {}

    # 調整後終値を使う。無ければ close
    stock_col = "adj_close" if "adj_close" in stock_prices.columns else "close"
    index_col = "close"

    stock = stock_prices[["date", stock_col]].dropna().rename(columns={stock_col: "stock_close"})
    index = index_prices[["date", index_col]].dropna().rename(columns={index_col: "index_close"})

    merged = stock.merge(index, on="date", how="inner").sort_values("date").reset_index(drop=True)
    if len(merged) < 20:
        return {}

    stock_close = merged["stock_close"].values.astype(float)
    index_close = merged["index_close"].values.astype(float)

    stock_ret = np.diff(np.log(stock_close))
    index_ret = np.diff(np.log(index_close))

    n = len(stock_ret)
    if n < 10:
        return {}

    # Beta & Alpha (OLS)
    cov = np.cov(stock_ret, index_ret)
    beta = cov[0, 1] / cov[1, 1] if cov[1, 1] != 0 else np.nan
    alpha_daily = np.mean(stock_ret) - beta * np.mean(index_ret)
    alpha_annual = alpha_daily * TRADING_DAYS_PER_YEAR

    # Correlation
    correlation = np.corrcoef(stock_ret, index_ret)[0, 1]

    # Volatility (annualized)
    volatility = np.std(stock_ret, ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR)
    index_volatility = np.std(index_ret, ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR)

    # Returns
    avg_daily = np.mean(stock_ret)
    avg_annual = avg_daily * TRADING_DAYS_PER_YEAR
    period_return = (stock_close[-1] / stock_close[0] - 1) * 100
    index_period_return = (index_close[-1] / index_close[0] - 1) * 100

    # Sharpe ratio
    daily_rf = RISK_FREE_RATE / TRADING_DAYS_PER_YEAR
    excess = stock_ret - daily_rf
    sharpe = (np.mean(excess) / np.std(excess, ddof=1)) * np.sqrt(TRADING_DAYS_PER_YEAR) if np.std(excess, ddof=1) > 0 else np.nan

    # Tracking error & Information ratio
    active_ret = stock_ret - index_ret
    tracking_error = np.std(active_ret, ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR)
    information_ratio = (np.mean(active_ret) * TRADING_DAYS_PER_YEAR) / tracking_error if tracking_error > 0 else np.nan

    # Max drawdown
    cumulative = np.cumprod(1 + (np.exp(stock_ret) - 1))
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = (cumulative - running_max) / running_max
    max_drawdown = float(np.min(drawdowns)) * 100

    return {
        "期間": period_label,
        "データ日数": n,
        "ベータ": round(beta, 4) if not np.isnan(beta) else None,
        "アルファ(年率%)": round(alpha_annual * 100, 2) if not np.isnan(alpha_annual) else None,
        "相関係数": round(correlation, 4) if not np.isnan(correlation) else None,
        "ボラティリティ(年率%)": round(volatility * 100, 2),
        "指数ボラティリティ(年率%)": round(index_volatility * 100, 2),
        "シャープレシオ": round(sharpe, 4) if not np.isnan(sharpe) else None,
        "インフォメーションレシオ": round(information_ratio, 4) if not np.isnan(information_ratio) else None,
        "トラッキングエラー(年率%)": round(tracking_error * 100, 2),
        "期間リターン(%)": round(period_return, 2),
        "指数期間リターン(%)": round(index_period_return, 2),
        "最大ドローダウン(%)": round(max_drawdown, 2),
        "平均日次リターン(年率%)": round(avg_annual * 100, 2),
    }


def compute_multi_period_metrics(
    client: JQuantsClient,
    code: str,
    as_of: str | None = None,
    benchmark: Benchmark = "TOPIX",
) -> pd.DataFrame:
    """複数期間 (1M / 3M / 6M / 1Y / 2Y) のベータ等をまとめて DataFrame で返す。"""
    if as_of is None:
        as_of_date = datetime.now().date()
    else:
        as_of_date = datetime.strptime(as_of, "%Y-%m-%d").date()

    end_str = as_of_date.isoformat()

    periods = [
        ("1M", 30),
        ("3M", 90),
        ("6M", 180),
        ("1Y", 365),
        ("2Y", 730),
    ]

    rows: list[dict] = []
    for label, days in periods:
        start_date = (as_of_date - timedelta(days=days)).isoformat()
        try:
            stock_df = client.get_stock_prices(code, start_date, end_str)
            index_df = client.get_benchmark_prices(benchmark, start_date, end_str)
            metrics = compute_stock_metrics(stock_df, index_df, period_label=label)
            if metrics:
                rows.append(metrics)
        except Exception as e:
            logger.warning("期間 %s の指標取得に失敗 (%s): %s", label, code, e)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index("期間")


def compute_price_chart_data(
    client: JQuantsClient,
    code: str,
    days: int = 365,
    benchmark: Benchmark = "TOPIX",
) -> pd.DataFrame:
    """株価 & ベンチマーク の正規化チャート用データを返す。"""
    end_date = datetime.now().date()
    start_date = (end_date - timedelta(days=days)).isoformat()
    end_str = end_date.isoformat()

    stock_df = client.get_stock_prices(code, start_date, end_str)
    index_df = client.get_benchmark_prices(benchmark, start_date, end_str)

    bm_label = BENCHMARK_LABELS.get(benchmark, benchmark)

    stock_col = "adj_close" if "adj_close" in stock_df.columns else "close"
    stock = stock_df[["date", stock_col]].dropna().rename(columns={stock_col: "stock"})
    index = index_df[["date", "close"]].dropna().rename(columns={"close": bm_label})

    merged = stock.merge(index, on="date", how="inner").sort_values("date").reset_index(drop=True)
    if merged.empty:
        return merged

    # 正規化 (開始日 = 100)
    merged["stock"] = merged["stock"] / merged["stock"].iloc[0] * 100
    merged[bm_label] = merged[bm_label] / merged[bm_label].iloc[0] * 100
    return merged


def compute_rolling_beta(
    client: JQuantsClient,
    code: str,
    window: int = 60,
    days: int = 500,
    benchmark: Benchmark = "TOPIX",
) -> pd.DataFrame:
    """ローリングベータを計算。"""
    end_date = datetime.now().date()
    start_date = (end_date - timedelta(days=days)).isoformat()
    end_str = end_date.isoformat()

    stock_df = client.get_stock_prices(code, start_date, end_str)
    index_df = client.get_benchmark_prices(benchmark, start_date, end_str)

    stock_col = "adj_close" if "adj_close" in stock_df.columns else "close"
    stock = stock_df[["date", stock_col]].dropna().rename(columns={stock_col: "stock"})
    index = index_df[["date", "close"]].dropna().rename(columns={"close": "index"})

    merged = stock.merge(index, on="date", how="inner").sort_values("date").reset_index(drop=True)
    if len(merged) < window + 1:
        return pd.DataFrame()

    stock_ret = np.log(merged["stock"]).diff()
    index_ret = np.log(merged["index"]).diff()

    betas = []
    for i in range(window, len(merged)):
        sr = stock_ret.iloc[i - window + 1 : i + 1].values
        ir = index_ret.iloc[i - window + 1 : i + 1].values
        mask = ~(np.isnan(sr) | np.isnan(ir))
        sr, ir = sr[mask], ir[mask]
        if len(sr) < 20:
            betas.append(np.nan)
            continue
        cov = np.cov(sr, ir)
        betas.append(cov[0, 1] / cov[1, 1] if cov[1, 1] != 0 else np.nan)

    result = merged.iloc[window:].copy().reset_index(drop=True)
    result["rolling_beta"] = betas
    return result[["date", "rolling_beta"]].dropna()


# ---------------------------------------------------------------------------
# ポートフォリオ全体の指標
# ---------------------------------------------------------------------------

def enrich_portfolio_with_market_info(
    snapshot_df: pd.DataFrame,
    client: JQuantsClient,
) -> pd.DataFrame:
    """スナップショットに銘柄マスタ情報 (セクター, 市場区分) を付与する。"""
    if snapshot_df.empty:
        return snapshot_df

    try:
        listed = client.get_listed_stocks()
    except Exception:
        return snapshot_df

    # code は J-Quants 側が5桁 (末尾0)、ポートフォリオ側が4桁の場合がある
    listed = listed[["code", "sector_17_name", "sector_33_name", "market_name", "scale_category"]].drop_duplicates(subset=["code"])

    result = snapshot_df.copy()
    # 4桁コード → 5桁に変換してマージ試行
    result["_code5"] = result["code"].astype(str).str.strip()
    result.loc[result["_code5"].str.len() == 4, "_code5"] = result["_code5"] + "0"
    listed["_code5"] = listed["code"].astype(str).str.strip()

    result = result.merge(
        listed[["_code5", "sector_17_name", "sector_33_name", "market_name", "scale_category"]],
        on="_code5",
        how="left",
    )
    result.drop(columns=["_code5"], inplace=True)
    return result


def compute_portfolio_weights(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    """ポジション時価をもとにウェイトを計算。ロング/ショート別。"""
    if snapshot_df.empty:
        return pd.DataFrame()

    df = snapshot_df.copy()
    df["position_market_value_jpy"] = pd.to_numeric(df["position_market_value_jpy"], errors="coerce").fillna(0)
    df["book_value_net"] = pd.to_numeric(df["book_value_net"], errors="coerce").fillna(0)
    df["abs_market_value"] = df["position_market_value_jpy"].abs()

    total_abs = df["abs_market_value"].sum()
    if total_abs == 0:
        return pd.DataFrame()

    df["weight_pct"] = df["position_market_value_jpy"] / total_abs * 100
    df["abs_weight_pct"] = df["abs_market_value"] / total_abs * 100

    cols = ["code", "name", "direction", "net_qty", "position_market_value_jpy", "book_value_net", "weight_pct", "abs_weight_pct"]
    extra = [c for c in ["product_type", "sector_33_name", "market_name"] if c in df.columns]
    return df[cols + extra].sort_values("abs_weight_pct", ascending=False).reset_index(drop=True)


def compute_sector_breakdown(weighted_df: pd.DataFrame) -> pd.DataFrame:
    """セクター別のウェイト集計。"""
    if weighted_df.empty or "sector_33_name" not in weighted_df.columns:
        return pd.DataFrame()

    return (
        weighted_df.groupby("sector_33_name", dropna=False)
        .agg(
            銘柄数=("code", "nunique"),
            評価額=("position_market_value_jpy", "sum"),
            ウェイト=("abs_weight_pct", "sum"),
        )
        .sort_values("ウェイト", ascending=False)
        .reset_index()
        .rename(columns={"sector_33_name": "セクター"})
    )


def _aggregate_betas(
    valid: pd.DataFrame,
    weighted_df: pd.DataFrame,
    beta_col: str = "ベータ",
) -> dict:
    """ベータ列からポートフォリオ加重ベータを集計するヘルパー。"""
    subset = valid.dropna(subset=[beta_col])
    if subset.empty:
        return {"weighted": None, "long": None, "short": None}

    total_abs = subset["絶対ウェイト(%)"].sum()
    if total_abs == 0:
        return {"weighted": None, "long": None, "short": None}

    weighted_beta = (subset["ウェイト(%)"] / 100 * subset[beta_col]).sum()

    long_s = subset[subset["ウェイト(%)"] > 0]
    short_s = subset[subset["ウェイト(%)"] < 0]
    long_abs = long_s["絶対ウェイト(%)"].sum()
    short_abs = short_s["絶対ウェイト(%)"].sum()

    long_beta = (long_s["絶対ウェイト(%)"] / long_abs * long_s[beta_col]).sum() if long_abs > 0 else 0
    short_beta = (short_s["絶対ウェイト(%)"] / short_abs * short_s[beta_col]).sum() if short_abs > 0 else 0

    return {
        "weighted": round(weighted_beta, 4),
        "long": round(long_beta, 4),
        "short": round(short_beta, 4),
    }


def compute_portfolio_all(
    client: JQuantsClient,
    weighted_df: pd.DataFrame,
    days: int = 365,
) -> dict:
    """TOPIX と日経平均の両方 × 3M/6M/12M でポートフォリオ指標を同時計算。

    stock_metrics DataFrame には:
        β(T3M), β(T6M), β(T12M), β(N3M), β(N6M), β(N12M)
    が入る。ポートフォリオ集計は days パラメータの期間で算出。
    """
    if weighted_df.empty:
        return {}

    end_date = datetime.now().date()
    end_str = end_date.isoformat()

    # ベータ算出期間の定義
    beta_periods = [
        ("3M", 90),
        ("6M", 180),
        ("12M", 365),
    ]

    # 各期間 × 各ベンチマークの指数データを事前取得
    benchmarks: dict[str, dict[str, pd.DataFrame]] = {"T": {}, "N": {}}
    for label, d in beta_periods:
        s = (end_date - timedelta(days=d)).isoformat()
        try:
            benchmarks["T"][label] = client.get_benchmark_prices("TOPIX", s, end_str)
        except Exception as e:
            logger.warning("TOPIX(%s) 取得失敗: %s", label, e)
            benchmarks["T"][label] = pd.DataFrame()
        try:
            benchmarks["N"][label] = client.get_benchmark_prices("日経平均", s, end_str)
        except Exception as e:
            logger.warning("日経平均(%s) 取得失敗: %s", label, e)
            benchmarks["N"][label] = pd.DataFrame()

    # メイン期間 (days) のベンチマーク (リスク指標用)
    main_start = (end_date - timedelta(days=days)).isoformat()
    topix_main = client.get_benchmark_prices("TOPIX", main_start, end_str) if days else pd.DataFrame()

    # 先物のクロスベータを実測
    futures_betas = compute_futures_cross_betas(client, beta_periods)

    # 銘柄別に株価取得 & 各期間×各ベンチマークでベータ計算
    stock_rows: list[dict] = []
    uc_cols = ["code", "name", "weight_pct", "abs_weight_pct", "direction"]
    if "product_type" in weighted_df.columns:
        uc_cols.append("product_type")
    unique_codes = weighted_df[uc_cols].drop_duplicates(subset=["code"])

    # 最長期間分の株価を1回取得すれば全期間で使い回せる
    longest_start = (end_date - timedelta(days=365)).isoformat()

    for _, row in unique_codes.iterrows():
        code = row["code"]
        pt = row.get("product_type")
        if not is_equity_code(code, pt):
            # 先物・指数等 → 実測クロスベータを割り当て
            entry = {
                "コード": code,
                "銘柄名": row["name"],
                "方向": row["direction"],
                "ウェイト(%)": round(row["weight_pct"], 2),
                "絶対ウェイト(%)": round(row["abs_weight_pct"], 2),
            }
            fut_type = classify_futures(code, row["name"])
            if fut_type and fut_type in futures_betas and futures_betas[fut_type]:
                for k, v in futures_betas[fut_type].items():
                    entry[k] = v
                fut_label = {"TOPIX": "TOPIX", "NK225": "日経225", "GROWTH": "グロース250"}.get(fut_type, fut_type)
                entry["備考"] = f"{fut_label}先物"
            else:
                entry["備考"] = "先物(種別不明)"
            stock_rows.append(entry)
            continue
        try:
            stock_df = client.get_stock_prices(code, longest_start, end_str)
        except Exception as e:
            logger.warning("銘柄 %s の株価取得失敗: %s", code, e)
            continue

        entry: dict = {
            "コード": code,
            "銘柄名": row["name"],
            "方向": row["direction"],
            "ウェイト(%)": round(row["weight_pct"], 2),
            "絶対ウェイト(%)": round(row["abs_weight_pct"], 2),
        }

        # 各期間 × 各ベンチマークでベータ算出
        for label, d in beta_periods:
            cutoff = pd.Timestamp(end_date - timedelta(days=d))
            stock_slice = stock_df[stock_df["date"] >= cutoff] if "date" in stock_df.columns else stock_df

            # TOPIX
            idx_t = benchmarks["T"].get(label, pd.DataFrame())
            if not idx_t.empty:
                m = compute_stock_metrics(stock_slice, idx_t)
                entry[f"β(T{label})"] = m.get("ベータ")

            # 日経
            idx_n = benchmarks["N"].get(label, pd.DataFrame())
            if not idx_n.empty:
                m2 = compute_stock_metrics(stock_slice, idx_n)
                entry[f"β(N{label})"] = m2.get("ベータ")

        # メイン期間でのリスク指標 (TOPIX ベース)
        if not topix_main.empty:
            main_cutoff = pd.Timestamp(end_date - timedelta(days=days))
            stock_main = stock_df[stock_df["date"] >= main_cutoff] if "date" in stock_df.columns else stock_df
            mm = compute_stock_metrics(stock_main, topix_main)
            entry["β(TOPIX)"] = mm.get("ベータ")
            entry["ボラティリティ(年率%)"] = mm.get("ボラティリティ(年率%)")
            entry["シャープレシオ"] = mm.get("シャープレシオ")
            entry["期間リターン(%)"] = mm.get("期間リターン(%)")
            entry["最大DD(%)"] = mm.get("最大ドローダウン(%)")

        stock_rows.append(entry)

    if not stock_rows:
        return {}

    sm = pd.DataFrame(stock_rows)

    # --- ポートフォリオ集計 ---
    result: dict = {"stock_metrics": sm, "stock_count": len(sm)}

    # エクスポージャー
    long_w = weighted_df[weighted_df["weight_pct"] > 0]
    short_w = weighted_df[weighted_df["weight_pct"] < 0]
    result["long_weight"] = round(long_w["abs_weight_pct"].sum(), 1)
    result["short_weight"] = round(short_w["abs_weight_pct"].sum(), 1)
    result["net_exposure"] = round(weighted_df["weight_pct"].sum(), 1)
    result["gross_exposure"] = round(weighted_df["abs_weight_pct"].sum(), 1)
    result["long_count"] = int(long_w["code"].nunique())
    result["short_count"] = int(short_w["code"].nunique())

    # 評価額
    total_mv = weighted_df["position_market_value_jpy"].sum()
    long_mv = long_w["position_market_value_jpy"].sum()
    short_mv = short_w["position_market_value_jpy"].sum()
    result["total_market_value"] = round(total_mv)
    result["long_market_value"] = round(long_mv)
    result["short_market_value"] = round(short_mv)

    # 集中度 (上位 N 銘柄の絶対ウェイト合計)
    sorted_abs = sm["絶対ウェイト(%)"].sort_values(ascending=False)
    result["concentration_top3"] = round(sorted_abs.head(3).sum(), 1)
    result["concentration_top5"] = round(sorted_abs.head(5).sum(), 1)

    # 株式のみサブセット
    sm_eq = sm[sm["コード"].apply(is_equity_code)]

    # 加重ベータ (メイン期間 TOPIX) — 全体
    if "β(TOPIX)" in sm.columns:
        tb = _aggregate_betas(sm, weighted_df, "β(TOPIX)")
        result["topix_beta"] = tb["weighted"]
        result["topix_long_beta"] = tb["long"]
        result["topix_short_beta"] = tb["short"]

    # 加重ベータ (各期間 × 各ベンチマーク) — 全体 (先物込み) & 株式のみ
    for prefix, bm_name in [("T", "topix"), ("N", "nikkei")]:
        for label, _ in [("3M", 90), ("6M", 180), ("12M", 365)]:
            col = f"β({prefix}{label})"
            if col in sm.columns:
                # 先物込み
                ab = _aggregate_betas(sm, weighted_df, col)
                result[f"{bm_name}_beta_{label}"] = ab["weighted"]
                # 株式のみ
                ab_eq = _aggregate_betas(sm_eq, weighted_df, col)
                result[f"{bm_name}_beta_{label}_eq"] = ab_eq["weighted"]

    # 加重ボラティリティ & シャープ
    valid_vol = sm.dropna(subset=["ボラティリティ(年率%)"])
    if not valid_vol.empty:
        total_abs_w = valid_vol["絶対ウェイト(%)"].sum()
        if total_abs_w > 0:
            result["weighted_vol"] = round(
                (valid_vol["絶対ウェイト(%)"] / total_abs_w * valid_vol["ボラティリティ(年率%)"]).sum(), 2
            )
    valid_sharpe = sm.dropna(subset=["シャープレシオ"])
    if not valid_sharpe.empty:
        total_abs_w = valid_sharpe["絶対ウェイト(%)"].sum()
        if total_abs_w > 0:
            result["weighted_sharpe"] = round(
                (valid_sharpe["絶対ウェイト(%)"] / total_abs_w * valid_sharpe["シャープレシオ"]).sum(), 4
            )

    # 加重リターン
    valid_ret = sm.dropna(subset=["期間リターン(%)"])
    if not valid_ret.empty:
        total_abs_w = valid_ret["絶対ウェイト(%)"].sum()
        if total_abs_w > 0:
            result["weighted_return"] = round(
                (valid_ret["ウェイト(%)"] / 100 * valid_ret["期間リターン(%)"]).sum(), 2
            )

    # ベスト / ワースト
    if not valid_ret.empty:
        best_idx = valid_ret["期間リターン(%)"].idxmax()
        worst_idx = valid_ret["期間リターン(%)"].idxmin()
        result["best_stock"] = f"{sm.loc[best_idx, 'コード']} {sm.loc[best_idx, '銘柄名']} ({sm.loc[best_idx, '期間リターン(%)']}%)"
        result["worst_stock"] = f"{sm.loc[worst_idx, 'コード']} {sm.loc[worst_idx, '銘柄名']} ({sm.loc[worst_idx, '期間リターン(%)']}%)"

    return result
