
#!/usr/bin/env python3
"""
Quicken TSV -> TradesViz CSV (stocks only)

- Filters to equity trades (Bought/Sold + reinvest buys from ReinvDiv/ReinvLg/ReinvSh)
- underlying = symbol for stock trades
- Emits full dividends to a separate cashflows.csv:
  * Reinvested dividends: amount = "Amount Invested" (fallback: price*shares)
  * Cash dividends & distributions: detect amount from Cash/Amount/Net/DivInc/etc.
  * Broad detection: includes rows with positive dividend amount even if Action text varies
- Skips money-market/cash-only securities to avoid double counting
"""

import argparse, os, re
from io import StringIO
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np

DEFAULT_SKIP_SECURITIES = {
    "Fidelity Govt Money Market",
    "-Cash-",
    "CASH",
    "CASH EQUIVALENT",
}

REINV_ACTIONS = {"ReinvDiv","ReinvLg","ReinvSh"}
BUY_ACTIONS   = {"Bought"} | REINV_ACTIONS
SELL_ACTIONS  = {"Sold"}

# Common Quicken labels for cash dividends / cap gains distributions
CASH_DIV_ACTIONS = {
    "Div","DivX","Dividend","CGLong","CGShort",
    "LT Cap Gain","ST Cap Gain","LT Cap Gn","ST Cap Gn"
}

DIVIDEND_AMOUNT_CANDIDATES = [
    "Cash",
    "Amount","Net","Net Amount",
    "DivInc","_DivInc","Div Inc","Dividend Income","Dividend","Income",
    "Cash+Invest","Cash + Invest","Cash Invest","Cash & Invest",
]

def canon(s: str) -> str:
    """canonicalize header names: alnum lowercase (remove spaces, _, +, etc.)"""
    return re.sub(r"[^a-z0-9]", "", s.lower())

def read_quicken(path: str) -> pd.DataFrame:
    """
    Read a Quicken export (.txt/.tsv) by locating the header row that contains
    'Date', 'Action', and 'Security', then parse as tab-delimited.
    """
    with open(path, "rb") as f:
        raw = f.read()
    text = raw.decode("latin-1", errors="replace")
    lines = text.splitlines()
    header_idx = None
    for i, line in enumerate(lines[:4000]):
        if "Date" in line and "Action" in line and "Security" in line:
            header_idx = i
            break
    if header_idx is None:
        raise SystemExit("Could not locate the header row with Date/Action/Security.")
    trimmed = "\n".join(lines[header_idx:])
    df = pd.read_csv(StringIO(trimmed), sep="\t", engine="python", dtype=str, keep_default_na=False)
    # normalize visible NBSPs in headers
    df.columns = [c.strip().replace("\xa0"," ").replace("\u00a0"," ") for c in df.columns]
    return df

def coerce_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.replace({",":""}, regex=True).replace("", np.nan), errors="coerce")

def build_iso(date_str: str, default_time: str, tzname: str) -> str | None:
    dt = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(dt):
        return None
    hh,mm,ss = map(int, default_time.split(":"))
    return datetime(dt.year, dt.month, dt.day, hh, mm, ss, tzinfo=ZoneInfo(tzname)).isoformat()

def pick_amount_from_candidates(frame: pd.DataFrame, candidates_display_names) -> pd.Series | None:
    """Return the first non-all-NaN numeric series from the candidate column names (robust to NBSP/underscores)."""
    name_map = {canon(c): c for c in frame.columns}
    for disp in candidates_display_names:
        key = canon(disp)
        if key in name_map:
            s = pd.to_numeric(frame[name_map[key]].astype(str).str.replace(",",""), errors="coerce")
            if s.notna().any():
                return s
    return None

def main():
    ap = argparse.ArgumentParser(description="Quicken TSV -> TradesViz CSV (stocks). Full dividends; underlying=symbol.")
    ap.add_argument("--input", required=True, help="Quicken export file (txt/tsv)")
    ap.add_argument("--output", required=True, help="Output TradesViz trade CSV path")
    ap.add_argument("--symbols-map", default="symbols_map.csv", help="CSV with columns: security,ticker")
    ap.add_argument("--tz", default="America/New_York")
    ap.add_argument("--emit-cashflows", action="store_true")
    ap.add_argument("--cashflows-output", default="cashflows.csv")
    ap.add_argument("--default-buy-time", default="09:31:00")
    ap.add_argument("--default-sell-time", default="15:59:00")
    ap.add_argument("--dedupe-cashflows", action="store_true", help="Combine same-day same-symbol dividends by summing amounts")
    args = ap.parse_args()

    df = read_quicken(args.input)

    # sanity check for required columns
    for col in ["Date","Action","Security","Quote/Price","Shares","Commission"]:
        if col not in df.columns:
            raise SystemExit(f"Missing required column: {col}")

    # Clean fields
    df["Action"] = df["Action"].str.strip()
    df["Security"] = df["Security"].str.strip()

    # Numeric convenience
    df["price"] = coerce_float(df["Quote/Price"])
    df["shares"] = coerce_float(df["Shares"])
    df["commission"] = coerce_float(df.get("Commission", pd.Series(index=df.index))).fillna(0.0)
    df["fees"] = coerce_float(df.get("Fees", pd.Series(index=df.index))).fillna(0.0)

    # ---- Trades (equities) ----
    trade_mask = df["Action"].isin(list(BUY_ACTIONS | SELL_ACTIONS))
    not_skip   = ~df["Security"].isin(DEFAULT_SKIP_SECURITIES)
    trades = df.loc[trade_mask & not_skip].copy()

    # Map Security -> ticker (symbol)
    if os.path.exists(args.symbols_map):
        sm = pd.read_csv(args.symbols_map)
        trades = trades.merge(sm, left_on="Security", right_on="security", how="left")
        trades["symbol"] = trades["ticker"].fillna("")
        trades.drop(columns=[c for c in ["security","ticker"] if c in trades.columns], inplace=True)
    else:
        trades["symbol"] = ""

    # Side mapping
    def to_side(a: str) -> str | None:
        a = (a or "").lower()
        if a in {"bought","reinvdiv","reinvlg","reinvsh"}: return "buy"
        if a == "sold": return "sell"
        return None

    trades["side"] = trades["Action"].map(to_side)
    trades["asset_type"] = "stock"
    trades["date"] = [build_iso(d, args.default_buy_time if a in BUY_ACTIONS else args.default_sell_time, args.tz)
                      for d,a in zip(trades["Date"], trades["Action"])]
    trades["underlying"] = trades["symbol"].fillna("")

    out = pd.DataFrame({
        "date": trades["date"],
        "symbol": trades["symbol"],
        "side": trades["side"],
        "currency": "USD",
        "underlying": trades["underlying"],
        "asset_type": trades["asset_type"],
        "price": trades["price"],
        "quantity": trades["shares"],
        "commission": trades["commission"],
        "fees": trades["fees"],
        "tags": "",
        "notes": trades["Action"],
        "spread_id": ""
    }).dropna(subset=["price","quantity"])

    out.to_csv(args.output, index=False)

    # ---- Dividends (full) ----
    if args.emit_cashflows:
        # Reinvested dividends -> dividend cashflow
        reinv = df.loc[df["Action"].isin(list(REINV_ACTIONS)) & (~df["Security"].isin(DEFAULT_SKIP_SECURITIES))].copy()
        if os.path.exists(args.symbols_map):
            sm = pd.read_csv(args.symbols_map)
            reinv = reinv.merge(sm, left_on="Security", right_on="security", how="left")
            reinv["symbol"] = reinv["ticker"].fillna("")
            reinv.drop(columns=[c for c in ["security","ticker"] if c in reinv.columns], inplace=True)
        else:
            reinv["symbol"] = ""
        reinv["date"] = [build_iso(d, args.default_buy_time, args.tz) for d in reinv["Date"]]

        reinv_amount = pick_amount_from_candidates(reinv, ["Amount Invested","AmountInvested"])
        if reinv_amount is None:
            reinv_amount = pd.to_numeric(reinv["Quote/Price"].str.replace(",",""), errors="coerce") * \
                           pd.to_numeric(reinv["Shares"].str.replace(",",""), errors="coerce")

        cf_reinv = pd.DataFrame({
            "date": reinv["date"],
            "symbol": reinv["symbol"],
            "type": "dividend",
            "amount": reinv_amount
        })

        # Cash dividends/distributions -> dividend cashflow
        # Candidates: explicit actions OR any row with positive amount in typical dividend columns,
        # excluding trades and reinvests, and excluding skip-securities.
        explicit_cash = df["Action"].isin(list(CASH_DIV_ACTIONS))
        not_trade_like = ~df["Action"].isin(list(BUY_ACTIONS | SELL_ACTIONS | REINV_ACTIONS))
        candidates = df.loc[(explicit_cash | not_trade_like) & (~df["Security"].isin(DEFAULT_SKIP_SECURITIES))].copy()

        cand_amount = pick_amount_from_candidates(candidates, DIVIDEND_AMOUNT_CANDIDATES)
        if cand_amount is None:
            # last resort fallback (often NaN for Div rows)
            cand_amount = pd.to_numeric(candidates["Quote/Price"].str.replace(",",""), errors="coerce") * \
                          pd.to_numeric(candidates["Shares"].str.replace(",",""), errors="coerce")
        candidates["__amt"] = cand_amount

        # Keep only positive amounts and remove reinvest rows
        candidates = candidates[pd.to_numeric(candidates["__amt"], errors="coerce").fillna(0) > 0]
        candidates = candidates[~candidates["Action"].isin(list(REINV_ACTIONS))]

        if os.path.exists(args.symbols_map):
            sm = pd.read_csv(args.symbols_map)
            candidates = candidates.merge(sm, left_on="Security", right_on="security", how="left")
            candidates["symbol"] = candidates["ticker"].fillna("")
            candidates.drop(columns=[c for c in ["security","ticker"] if c in candidates.columns], inplace=True)
        else:
            candidates["symbol"] = ""

        candidates["date"] = [build_iso(d, "12:00:00", args.tz) for d in candidates["Date"]]

        cf_cash = pd.DataFrame({
            "date": candidates["date"],
            "symbol": candidates["symbol"],
            "type": "dividend",
            "amount": candidates["__amt"]
        })

        cf_all = pd.concat([cf_reinv, cf_cash], ignore_index=True)
        cf_all = cf_all[pd.to_numeric(cf_all["amount"], errors="coerce").fillna(0) > 0]

        if args.dedupe_cashflows:
            # Combine same-day same-symbol
            cf_all["date"] = pd.to_datetime(cf_all["date"], errors="coerce")
            cf_all = (cf_all
                      .dropna(subset=["date"])
                      .groupby([cf_all["symbol"].astype(str).str.upper(), cf_all["date"].dt.strftime("%Y-%m-%d")], as_index=False)["amount"]
                      .sum())
            cf_all.insert(0, "type", "dividend")
            # restore order: date, symbol, type, amount
            cf_all = cf_all.rename(columns={"0":"symbol","1":"date"})[["date","symbol","type","amount"]]

        cf_all.to_csv(args.cashflows_output, index=False)

    print(f"[OK] wrote {len(out)} trades to {args.output}")

if __name__ == "__main__":
    main()
