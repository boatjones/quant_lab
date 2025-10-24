
#!/usr/bin/env python3
import argparse, os, re
from io import StringIO
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np

DEFAULT_SKIP_SECURITIES = {"Fidelity Govt Money Market","-Cash-","CASH","CASH EQUIVALENT"}
REINV_ACTIONS = {"ReinvDiv","ReinvLg","ReinvSh"}
BUY_ACTIONS   = {"Bought"} | REINV_ACTIONS
SELL_ACTIONS  = {"Sold"}
CASH_DIV_ACTIONS = {"Div","DivX","Dividend","CGLong","CGShort","LT Cap Gain","ST Cap Gain","LT Cap Gn","ST Cap Gn"}

DIV_AMT_CANDIDATES = [
    "DivInc","_DivInc","Dividend Income","Dividend","Income",
    "Cash","Amount","Net","Net Amount",
    "Cash+Invest","Cash + Invest","Cash Invest","Cash & Invest",
    "Amount Invested","AmountInvested"
]

def canon(s: str) -> str:
    s = re.sub(r"[^0-9A-Za-z]+", " ", str(s)).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def read_quicken(path: str) -> pd.DataFrame:
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

def pick_amount(frame: pd.DataFrame, candidates):
    name_map = {re.sub(r"[^0-9A-Za-z]+","", c.lower()): c for c in frame.columns}
    for disp in candidates:
        key = re.sub(r"[^0-9A-Za-z]+","", disp.lower())
        if key in name_map:
            s = pd.to_numeric(frame[name_map[key]].astype(str).str.replace(",",""), errors="coerce")
            if s.notna().any():
                return s
    return None

def main():
    ap = argparse.ArgumentParser(description="Quicken TSV -> TradesViz CSV (stocks). Ticker-only + full dividends; underlying=symbol.")
    ap.add_argument("--input", required=True, help="Quicken export file (txt/tsv)")
    ap.add_argument("--output", required=True, help="Output TradesViz trade CSV path")
    ap.add_argument("--symbols-map", default="symbols_map.csv", help="CSV with columns: security,ticker")
    ap.add_argument("--tz", default="America/New_York")
    ap.add_argument("--emit-cashflows", action="store_true")
    ap.add_argument("--cashflows-output", default="cashflows.csv")
    ap.add_argument("--default-buy-time", default="09:31:00")
    ap.add_argument("--default-sell-time", default="15:59:00")
    args = ap.parse_args()

    df = read_quicken(args.input)
    for col in ["Date","Action","Security","Quote/Price","Shares","Commission"]:
        if col not in df.columns:
            raise SystemExit(f"Missing required column: {col}")
    df["Action"] = df["Action"].astype(str).str.strip()
    df["Security"] = df["Security"].astype(str).str.strip()

    sm = pd.read_csv(args.symbols_map, dtype=str).fillna("")
    if "security" not in sm.columns or "ticker" not in sm.columns:
        raise SystemExit("symbols_map.csv must have columns: security,ticker")
    sm["security_norm"] = sm["security"].apply(canon)
    sm["ticker"] = sm["ticker"].astype(str).str.strip()
    sm_with_ticker = sm[sm["ticker"] != ""].copy()

    df["price"] = coerce_float(df["Quote/Price"])
    df["shares"] = coerce_float(df["Shares"])
    df["commission"] = coerce_float(df.get("Commission", pd.Series(index=df.index))).fillna(0.0)
    df["fees"] = coerce_float(df.get("Fees", pd.Series(index=df.index))).fillna(0.0)

    # ===== Trades (ticker-only) =====
    trade_mask = df["Action"].isin(list(BUY_ACTIONS | SELL_ACTIONS))
    not_skip   = ~df["Security"].isin(DEFAULT_SKIP_SECURITIES)
    trades = df.loc[trade_mask & not_skip].copy()
    trades["security_norm"] = trades["Security"].apply(canon)
    trades = trades.merge(sm_with_ticker[["security_norm","ticker"]], how="inner", on="security_norm")
    trades["symbol"] = trades["ticker"]

    def to_side(a: str) -> str | None:
        a = (a or "").lower()
        if a in {"bought","reinvdiv","reinvlg","reinvsh"}: return "buy"
        if a == "sold": return "sell"
        return None
    trades["side"] = trades["Action"].map(to_side)
    trades["asset_type"] = "stock"
    trades["date"] = [build_iso(d, args.default_buy_time if a in BUY_ACTIONS else args.default_sell_time, args.tz)
                      for d,a in zip(trades["Date"], trades["Action"])]
    trades["underlying"] = trades["symbol"]

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

    # ===== Dividends (ticker-only) =====
    if args.emit_cashflows:
        # Reinvested dividends (dividend cashflow + separate buy already in trades via REINV_ACTIONS)
        reinv = df.loc[df["Action"].isin(list(REINV_ACTIONS)) & (~df["Security"].isin(DEFAULT_SKIP_SECURITIES))].copy()
        reinv["security_norm"] = reinv["Security"].apply(canon)
        reinv = reinv.merge(sm_with_ticker[["security_norm","ticker"]], how="inner", on="security_norm")
        reinv["symbol"] = reinv["ticker"]
        reinv["date"] = [build_iso(d, "09:31:00", args.tz) for d in reinv["Date"]]
        reinv_amt = pick_amount(reinv, ["Amount Invested","AmountInvested"])
        if reinv_amt is None:
            reinv_amt = pd.to_numeric(reinv["Quote/Price"].str.replace(",",""), errors="coerce") * \
                        pd.to_numeric(reinv["Shares"].str.replace(",",""), errors="coerce")
        cf_reinv = pd.DataFrame({
            "date": reinv["date"],
            "symbol": reinv["symbol"],
            "type": "dividend",
            "amount": reinv_amt
        })

        # Cash dividends and distributions
        explicit_cash = df["Action"].isin(list(CASH_DIV_ACTIONS))
        not_trade_like = ~df["Action"].isin(list(BUY_ACTIONS | SELL_ACTIONS | REINV_ACTIONS))
        cands = df.loc[(explicit_cash | not_trade_like) & (~df["Security"].isin(DEFAULT_SKIP_SECURITIES))].copy()
        cands["security_norm"] = cands["Security"].apply(canon)
        cands = cands.merge(sm_with_ticker[["security_norm","ticker"]], how="inner", on="security_norm")
        cands["symbol"] = cands["ticker"]
        cands["date"] = [build_iso(d, "12:00:00", args.tz) for d in cands["Date"]]

        amt = pick_amount(cands, DIV_AMT_CANDIDATES)
        if amt is None:
            amt = pd.to_numeric(cands["Quote/Price"].str.replace(",",""), errors="coerce") * \
                  pd.to_numeric(cands["Shares"].str.replace(",",""), errors="coerce")
        cands["__amt"] = amt
        cands = cands[pd.to_numeric(cands["__amt"], errors="coerce").fillna(0) > 0]

        cf_cash = pd.DataFrame({
            "date": cands["date"],
            "symbol": cands["symbol"],
            "type": "dividend",
            "amount": cands["__amt"]
        })

        cf_all = pd.concat([cf_reinv, cf_cash], ignore_index=True)
        cf_all = cf_all[pd.to_numeric(cf_all["amount"], errors="coerce").fillna(0) > 0]
        cf_all.to_csv(args.cashflows_output, index=False)

    print(f"[OK] wrote {len(out)} trades to {args.output}")

if __name__ == "__main__":
    main()
