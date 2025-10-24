
#!/usr/bin/env python3
import argparse, os, re
from io import StringIO
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np

REINV_ACTIONS = {"ReinvDiv","ReinvLg","ReinvSh"}
BUY_ACTIONS   = {"Bought"} | REINV_ACTIONS
SELL_ACTIONS  = {"Sold"}

def iso_from_mdY(mdY: str, hhmmss: str, tz="America/New_York"):
    dt = datetime.strptime(mdY, "%m/%d/%Y")
    h,m,s = [int(x) for x in hhmmss.split(":")]
    return datetime(dt.year, dt.month, dt.day, h, m, s, tzinfo=ZoneInfo(tz)).isoformat()

def to_num(x):
    try:
        return float(str(x).replace(",",""))
    except:
        return None

def load_quicken(path: str) -> pd.DataFrame:
    with open(path, "rb") as f:
        txt = f.read().decode("latin-1", errors="replace")
    lines = txt.splitlines()
    hdr = None
    for i, ln in enumerate(lines[:5000]):
        if "Date" in ln and "Action" in ln and "Security" in ln:
            hdr = i
            break
    if hdr is None:
        raise SystemExit("Header not found (no Date/Action/Security).")
    df = pd.read_csv(StringIO("\n".join(lines[hdr:])), sep="\t", engine="python", dtype=str, keep_default_na=False)
    df.columns = [c.strip().replace("\xa0"," ").replace("\u00a0"," ") for c in df.columns]
    for c in ["Date","Action","Security"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def main():
    ap = argparse.ArgumentParser(description="Quicken -> TradesViz (ticker-only; header+detail dividends).")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--symbols-map", default="symbols_map.csv")
    ap.add_argument("--tz", default="America/New_York")
    ap.add_argument("--emit-cashflows", action="store_true")
    ap.add_argument("--cashflows-output", default="cashflows.csv")
    ap.add_argument("--default-buy-time", default="09:31:00")
    ap.add_argument("--default-sell-time", default="15:59:00")
    args = ap.parse_args()

    df = load_quicken(args.input)
    for col in ["Date","Action","Security","Quote/Price","Shares","Commission"]:
        if col not in df.columns:
            raise SystemExit(f"Missing required column: {col}")

    # mapping: exact Security -> ticker; enforce ticker-only
    sm = pd.read_csv(args.symbols_map, dtype=str).fillna("")
    if "security" not in sm.columns or "ticker" not in sm.columns:
        raise SystemExit("symbols_map.csv must have columns: security,ticker")
    sm["ticker"] = sm["ticker"].astype(str).str.strip()
    sm = sm[sm["ticker"] != ""].copy()
    sec_to_ticker = dict(zip(sm["security"], sm["ticker"]))

    # ===== Trades =====
    df["Action"] = df["Action"].astype(str).str.strip()
    df["Security"] = df["Security"].astype(str).str.strip()

    def coerce_float(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series.replace({",":""}, regex=True).replace("", np.nan), errors="coerce")

    df["price"] = coerce_float(df["Quote/Price"])
    df["shares"] = coerce_float(df["Shares"])
    df["commission"] = coerce_float(df.get("Commission", pd.Series(index=df.index))).fillna(0.0)
    df["fees"] = coerce_float(df.get("Fees", pd.Series(index=df.index))).fillna(0.0)

    trade_mask = df["Action"].isin(list(BUY_ACTIONS | SELL_ACTIONS))
    trades = df.loc[trade_mask].copy()
    # keep only those with known tickers (filters MMF/cash)
    trades = trades[trades["Security"].isin(sec_to_ticker.keys())].copy()
    trades["symbol"] = trades["Security"].map(sec_to_ticker)

    def to_side(a: str) -> str | None:
        a = (a or "").lower()
        if a in {"bought","reinvdiv","reinvlg","reinvsh"}: return "buy"
        if a == "sold": return "sell"
        return None
    trades["side"] = trades["Action"].map(to_side)
    trades["asset_type"] = "stock"
    # timestamp by side default
    dates = []
    for d, a in zip(trades["Date"], trades["Action"]):
        t = args.default_buy_time if a in BUY_ACTIONS else args.default_sell_time
        dates.append(iso_from_mdY(d, t, tz=args.tz))
    trades["date"] = dates
    trades["underlying"] = trades["symbol"]

    out_trades = pd.DataFrame({
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
    out_trades.to_csv(args.output, index=False)

    # ===== Cashflows (dividends) =====
    if args.emit_cashflows:
        rows = []
        n = len(df)
        i = 0
        while i < n:
            row = df.iloc[i]
            act = row.get("Action","")
            sec = row.get("Security","")
            date = row.get("Date","")
            # reinvested dividends -> amount on the header row (Amount Invested)
            if act in {"ReinvDiv","ReinvLg","ReinvSh"} and sec in sec_to_ticker:
                amt = None
                if "Amount Invested" in df.columns:
                    amt = to_num(row.get("Amount Invested",""))
                if amt is None:
                    price = to_num(row.get("Quote/Price",""))
                    shares = to_num(row.get("Shares",""))
                    if price is not None and shares is not None:
                        amt = price * shares
                if amt and amt > 0:
                    rows.append({
                        "date": iso_from_mdY(date, "09:31:00", tz=args.tz),
                        "symbol": sec_to_ticker[sec],
                        "type": "dividend",
                        "amount": amt
                    })
                i += 1
                continue

            # cash dividends: header 'Div' + next detail row
            if act == "Div" and sec in sec_to_ticker:
                amt = None
                if i+1 < n:
                    nxt = df.iloc[i+1]
                    if str(nxt.get("Action","")).strip() == "" and str(nxt.get("Date","")).strip() == "":
                        # try Cash first, then common aliases
                        for col in ["Cash","DivInc","_DivInc","Dividend Income","Amount","Net","Net Amount"]:
                            if col in df.columns:
                                val = to_num(nxt.get(col,""))
                                if val is not None and val > 0:
                                    amt = val
                                    break
                if amt and amt > 0:
                    rows.append({
                        "date": iso_from_mdY(date, "12:00:00", tz=args.tz),
                        "symbol": sec_to_ticker[sec],
                        "type": "dividend",
                        "amount": amt
                    })
                i += 2
                continue

            i += 1

        cf = pd.DataFrame(rows)
        cf.to_csv(args.cashflows_output, index=False)

    print(f"[OK] Trades: {len(out_trades)} rows. Dividends: {len(rows) if args.emit_cashflows else 0} rows.")

if __name__ == "__main__":
    main()
