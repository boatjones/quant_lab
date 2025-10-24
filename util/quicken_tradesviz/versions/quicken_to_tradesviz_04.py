
import pandas as pd, numpy as np, argparse, os
from datetime import datetime
from zoneinfo import ZoneInfo
from io import StringIO

DEFAULT_SKIP_SECURITIES = {"Fidelity Govt Money Market","-Cash-","CASH","CASH EQUIVALENT"}
REINV_ACTIONS = {"ReinvDiv","ReinvLg","ReinvSh"}
BUY_ACTIONS = {"Bought"} | REINV_ACTIONS
SELL_ACTIONS = {"Sold"}
CASH_DIV_ACTIONS = {"Div","DivX","Dividend","CGLong","CGShort"}

def read_quicken(path):
    if os.path.basename(path).endswith(".tsv"):
        df = pd.read_csv(path, sep="\t", engine="python", dtype=str, keep_default_na=False, encoding="latin-1")
    else:
        with open(path, "rb") as f:
            raw = f.read()
        text = raw.decode("latin-1", errors="replace")
        lines = text.splitlines()
        header_idx = None
        for i, line in enumerate(lines[:1000]):
            if "Date" in line and "Action" in line and "Security" in line:
                header_idx = i
                break
        if header_idx is None:
            raise SystemExit("Header row not found in input file.")
        trimmed = "\n".join(lines[header_idx:])
        df = pd.read_csv(StringIO(trimmed), sep="\t", engine="python", dtype=str, keep_default_na=False)
    df.columns = [c.strip().replace("\xa0"," ").replace("\u00a0"," ") for c in df.columns]
    return df

def coerce_float(s):
    return pd.to_numeric(s.replace({",":""}, regex=True).replace("", np.nan), errors="coerce")

def build_iso(date_str, default_time, tzname):
    dt = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(dt): return None
    hh,mm,ss = map(int, default_time.split(":"))
    return datetime(dt.year, dt.month, dt.day, hh, mm, ss, tzinfo=ZoneInfo(tzname)).isoformat()

def main():
    ap = argparse.ArgumentParser(description="Quicken TSV -> TradesViz CSV (stocks). underlying=symbol; full dividends.")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--symbols-map", default="symbols_map.csv")
    ap.add_argument("--tz", default="America/New_York")
    ap.add_argument("--emit-cashflows", action="store_true")
    ap.add_argument("--cashflows-output", default="cashflows.csv")
    ap.add_argument("--default-buy-time", default="09:31:00")
    ap.add_argument("--default-sell-time", default="15:59:00")
    args = ap.parse_args()

    df = read_quicken(args.input)

    req = ["Date","Action","Security","Quote/Price","Shares","Commission"]
    for c in req:
        if c not in df.columns: raise SystemExit(f"Missing column: {c}")

    df["Action"] = df["Action"].str.strip()
    df["Security"] = df["Security"].str.strip()

    df["price"] = coerce_float(df["Quote/Price"])
    df["shares"] = coerce_float(df["Shares"])
    df["commission"] = coerce_float(df.get("Commission", pd.Series(index=df.index))).fillna(0.0)
    df["fees"] = coerce_float(df.get("Fees", pd.Series(index=df.index))).fillna(0.0)

    trade_mask = df["Action"].isin(list(BUY_ACTIONS | SELL_ACTIONS))
    not_skip = ~df["Security"].isin(DEFAULT_SKIP_SECURITIES)
    trades = df.loc[trade_mask & not_skip].copy()

    if os.path.exists(args.symbols_map):
        sm = pd.read_csv(args.symbols_map)
        trades = trades.merge(sm, left_on="Security", right_on="security", how="left")
        trades["symbol"] = trades["ticker"].fillna("")
        trades.drop(columns=[c for c in ["security","ticker"] if c in trades.columns], inplace=True)
    else:
        trades["symbol"] = ""

    def to_side(a):
        a=a.lower()
        if a in {"bought","reinvdiv","reinvlg","reinvsh"}: return "buy"
        if a=="sold": return "sell"
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

    if args.emit_cashflows:
        REINV_ACTIONS = {"ReinvDiv","ReinvLg","ReinvSh"}
        reinv = df.loc[df["Action"].isin(list(REINV_ACTIONS)) & (~df["Security"].isin(DEFAULT_SKIP_SECURITIES))].copy()
        if os.path.exists(args.symbols_map):
            sm = pd.read_csv(args.symbols_map)
            reinv = reinv.merge(sm, left_on="Security", right_on="security", how="left")
            reinv["symbol"] = reinv["ticker"].fillna("")
        else:
            reinv["symbol"] = ""
        reinv["date"] = [build_iso(d, args.default_buy_time, args.tz) for d in reinv["Date"]]

        amt_inv_col = None
        for c in reinv.columns:
            if c.replace("\xa0"," ").replace("\u00a0"," ") == "Amount Invested":
                amt_inv_col = c
                break
        if amt_inv_col is not None:
            reinv_amount = pd.to_numeric(reinv[amt_inv_col].str.replace(",",""), errors="coerce")
        else:
            reinv_amount = pd.to_numeric(reinv["Quote/Price"].str.replace(",",""), errors="coerce") * \
                           pd.to_numeric(reinv["Shares"].str.replace(",",""), errors="coerce")

        cf_reinv = pd.DataFrame({
            "date": reinv["date"],
            "symbol": reinv["symbol"],
            "type": "dividend",
            "amount": reinv_amount
        }).dropna(subset=["amount"])

        cashdiv = df.loc[df["Action"].isin(list(CASH_DIV_ACTIONS)) & (~df["Security"].isin(DEFAULT_SKIP_SECURITIES))].copy()
        if os.path.exists(args.symbols_map):
            sm = pd.read_csv(args.symbols_map)
            cashdiv = cashdiv.merge(sm, left_on="Security", right_on="security", how="left")
            cashdiv["symbol"] = cashdiv["ticker"].fillna("")
        else:
            cashdiv["symbol"] = ""
        cashdiv["date"] = [build_iso(d, "12:00:00", args.tz) for d in cashdiv["Date"]]

        # Robust amount detection for cash dividends
        norm_cols = {c: c.replace("\xa0"," ").replace("\u00a0"," ").strip() for c in cashdiv.columns}
        inv_norm = {v.lower(): k for k, v in norm_cols.items()}

        candidate_names = [
            "Cash",
            "Amount", "Net", "Net Amount",
            "DivInc", "Div Inc", "Dividend Income", "Dividend", "Income",
            "Cash+Invest", "Cash + Invest", "Cash Invest"
        ]

        amount_series = None
        for name in candidate_names:
            key = inv_norm.get(name.lower())
            if key and key in cashdiv.columns:
                s = pd.to_numeric(cashdiv[key].astype(str).str.replace(",",""), errors="coerce")
                if s.notna().any():
                    amount_series = s
                    break

        if amount_series is None:
            amount_series = pd.to_numeric(cashdiv["Quote/Price"].str.replace(",",""), errors="coerce") * \
                            pd.to_numeric(cashdiv["Shares"].str.replace(",",""), errors="coerce")

        cf_cash = pd.DataFrame({
            "date": cashdiv["date"],
            "symbol": cashdiv["symbol"],
            "type": "dividend",
            "amount": amount_series
        }).dropna(subset=["amount"])

        cf_all = pd.concat([cf_reinv, cf_cash], ignore_index=True)
        cf_all = cf_all[pd.to_numeric(cf_all["amount"], errors="coerce").fillna(0) > 0]
        cf_all.to_csv(args.cashflows_output, index=False)

    print(f"[OK] wrote {len(out)} trades to {args.output}")

if __name__ == "__main__":
    main()
