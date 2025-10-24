'''
    Order submission front-end to Interactive Brokers
    1) Load buy orders from spreadsheet using ATR position sizing and ATR stops
        provide buttons to put in entry and, after order submission, put in stop order
        Limitation: for limit orders, must use TWS to determine if filled - no polling
    2) Load portfolio positions to allow 
        a. Selling all or portion of position
        b. Put in stop orders: 
            1. Percent only is trailing %, 
            2. Dollar value only is dollar stop, 
            3. Both % and $ amounts is initial dollar trail amount and then trailing percent.
'''

import asyncio
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())
    
import streamlit as st
import pandas as pd
from ib_insync import IB, util
from pathlib import Path
import os, sys
import builtins, functools
import socket
import traceback
import time

# Ensure every print flushes immediately for debugging
builtins.print = functools.partial(builtins.print, flush=True)
sys.stdout.reconfigure(line_buffering=True)

# Ensure event loop is running in the Streamlit thread
try:
    util.startLoop()
except:
    pass

sys.path.insert(0, os.path.join(Path(__file__).parents[1]))
from ib_classes import marketEqOrder, limitEqOrder, stopEqOrder, trailStopEqOrder

st.set_page_config(page_title="IB Orders", layout="wide")

# --- Guarantee a running asyncio loop in Streamlit threads ---
def ensure_event_loop():
    """Ensure a running asyncio event loop in the current thread."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
    else:
        return asyncio.get_running_loop()

# --- Persistent IB object ---
if "ib" not in st.session_state:
    st.session_state.ib = IB()
ib = st.session_state.ib

# Initialize session state at the top of your script
if "positions_df" not in st.session_state:
    st.session_state.positions_df = None
if "orders_loaded" not in st.session_state:
    st.session_state.orders_loaded = False
if "trades" not in st.session_state:
    st.session_state.trades = {}
if "entry_filled" not in st.session_state:
    st.session_state.entry_filled = {}

# --- Connect / Disconnect Handlers ---
def connect_ib():
    """Safely connect to Interactive Brokers."""
    # loop = get_or_create_event_loop()
    try:
        if ib.isConnected():
            st.info("Already connected to IB Gateway/TWS.")
        
        client_id = st.session_state.get("ib_client_id", 3)
        ib.connect("127.0.0.1", 7497, clientId=client_id)
        st.write(f"Connected: {ib.isConnected()}, Client ID: {ib.client.clientId}")

        if ib.isConnected():
            st.session_state["ib_connected"] = True
            client_info = f"{ib.client.host}:{ib.client.port} (clientId={ib.client.clientId})"
            st.success(f"✅ Connected to IB at: {client_info}")
        else:
            st.session_state["ib_connected"] = False
            st.error("⚠️ Connection attempt failed. Check that TWS or IB Gateway is open and API is enabled.")
    except ConnectionRefusedError:
        st.session_state['ib_connected'] = False
        st.error("❌ Connection refused — TWS or IB Gateway is not running, or API access is disabled.")
    except socket.timeout:
        st.session_state['ib_connected'] = False
        st.error("⏱️ Connection timed out — check your TWS network settings and retry.")
    except OSError as e:
        if "already connected" in str(e).lower():
            st.warning("⚠️ Client ID already in use. Try using a different clientId.")
        else:
            st.error(f"💥 OS-level socket error: {e}")    
    except Exception as e:
        st.session_state["ib_connected"] = False
        st.error(f"⚠️ Unexpected connection error: {type(e).__name__} — {e}")
        st.text(traceback.format_exc())

def disconnect_ib():
    """Safely disconnect from IB."""
    try:
        if ib.isConnected():
            ib.disconnect()
            st.session_state["ib_connected"] = False
            st.info("🔌 Disconnected from IB.")
        else:
            st.warning("Already disconnected.")
    except Exception as e:
        st.error(f"⚠️ Error during disconnect: {type(e).__name__} — {e}")
        st.text(traceback.format_exc())     

# --- Get Portfolio Position function
def get_ib_positions(ib):
    """Retrieve and format current IB positions."""
    try:
        pos = ib.positions()
        if not pos:
            st.info("No positions found.")
            return pd.DataFrame()

        df = util.df(pos)
        df["Symbol"] = df.contract.apply(lambda x: x.symbol)
        df["ConID"] = df.contract.apply(lambda x: x.conId)
        df["Exchange"] = df.contract.apply(lambda x: x.exchange)
        df["Position"] = df["position"]
        df["AvgCost"] = df["avgCost"].round(2)

        # Drop internal contract objects
        df = df[["Symbol", "Exchange", "Position", "AvgCost"]]
        return df

    except Exception as e:
        st.error(f"Error retrieving positions: {e}")
        return pd.DataFrame()

# --- Client ID selector (default 3) ---
st.markdown("### IB Connection Settings")
client_id = st.number_input(
    "Client ID",
    min_value=1,
    max_value=100,
    value=st.session_state.get("ib_client_id", 3),
    step=1,
    help="Each running IB client (like TWS, Gateway, or app) must use a unique Client ID (1–100)."
)
st.session_state["ib_client_id"] = client_id

# --- UI elements ---
col1, col2 = st.columns(2)
if col1.button("🔌 Connect to IB"):
    connect_ib()
if col2.button("❌ Disconnect"):
    disconnect_ib()

# --- Status indicator ---
if ib.isConnected():
    st.success("🟢 Connected to IB Gateway/TWS")
else:
    st.warning("🟠 Not connected")

#### Mode switch between spreadsheet import and portfolio positions
# --- Page's Mode selector with stable keys and friendly labels ---
modes = {
    "sheet": "📄 Import from Excel / ODS",
    "ib_positions": "📊 Load from IB Positions"
}

# Reverse lookup for the radio display
display_to_key = {v: k for k, v in modes.items()}

selected_label = st.radio(
    "Select Input Source:",
    list(modes.values()),
    horizontal=True
)

# Convert back to the internal key (e.g. "sheet" or "ib_positions")
mode = display_to_key[selected_label]

#### If spreadsheet import mode
if mode == "sheet":

    if st.button("Import Positions To Execute"):
        try:        
            df = pd.read_excel("PositionsStops.ods", engine="odf")
            # Debug: writes contents of imported spreadsheet to page
            # st.write(df)
        except:
            st.error("PositionsStops.ods file not found")
            st.stop()
        
        # --- Parse tickers and positions from fixed rows ---
        tickers = df.iloc[3, 1:8].dropna().tolist()  # row 5 in Excel is index 3 in pandas (0-based)
        share_prices = df.iloc[4, 1:8].tolist()
        shares_to_buy = df.iloc[13, 1:8].tolist()
        limit_prices = df.iloc[14, 1:8].tolist()
        trail_stops = df.iloc[21, 1:8].tolist()
        trail_percents = df.iloc[24, 1:8].tolist()

        # --- Build table ---
        data = []
        for i, ticker in enumerate(tickers):
            limit_price = limit_prices[i]
            order_type = "LMT" if pd.notna(limit_price) else "MKT"
            data.append({
                "Ticker": ticker,
                "Price": share_prices[i],
                "Shares": int(shares_to_buy[i]),
                "OrderType": order_type,
                "LimitPrice": limit_price if pd.notna(limit_price) else "",
                "TrailStopPrice": trail_stops[i],
                "TrailPercent": trail_percents[i],
            })

        st.session_state.positions_df = pd.DataFrame(data)
        st.session_state.trades = {}
        st.session_state["orders_loaded"] = True

    # --- Persisted table ---
    if st.session_state.get("orders_loaded"):
        df = st.session_state.positions_df

        # Remove rows with zero shares
        df = df[df["Shares"] > 0].reset_index(drop=True)

        st.subheader("📋 Review Orders to Submit")

        # --- Table header ---
        headers = ["Ticker", "Price", "Shares", "OrderType", "LimitPrice", "TrailStopPrice", "TrailPercent", "Actions"]
        cols = st.columns([1, 1, 1, 1, 1, 1, 1, 1])
        for c, h in zip(cols, headers):
            c.markdown(f"**{h}**")

        # Refresh statuses for all saved trades
        if ib.isConnected():
            for tkr, trade in list(st.session_state.trades.items()):
                try:
                    if not ib.isConnected():  # skip mid-loop if disconnected
                        continue
                    ib.sleep(0.2)  # non-blocking sleep
                    status = trade.orderStatus.status
                    if status == "Filled":
                        st.session_state.entry_filled[tkr] = True
                except Exception as e:
                    print(f"[DEBUG] Could not update status for {tkr}: {e}", flush=True)


        # --- Render each row ---
        for idx, row in df.iterrows():
            c1, c2, c3, c4, c5, c6, c7, c8 = st.columns([1, 1, 1, 1, 1, 1, 1, 1])

            c1.write(row.Ticker)
            c2.write(f"{row.Price:.2f}")
            c3.write(int(row.Shares))
            c4.write(row.OrderType)
            c5.write("" if pd.isna(row.LimitPrice) else row.LimitPrice)
            c6.write(f"{row.TrailStopPrice:.2f}")
            c7.write(f"{row.TrailPercent:.2f}")

            ticker_key = row.Ticker
            entry_key = f"entry_{row.Ticker}_{idx}"
            stop_key = f"stop_{row.Ticker}_{idx}"

            trade = st.session_state.trades.get(ticker_key)
            filled = st.session_state.entry_filled.get(ticker_key, False)

            # --- Connection Check ---
            if not ib.isConnected():
                c8.warning("⚠️ Not connected")
                continue

            # --- Entry button or checkmark ---
            if not trade:
                # Skip if not connected
                if not ib.isConnected():
                    st.error("⚠️ Not connected to IB. Please connect first.")
                    # st.stop()
                else:                
                    if c8.button("🚀 Entry", key=entry_key):
                        try:
                            # create order based on type
                            if row.OrderType == "MKT":
                                order = marketEqOrder(row.Ticker, "B", int(row.Shares), ib_instance=ib)
                            elif row.OrderType == "LMT":
                                order = limitEqOrder(row.Ticker, "B", int(row.Shares), row.LimitPrice, ib_instance=ib)
                            else:
                                st.warning(f"Unknown order type for {row.Ticker}")
                                continue

                            # Execute trade (runs in Streamlit's thread)
                            trade_log = order.execTrade()

                            if trade_log:
                                st.session_state.trades[ticker_key] = order.trade
                                st.success(f"Entry order sent for {row.Ticker}")                            
                                st.rerun()
                            else:
                                st.error(f"❌ Failed to place order for {row.Ticker}")

                        except Exception as e:
                            st.error(f"Trade execution error for {row.Ticker}: {e}")
                            import traceback
                            st.code(traceback.format_exc())

            else:
                # show check icon once order submitted
                c8.markdown("✅ Entry Placed")

            # --- Stop button - always show once entry is placed ---
            entry_submitted = st.session_state.get(f"entry_submitted_{ticker_key}", False)

            # State 3: Stop already submitted -- show checkmark only
            if st.session_state.get(f"stop_submitted_{ticker_key}", False):            
                c8.markdown("✅ Stop Submitted")
            
            # State 2: Entry submitted - show active Stop button
            elif entry_submitted:              
                if not ib.isConnected():
                    st.error("⚠️ Not connected to IB. Please connect first.")
                else:
                    if c8.button("🛑 Stop", key=stop_key):
                        try:
                            # choose the appropriate stop order type
                            if row.TrailPercent > 0:
                                order = trailStopEqOrder(
                                    row.Ticker, "S", int(row.Shares),
                                    trailing_percent=row.TrailPercent,
                                    trail_stop_price=row.TrailStopPrice,
                                    ib_instance=ib
                                )
                            else:
                                order = stopEqOrder(
                                    row.Ticker, "S", int(row.Shares), 
                                    row.TrailStopPrice, 
                                    ib_instance=ib
                                )
                            
                            # execute the order
                            trade_log = order.execTrade()
                            if trade_log:
                                st.success(f"🛑 Stop order submitted for {row.Ticker}")
                                st.session_state[f"stop_submitted_{ticker_key}"] = True
                                st.rerun()
                            else:
                                st.error(f"Failed to submit stop order for {row.Ticker}")

                        except Exception as e:
                            st.error(f"Stop order error for {row.Ticker}: {e}")
                            import traceback
                            st.code(traceback.format_exc())

            else:
                # State 1: Entry not yet filled - hide Stop button entirely.
                c8.empty()

#### If pulling down postions in IB mode
elif mode == "ib_positions":
    positions_df = get_ib_positions(ib)

    if not positions_df.empty:
        st.subheader("Current IB Positions")

        # --- Column headers ---
        header_cols = st.columns([1.3, 1, 1, 1, 1.2, 1, .5])
        header_cols[0].markdown("**Symbol (Exchange)**")
        header_cols[1].markdown("**Position**")
        header_cols[2].markdown("**Avg Cost**")
        header_cols[3].markdown("**Shares to Sell**")
        header_cols[4].markdown("**Stop / Trail Amt ($)**")
        header_cols[5].markdown("**Trail %**")
        header_cols[6].markdown("") # action button

        for i, row in positions_df.iterrows():
            col1, col2, col3, col4, col5, col6, col7 = st.columns([1.5, 1, 1, 1, 1, 1, 1])
            col1.markdown(f"**{row.Symbol}** ({row.Exchange})")
            col2.write(f"{row.Position}")
            col3.write(f"${row.AvgCost:.2f}")

            shares = col4.number_input(f"Shares_{i}", min_value=0, step=1, label_visibility="collapsed", value=int(abs(row.Position)))
            trail_stop_price = col5.number_input(f"TrailStopPrice_{i}", min_value=0.0, step=0.01, label_visibility="collapsed")
            trail_percent = col6.number_input(f"TrailPercent_{i}", min_value=0.0, step=0.1, label_visibility="collapsed")

            sell_key = f"sell_{row.Symbol}_{i}"
            stop_key = f"stop_{row.Symbol}_{i}"

            # --- Sell Button ---
            if col7.button("💰 Sell", key=sell_key):
                try:
                    order = marketEqOrder(row.Symbol, "S", shares, ib_instance=ib)
                    order.execTrade()
                    st.success(f"Sell order submitted for {shares} {row.Symbol}")
                except Exception as e:
                    st.error(f"Sell order failed for {row.Symbol}: {e}")

            # --- Stop Button ---
            if col7.button("🛑 Stop", key=stop_key):
                try:
                    if trail_percent > 0:
                        order = trailStopEqOrder(
                            row.Symbol, "S", int(shares),
                            trailing_percent=trail_percent,
                            trail_stop_price=trail_stop_price,
                            ib_instance=ib
                        )
                    elif trail_stop_price > 0:
                        order = stopEqOrder(
                            row.Symbol, "S", int(shares),
                            trail_stop_price,
                            ib_instance=ib
                        )
                    else:
                        st.warning(f"Please enter either a Trail Stop Price or Trail Percent for {row.Symbol}")
                        continue

                    order.execTrade()
                    st.success(f"Stop order submitted for {row.Symbol}")

                except Exception as e:
                    st.error(f"Stop order failed for {row.Symbol}: {e}")
