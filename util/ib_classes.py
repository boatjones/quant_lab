import pandas as pd
import asyncio
import time
import os
import sys
import functools, builtins
from datetime import datetime
from ib_insync import IB, Stock, MarketOrder, LimitOrder, StopOrder, Order, util

print(f"[DEBUG] Current working directory: {os.getcwd()}", flush=True)

# Ensure every print flushes immediately for debugging
builtins.print = functools.partial(builtins.print, flush=True)
# sys.stdout.reconfigure(line_buffering=True)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_FILE = os.path.join(BASE_DIR, "IB_orders.log")
DEBUG_FILE = os.path.join(BASE_DIR, "IB_debug.log")

# Ensure event loop exists for Streamlit threads
try:
    util.startLoop()
except RuntimeError:
    pass

ib = IB()

# --- Helper Class Section ---
def ensure_connected(ib_instance):
    if not ib_instance.isConnected():
        cid = getattr(ib_instance.client, "clientId", 3)
        debug(f"Connecting to IB with clientId={cid}")
        ib_instance.connect("127.0.0.1", 7497, clientId=cid)
        time.sleep(0.25)

def log_order(action, ticker, quantity, order_type, price=None, extra=None, client_id=None):
    """Append an order log entry with timestamp to a local file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = (f"[{timestamp}] "
           f"ClientID={client_id or '?'} | {action} {quantity} {ticker} "
           f"Type={order_type}")
    if price is not None:
        msg += f" @ {price}"
    if extra:
        msg += f" | {extra}"
    with open(LOG_FILE, "a") as f:
        f.write(msg + "\n")
    print(msg)  # also prints to Streamlit/console for live feedback

def debug(msg: str):
    """Write debug messages to both file and Streamlit (if available)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    text = f"[{timestamp}] {msg}"

    # --- Always write to disk ---
    try:
        with open(DEBUG_FILE, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception as e:
        print(f"[DEBUG ERROR] Could not write to {DEBUG_FILE}: {e}", flush=True)

    # --- Always echo to terminal (for streamlit run) ---
    print(text, flush=True)

    # --- Append to Streamlit session if running ---
    try:
        import streamlit as st
        if "debug_lines" not in st.session_state:
            st.session_state["debug_lines"] = []
        st.session_state["debug_lines"].append(text)
    except Exception:
        pass

#####################################################################################################################
class marketEqOrder:
    ''' Class for a Stock Market Order
        params: ticker, b_s, quantity, currency (optional/USD default)
        functions: execTrade, tradeStatus
    '''
    def __init__(self, ticker, b_s, quantity, currency='USD', ib_instance=None):
        self.ib = ib_instance or ib  # use provided one, or default
        self.ticker = ticker
        if b_s == 'B':        
            self.b_s = 'BUY'
        elif b_s == 'S':
            self.b_s = 'SELL'
        else:
            print('Error: direction must be "B" or "S"')
            self.b_s = 'SELL'
        self.quantity = quantity
        self.currency = currency
        self.contract = Stock(self.ticker, "SMART", self.currency)
        debug(f"Prepared market order: {self.ticker} {self.b_s} {self.quantity}")

    def __repr__(self):
        return 'Market order: ticker {}, direction {}, quantity {}'.format(self.ticker, self.b_s, self.quantity)

    def execTrade(self):
        ensure_connected(self.ib)
        self.order = MarketOrder(action=self.b_s, totalQuantity=self.quantity)
        debug(f"Starting execTrade for {self.ticker} ({self.b_s} {self.quantity})")
        self.trade = self.ib.placeOrder(self.contract, self.order)
        debug(f"Placed order {self.order} → tradeId={getattr(self.trade, 'tradeId', '?')}")

        log_order(
            action=self.b_s,
            ticker=self.ticker,
            quantity=self.quantity,
            order_type=self.__class__.__name__,
            price=getattr(self, "limit_price", None) or getattr(self, "stop_price", None),
            extra=f"Trail%={getattr(self, 'trailing_percent', None)} TrailStop={getattr(self, 'trail_stop_price', None)}",
            client_id=self.ib.client.clientId
        )
       
        # print(self.trade.log)
        return self.trade.log

    def tradeStatus(self):
        status = self.trade.orderStatus.status
        fill = self.trade.orderStatus.avgFillPrice
        debug(f'Trade status: {status}.  Average Fill Price: {fill}')

#####################################################################################################################
class limitEqOrder:
    ''' Class for a Stock Limit Order
        params: ticker, b_s, quantity, limit_price, time_force (default='GTC'), currency (optional/USD default)
        functions: execTrade, tradeStatus, cancelOrder
    '''
    def __init__(self, ticker, b_s, quantity, limit_price, time_force='GTC', currency='USD', ib_instance=None):
        self.ib = ib_instance or ib  # use provided one, or default
        self.ticker = ticker
        if b_s == 'B':        
            self.b_s = 'BUY'
        elif b_s == 'S':
            self.b_s = 'SELL'
        else:
            print('Error: direction must be "B" or "S"')
            self.b_s = 'SELL'
        self.quantity = quantity
        self.limit_price = limit_price
        self.time_force = time_force
        self.currency = currency
        self.contract = Stock(self.ticker, "SMART", self.currency)
        debug(f"Prepared limit order: {self.ticker} {self.b_s} {self.quantity} {self.limit_price} {self.time_force}")

    def __repr__(self):
        return 'Limit order: ticker {}, direction {}, quantity {}, limit price {}, time {}'.format(self.ticker, self.b_s, self.quantity, self.limit_price, self.time_force)

    def execTrade(self):
        debug(f"[DEBUG] Starting execTrade for {self.ticker}")
        ensure_connected(self.ib)

        debug("[DEBUG] Placing order...")
        self.order = LimitOrder(
            action=self.b_s, 
            totalQuantity=self.quantity, 
            lmtPrice=self.limit_price, 
            tif=self.time_force
        )
        self.trade = self.ib.placeOrder(self.contract, self.order)
        debug(f"[DEBUG] placeOrder() returned Trade object {self.trade}")

        log_order(
            action=self.b_s,
            ticker=self.ticker,
            quantity=self.quantity,
            order_type=self.__class__.__name__,
            price=getattr(self, "limit_price", None) or getattr(self, "stop_price", None),
            extra=f"Trail%={getattr(self, 'trailing_percent', None)} TrailStop={getattr(self, 'trail_stop_price', None)}",
            client_id=self.ib.client.clientId
        )        
        
        # print(self.trade.log)
        return self.trade.log
    
    def cancelOrder(self):
        ensure_connected(self.ib)
        self.ib.cancelOrder(self.order)

    def modifyOrder(self, b_s=None, quantity=None, limit_price=None, time_force=None):
        ensure_connected(self.ib)
        self.ib.cancelOrder(self.order)
        
        if b_s is not None:
            self.b_s = b_s
        if quantity is not None:
            self.quantity = quantity
        if limit_price is not None:
            self.limit_price = limit_price
        if time_force is not None:
            self.time_force = time_force
        self.execTrade()

    def tradeStatus(self):
        status = self.trade.orderStatus.status
        fill = self.trade.orderStatus.avgFillPrice
        debug(f'Trade status: {status}.  Average Fill Price: {fill}')

#####################################################################################################################
class stopEqOrder:
    ''' Class for a Stock Stop Loss Order
        params: ticker, b_s, quantity, limit_price, time_force (default='GTC'), currency (optional/USD default)
        functions: execTrade, tradeStatus, cancelOrder
    '''
    def __init__(self, ticker, b_s, quantity, stop_price, time_force='GTC', currency='USD', ib_instance=None):
        self.ib = ib_instance or ib  # use provided one, or default
        self.ticker = ticker
        if b_s == 'B':        
            self.b_s = 'BUY'
        elif b_s == 'S':
            self.b_s = 'SELL'
        else:
            print('Error: direction must be "B" or "S"')
            self.b_s = 'SELL'
        self.quantity = quantity
        self.stop_price = stop_price
        self.time_force = time_force
        self.currency = currency
        self.contract = Stock(self.ticker, "SMART", self.currency)
        debug(f"Prepared stop loss order: {self.ticker} {self.b_s} {self.quantity} {self.stop_price} {self.time_force}")

    def __repr__(self):
        return 'Stop order: ticker {}, direction {}, quantity {}, stop price {}, time {}'.format(self.ticker, self.b_s, self.quantity, self.stop_price, self.time_force)

    def execTrade(self):
        ensure_connected(self.ib)
        self.order = StopOrder(action=self.b_s, totalQuantity=self.quantity, stopPrice=self.stop_price, tif=self.time_force)
        self.trade = self.ib.placeOrder(self.contract, self.order)
        log_order(
            action=self.b_s,
            ticker=self.ticker,
            quantity=self.quantity,
            order_type=self.__class__.__name__,
            price=getattr(self, "limit_price", None) or getattr(self, "stop_price", None),
            extra=f"Trail%={getattr(self, 'trailing_percent', None)} TrailStop={getattr(self, 'trail_stop_price', None)}",
            client_id=self.ib.client.clientId
        )        

        return self.trade.log
    
    def cancelOrder(self):
        ensure_connected(self.ib)
        self.ib.cancelOrder(self.order)

    def modifyOrder(self, b_s=None, quantity=None, stop_price=None, time_force=None):
        ensure_connected(self.ib)
        self.ib.cancelOrder(self.order)
        
        if b_s is not None:
            self.b_s = b_s
        if quantity is not None:
            self.quantity = quantity
        if stop_price is not None:
            self.stop_price = stop_price
        if time_force is not None:
            self.time_force = time_force        
        self.execTrade()

    def tradeStatus(self):
        status = self.trade.orderStatus.status
        fill = self.trade.orderStatus.avgFillPrice
        debug(f'Trade status: {status}.  Average Fill Price: {fill}')
#####################################################################################################################
class trailStopEqOrder:
    ''' Class for a Stock Trailing Stop Order
        params: ticker, b_s, quantity,
                trailing_percent (optional), trailing_amount (optional),
                trail_stop_price (optional), time_force (default='GTC'),
                currency (default='USD')

        functions: execTrade, tradeStatus, cancelOrder, modifyOrder
    '''
    def __init__(self, ticker, b_s, quantity, trailing_percent=None, trailing_amount=None,
                 trail_stop_price=None, time_force='GTC', currency='USD', ib_instance=None):
        self.ib = ib_instance or ib  # use provided one, or default
        self.ticker = ticker
        
        if b_s == 'B':        
            self.b_s = 'BUY'
        elif b_s == 'S':
            self.b_s = 'SELL'
        else:
            debug('Error: direction must be "B" or "S"')
            self.b_s = 'SELL'

        # assign inputs to attributes
        self.quantity = quantity
        self.trailing_percent = trailing_percent
        self.trailing_amount = trailing_amount
        self.trail_stop_price = trail_stop_price
        self.time_force = time_force
        self.currency = currency
        
        # Ensure at least one trailing parameter is set
        if (self.trailing_percent or 0) == 0 and (self.trail_stop_price or 0) == 0:
            raise ValueError("You must specify either trailing_percent or trail_stop_price.")

        # Case 3: both provided (combined trailing stop)
        if (self.trailing_percent or 0) > 0 and (self.trail_stop_price or 0) > 0:
            debug(
                f"Notice: Both trailing_percent ({self.trailing_percent}%) and "
                f"trail_stop_price (${self.trail_stop_price}) provided — submitting combined trailing stop."
            )

        self.contract = Stock(self.ticker, "SMART", self.currency)

        debug(f'Order preview - Direction: {self.b_s}, Quantity: {self.quantity}, '
              f'Trailing %: {self.trailing_percent}, Trailing $: {self.trailing_amount}, '
              f'Trail Stop Price: {self.trail_stop_price}, '
              f'Time: {self.time_force}, Currency: {self.currency}')

    def __repr__(self):
        trail_info = (f'{self.trailing_percent}%' if self.trailing_percent
                      else f'${self.trailing_amount}')
        return (f'Trailing stop order: ticker {self.ticker}, direction {self.b_s}, '
                f'quantity {self.quantity}, trail {trail_info}, time {self.time_force}')

    def execTrade(self):
        ensure_connected(self.ib)
        # Generic order definition for all ib_insync versions
        self.order = Order(
            action=self.b_s,
            orderType='TRAIL',
            totalQuantity=self.quantity,
            tif=self.time_force
        )

        # --- CASE 1: trailing percent only ---
        if (self.trailing_percent or 0) > 0 and (self.trail_stop_price or 0) == 0:
            self.order.trailingPercent = self.trailing_percent
            debug(f"{self.ticker}: Trailing % only ({self.trailing_percent}%)")

        # --- CASE 2: trail stop price only (fixed-price stop) ---
        elif (self.trail_stop_price or 0) > 0 and (self.trailing_percent or 0) == 0:
            self.order.trailStopPrice = self.trail_stop_price
            debug(f"{self.ticker}: Trail Stop Price only (${self.trail_stop_price})")

        # --- CASE 3: both provided (initial anchor + percent trail) ---
        elif (self.trail_stop_price or 0) > 0 and (self.trailing_percent or 0) > 0:
            self.order.trailStopPrice = self.trail_stop_price
            self.order.trailingPercent = self.trailing_percent
            debug(
                f"{self.ticker}: Combined Trail Stop ${self.trail_stop_price} + "
                f"{self.trailing_percent}% trailing"
            )
        else:
            raise ValueError(
                f"{self.ticker}: Invalid trailing stop parameters - "
                "you must specify at least one of trail_stop_price or trailing_percent"
            )
        
        # Place the order
        self.trade = self.ib.placeOrder(self.contract, self.order)

        log_order(
            action=self.b_s,
            ticker=self.ticker,
            quantity=self.quantity,
            order_type=self.__class__.__name__,
            price=getattr(self, "trail_stop_price", None),
            extra=f"Trail%={self.trailing_percent} TrailStop={self.trail_stop_price}",
            client_id=self.ib.client.clientId
        )        
        
        debug(f"{self.ticker}: TRAIL order submitted - {self.order}")
        return self.trade.log

    def cancelOrder(self):
        ensure_connected(self.ib)
        self.ib.cancelOrder(self.order)

    def modifyOrder(self, b_s=None, quantity=None, trailing_percent=None,
                    trailing_amount=None, trail_stop_price=None, time_force=None):
        ensure_connected(self.ib)
        self.ib.cancelOrder(self.order)
        
        if b_s is not None:
            self.b_s = b_s
        if quantity is not None:
            self.quantity = quantity
        if trailing_percent is not None:
            self.trailing_percent = trailing_percent
            self.trailing_amount = None  # reset the other type
        if trailing_amount is not None:
            self.trailing_amount = trailing_amount
            self.trailing_percent = None
        if trail_stop_price is not None:
            self.trail_stop_price = trail_stop_price
        if time_force is not None:
            self.time_force = time_force        

        self.execTrade()

    def tradeStatus(self):
        status = self.trade.orderStatus.status
        fill = self.trade.orderStatus.avgFillPrice
        debug(f'Trade status: {status}.  Average Fill Price: {fill}')

