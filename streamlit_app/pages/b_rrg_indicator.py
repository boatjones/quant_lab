"""
RRG Indicator Page for quant_lab Streamlit App

Project Structure:
quant_lab/
├── util/
│   └── to_postgres.py
└── streamlit_app/
    ├── home.py
    └── pages/
        └── rrg_indicator.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pathlib import Path
import sys
from datetime import datetime, timedelta

# Add project root to path
# Use .resolve() to get absolute path first (fixes Streamlit path resolution issues)
project_root = Path(__file__).resolve().parents[2]  # Go up 2 levels: pages -> streamlit_app -> quant_lab
sys.path.insert(0, str(project_root))

try:
    from util.to_postgres import PgHook
except ImportError as e:
    st.error(f"Cannot import PgHook: {e}")
    st.error(f"Project root: {project_root}")
    st.error(f"Expected path: {project_root / 'util' / 'to_postgres.py'}")
    st.stop()

# =============================================================================
# CONFIGURATION
# =============================================================================

# Try to load from config file
try:
    import config
    OHLCV_TABLE = config.OHLCV_TABLE
    DATE_COL = config.DATE_COL
    SYMBOL_COL = config.SYMBOL_COL
    CLOSE_COL = config.CLOSE_COL
    TICKERS_FILE = config.TICKERS_FILE
    BENCHMARK = config.BENCHMARK
    DEFAULT_SHEET = getattr(config, 'DEFAULT_SHEET', 'Sectors')
    TICKER_SHEETS = getattr(config, 'TICKER_SHEETS', {})
    SHEET_BENCHMARKS = getattr(config, 'SHEET_BENCHMARKS', {})
except (ImportError, AttributeError) as e:
    # Fallback to defaults
    OHLCV_TABLE = 'ohlcv'
    DATE_COL = 'trade_date'
    SYMBOL_COL = 'ticker'
    CLOSE_COL = 'price_close'
    TICKERS_FILE = 'Tickers.xlsx'
    BENCHMARK = '^STOXX'
    DEFAULT_SHEET = 'Sectors'
    TICKER_SHEETS = {}
    SHEET_BENCHMARKS = {}
    st.warning(f"⚠️ Using default configuration. Create config.py in project root for custom settings. ({e})")

WINDOW = 14

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="RRG Indicator",
    page_icon="📊",
    layout="wide"
)

# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================

if 'rrg_data_loaded' not in st.session_state:
    st.session_state.rrg_data_loaded = False
if 'rrg_current_date_idx' not in st.session_state:
    st.session_state.rrg_current_date_idx = 5
if 'rrg_selected_tickers' not in st.session_state:
    st.session_state.rrg_selected_tickers = []

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

@st.cache_resource
def get_db_connection():
    """Get database connection (cached across all pages)"""
    try:
        return PgHook()
    except Exception as e:
        st.error(f"Failed to create PgHook: {e}")
        return None


@st.cache_data(ttl=3600)
def load_tickers_from_excel(filepath, sheet_name):
    """Load tickers from Excel file"""
    try:
        # Try multiple path resolutions
        paths_to_try = [
            Path(filepath),  # As provided
            project_root / filepath,  # Relative to project root
            Path(__file__).parent.parent / filepath,  # Relative to streamlit_app
        ]
        
        for path in paths_to_try:
            if path.exists():
                df = pd.read_excel(path, sheet_name=sheet_name)
                filtered_df = df[df["Show"].notna()]
                return filtered_df["Ticker"].tolist()
        
        st.error(f"Could not find {filepath} in any of these locations:")
        for p in paths_to_try:
            st.error(f"  - {p}")
        return []
        
    except Exception as e:
        st.error(f"Failed to load tickers: {e}")
        return []


@st.cache_data(ttl=3600)
def load_data_from_postgres(_db, symbols, start_date=None, end_date=None):
    """Load price data from PostgreSQL"""
    if _db is None:
        return pd.DataFrame()
    
    placeholders = ','.join([f"'{s}'" for s in symbols])
    
    query = f"""
    SELECT {DATE_COL}::date as date, {SYMBOL_COL} as symbol, {CLOSE_COL} as close
    FROM {OHLCV_TABLE}
    WHERE {SYMBOL_COL} IN ({placeholders})
    """
    
    if start_date:
        query += f" AND {DATE_COL} >= '{start_date}'"
    if end_date:
        query += f" AND {DATE_COL} <= '{end_date}'"
    
    query += f" ORDER BY {DATE_COL}, {SYMBOL_COL}"
    
    df = _db.alc_query(query)
    
    if df.empty:
        return pd.DataFrame()
    
    # DEBUG: Show what we loaded
    st.info(f"Loaded {len(df)} daily records for {df['symbol'].nunique()} tickers")

    # DEBUG: Check each ticker
    ticker_counts = df.groupby('symbol').size()
    st.write("***Daily records per ticker:***")
    st.write(ticker_counts.to_dict())
    
    df['date'] = pd.to_datetime(df['date'])
    df_wide = df.pivot(index='date', columns='symbol', values='close')
    
    # DEBUG: Check pivot result
    st.write(f"**After pivot:** {len(df_wide)} daily dates, {len(df_wide.columns)} tickers")
    st.write(f"**Ticker columns:** {list(df_wide.columns)}")

    # Conditional resampling based on frequency
    use_weekly = st.session_state.get('rrg_use_weekly', True)
    if use_weekly:
        df_wide = df_wide.resample('W-FRI').last()
        frequency_text = "weekly"
    else:
        frequency_text = "daily"

    # DEBUG: Check after resampling
    st.write(f"**After {frequency_text} resample:** {len(df_wide)} dates")

    # DEBUG: Check how many non-NaN values each ticker has
    non_nan_counts = df_wide.notna().sum()
    frequency_text_display = "weekly" if use_weekly else "daily"
    st.write(f"**{frequency_text_display.capitalize()} records per ticker (non-NaN):**")
    st.write(non_nan_counts.to_dict())
    
    # DEBUG: Check for tickers that were completely lost
    lost_tickers = [s for s in symbols if s not in df_wide.columns]
    if lost_tickers:
        st.error(f"❌ Tickers lost during pivot: {lost_tickers}")
    
    return df_wide


def calculate_rrg_metrics(tickers_data, benchmark_data, window=14):
    """Calculate RRG metrics for all tickers"""
    tickers = tickers_data.columns.tolist()
    
    rs_dict = {}
    rsr_dict = {}
    rsr_roc_dict = {}
    rsm_dict = {}
    failed_tickers = {}
    
    for ticker in tickers:
        try:
            ticker_series = tickers_data[ticker]

            # Check for sufficient data
            valid_data = ticker_series.dropna()
            if len(valid_data) < window * 4: # Need at least 4x the window size
                failed_tickers[ticker] = f"Insufficient data: only {len(valid_data)} weeks (need {window * 4}+)"
                continue

            rs = 100 * (ticker_series / benchmark_data)

            # Check if RS calculation succeeded
            if rs.isna().all():
                failed_tickers[ticker] = "All RS values are NaN"
                continue

            rsr = (
                100 + (rs - rs.rolling(window=window).mean()) / rs.rolling(window=window).std(ddof=0)
            ).dropna()

            if len(rsr) < window:
                failed_tickers[ticker] = f"Insufficient RSR data: only {len(rsr)} weeks after calculation"
                continue

            rsr_roc = 100 * ((rsr / rsr.iloc[1]) - 1)
            rsm = (
                101 + (
                    (rsr_roc - rsr_roc.rolling(window=window).mean()) / rsr_roc.rolling(window=window).std(ddof=0)
                )
            ).dropna()

            if len(rsm) < 1:
                failed_tickers[ticker] = "No RSM data after calculation"
                continue

            common_dates = rsr.index.intersection(rsm.index)
            if len(common_dates) < 1:
                failed_tickers[ticker] = "No overlapping dates between RSR and RSM"
                continue

            rsr = rsr[rsr.index.isin(common_dates)]
            rsm = rsm[rsm.index.isin(common_dates)]

            rs_dict[ticker] = rs
            rsr_dict[ticker] = rsr
            rsr_roc_dict[ticker] = rsr_roc
            rsm_dict[ticker] = rsm

        except Exception as e:
            failed_tickers[ticker] = f"Calculation error: {str(e)}"

    
    return rs_dict, rsr_dict, rsr_roc_dict, rsm_dict, failed_tickers


def get_status(x, y):
    """Determine quadrant"""
    if x < 100 and y < 100:
        return "Lagging"
    elif x > 100 and y > 100:
        return "Leading"
    elif x < 100 and y > 100:
        return "Improving"
    elif x > 100 and y < 100:
        return "Weakening"


def get_color(x, y):
    """Get color based on quadrant"""
    status = get_status(x, y)
    color_map = {
        "Lagging": "#FF4444",
        "Leading": "#44FF44",
        "Improving": "#4444FF",
        "Weakening": "#FFFF44"
    }
    return color_map.get(status, "gray")


def create_rrg_plot(rsr_dict, rsm_dict, tickers, tail, end_date_idx, selected_tickers):
    """Create the RRG plot using Plotly"""
    fig = go.Figure()
    
    # Add quadrant backgrounds
    fig.add_shape(type="rect", x0=94, y0=94, x1=100, y1=100,
                  fillcolor="red", opacity=0.2, line_width=0)
    fig.add_shape(type="rect", x0=100, y0=94, x1=106, y1=100,
                  fillcolor="yellow", opacity=0.2, line_width=0)
    fig.add_shape(type="rect", x0=100, y0=100, x1=106, y1=106,
                  fillcolor="green", opacity=0.2, line_width=0)
    fig.add_shape(type="rect", x0=94, y0=100, x1=100, y1=106,
                  fillcolor="blue", opacity=0.2, line_width=0)
    
    # Add reference lines
    fig.add_hline(y=100, line_dash="dash", line_color="black")
    fig.add_vline(x=100, line_dash="dash", line_color="black")
    
    # Add labels
    fig.add_annotation(x=95, y=105, text="Improving", showarrow=False)
    fig.add_annotation(x=104, y=105, text="Leading", showarrow=False)
    fig.add_annotation(x=104, y=95, text="Weakening", showarrow=False)
    fig.add_annotation(x=95, y=95, text="Lagging", showarrow=False)
    
    # Plot tickers
    for ticker in tickers:
        if ticker not in selected_tickers:
            continue

        if ticker not in rsr_dict or ticker not in rsm_dict:
            continue
            
        rsr = rsr_dict[ticker]
        rsm = rsm_dict[ticker]
        
        end_idx = min(end_date_idx, len(rsr) - 1)
        start_idx = max(0, end_idx - tail + 1)
        
        rsr_window = rsr.iloc[start_idx:end_idx + 1]
        rsm_window = rsm.iloc[start_idx:end_idx + 1]
        
        if len(rsr_window) == 0:
            continue
        
        color = get_color(rsr_window.iloc[-1], rsm_window.iloc[-1])
        
        fig.add_trace(go.Scatter(
            x=rsr_window.values,
            y=rsm_window.values,
            mode='lines+markers',
            name=ticker,
            line=dict(color=color, width=2),
            marker=dict(
                size=[8] * (len(rsr_window) - 1) + [15],
                color=color
            ),
            text=[ticker] * len(rsr_window),
            hovertemplate='<b>%{text}</b><br>RSR: %{x:.2f}<br>RSM: %{y:.2f}<extra></extra>'
        ))
        
        fig.add_annotation(
            x=rsr_window.iloc[-1],
            y=rsm_window.iloc[-1],
            text=ticker,
            showarrow=False,
            font=dict(size=10, color="black"),
            bgcolor="rgba(255, 255, 255, 0.7)",
            xshift=15,
            yshift=10
        )
    
    fig.update_layout(
        title="Relative Rotation Graph (RRG)",
        xaxis_title="JdK RS Ratio",
        yaxis_title="JdK RS Momentum",
        xaxis=dict(range=[94, 106]),
        yaxis=dict(range=[94, 106]),
        height=600,
        showlegend=True,
        hovermode='closest'
    )
    
    return fig


def create_performance_table(tickers_data, rsr_dict, rsm_dict, tickers, current_date):
    """Create performance summary table"""
    data = []
    
    for ticker in tickers:
        try:
            rsr = rsr_dict[ticker].loc[:current_date].iloc[-1]
            rsm = rsm_dict[ticker].loc[:current_date].iloc[-1]
            price = tickers_data[ticker].loc[:current_date].iloc[-1]
            
            if len(tickers_data[ticker].loc[:current_date]) >= 2:
                prev_price = tickers_data[ticker].loc[:current_date].iloc[-2]
                chg = ((price - prev_price) / prev_price) * 100
            else:
                chg = 0
            
            status = get_status(rsr, rsm)
            
            data.append({
                'Ticker': ticker,
                'Price': f'${price:.2f}',
                'Change %': f'{chg:+.2f}%',
                'RSR': f'{rsr:.2f}',
                'RSM': f'{rsm:.2f}',
                'Status': status
            })
        except:
            pass
    
    return pd.DataFrame(data)


# =============================================================================
# MAIN APP
# =============================================================================

st.title("📊 Relative Rotation Graph (RRG)")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("⚙️ RRG Configuration")

    # Data frequency selection
    st.subheader("📊 Data Frequency")
    use_weekly = st.checkbox(
        "Use Weekly Data (Recommended)",
        value=True,
        key="rrg_use_weekly",
        help="Weekly data provides smoother signals and follows standard RRG methodology"
    )

    if use_weekly:
        st.caption("📈 Standard RRG - smoother rotation signals")
    else:
        st.caption("⚡ Daily data - faster but noisier signals")
        st.warning("⚠️ Daily RRG may show false rotation signals")

    st.markdown("---")
    
    # Show project info for debugging
    with st.expander("🔍 Debug Info", expanded=False):
        st.code(f"Project root: {project_root}")
        st.code(f"Current file: {__file__}")
        st.code(f"util path: {project_root / 'util'}")
        st.code(f"Tickers path: {TICKERS_FILE}")
    
    # Database connection (shared across pages)
    if 'db' not in st.session_state or st.session_state.db is None:
        if st.button("🔌 Connect to Database"):
            with st.spinner("Connecting to PostgreSQL..."):
                try:
                    st.session_state.db = get_db_connection()
                    if st.session_state.db:
                        st.success("✓ Connected to database")
                    else:
                        st.error("Failed to connect - check logs above")
                except Exception as e:
                    st.error(f"Failed to connect: {e}")
    else:
        st.success("✓ Database connected")
    
    # Load tickers
    if st.session_state.get('db') is not None:
        st.subheader("Tickers")
        
        ticker_source = st.radio(
            "Ticker Source",
            ["From Excel", "Manual Entry"],
            key="rrg_ticker_source"
        )
        
        if ticker_source == "From Excel":
            # Show available sheets with better UI
            try:
                # Try to find the Excel file
                ticker_paths = [
                    Path(TICKERS_FILE),
                    project_root / TICKERS_FILE,
                    Path(__file__).parent.parent / TICKERS_FILE,
                ]
                ticker_file_path = None
                for path in ticker_paths:
                    if path.exists():
                        ticker_file_path = path
                        break
                
                if ticker_file_path:
                    xls = pd.ExcelFile(ticker_file_path)
                    
                    # Get default index
                    try:
                        default_idx = xls.sheet_names.index(DEFAULT_SHEET)
                    except ValueError:
                        default_idx = 0
                    
                    # Create sheet labels (with descriptions if available)
                    sheet_labels = []
                    for sheet in xls.sheet_names:
                        if sheet in TICKER_SHEETS:
                            sheet_labels.append(f"{sheet} - {TICKER_SHEETS[sheet]}")
                        else:
                            sheet_labels.append(sheet)
                    
                    # Sheet selector - more prominent
                    st.markdown("**Select Watchlist:**")
                    selected_label = st.selectbox(
                        "Sheet",
                        options=sheet_labels,
                        index=default_idx,
                        key="rrg_sheet_select",
                        label_visibility="collapsed"
                    )
                    
                    # Extract actual sheet name (before the dash if description present)
                    sheet_name = selected_label.split(' - ')[0]
                    
                    # Show info about selected sheet
                    st.caption(f"📋 Loading from sheet: **{sheet_name}**")
                    
                else:
                    st.error(f"❌ Cannot find {TICKERS_FILE}")
                    st.error("Tried these locations:")
                    for p in ticker_paths:
                        st.code(str(p))
                    sheet_name = DEFAULT_SHEET
            except Exception as e:
                st.error(f"Error reading Excel file: {e}")
                sheet_name = DEFAULT_SHEET
            
            if st.button("📋 Load Tickers from Excel", type="primary"):
                with st.spinner(f"Loading tickers from {sheet_name}..."):
                    tickers = load_tickers_from_excel(TICKERS_FILE, sheet_name)
                    if tickers:
                        st.session_state.rrg_tickers = tickers
                        st.session_state.rrg_selected_tickers = tickers.copy()
                        st.session_state.rrg_current_sheet = sheet_name
                        st.success(f"✓ Loaded {len(tickers)} tickers from **{sheet_name}**")
                    else:
                        st.error("No tickers loaded - check file path and sheet name")
        else:
            manual_tickers = st.text_area(
                "Enter tickers (one per line)",
                value="AAPL\nMSFT\nGOOGL\nAMZN\nMETA",
                key="rrg_manual_tickers"
            )
            if st.button("✅ Use These Tickers"):
                tickers = [t.strip() for t in manual_tickers.split('\n') if t.strip()]
                st.session_state.rrg_tickers = tickers
                st.session_state.rrg_selected_tickers = tickers.copy()
                st.success(f"✓ Using {len(tickers)} tickers")
        
        # Benchmark - auto-switch based on sheet if configured
        st.markdown("**Benchmark:**")
        
        # Get the current sheet if available
        current_sheet = st.session_state.get('rrg_current_sheet', DEFAULT_SHEET)
        
        # Use sheet-specific benchmark if configured, otherwise use default
        default_benchmark = SHEET_BENCHMARKS.get(current_sheet, BENCHMARK)
        
        benchmark = st.text_input(
            "Benchmark Symbol",
            value=default_benchmark,
            key="rrg_benchmark",
            label_visibility="collapsed",
            help=f"Default benchmark for {current_sheet}: {default_benchmark}"
        )
        
        if current_sheet in SHEET_BENCHMARKS and SHEET_BENCHMARKS[current_sheet] != BENCHMARK:
            st.caption(f"💡 Using {benchmark} for {current_sheet} watchlist")
        
        # Date range
        st.subheader("Date Range")
        use_date_filter = st.checkbox("Filter by date range", key="rrg_use_dates")
        if use_date_filter:
            start_date = st.date_input(
                "Start Date", 
                value=datetime.now() - timedelta(days=365),
                key="rrg_start_date"
            )
            end_date = st.date_input(
                "End Date", 
                value=datetime.now(),
                key="rrg_end_date"
            )
        else:
            start_date = None
            end_date = None
        
        # Load data button
        if st.button("🚀 Load Data", type="primary"):
            if 'rrg_tickers' in st.session_state and st.session_state.rrg_tickers:
                with st.spinner("Loading data from PostgreSQL..."):
                    try:
                        all_symbols = st.session_state.rrg_tickers + [benchmark]
                        data = load_data_from_postgres(
                            st.session_state.db,
                            all_symbols,
                            start_date.strftime('%Y-%m-%d') if use_date_filter else None,
                            end_date.strftime('%Y-%m-%d') if use_date_filter else None
                        )
                        
                        if not data.empty:
                            st.session_state.rrg_tickers_data = data[st.session_state.rrg_tickers]
                            st.session_state.rrg_benchmark_data = data[benchmark]
                            current_benchmark = st.session_state.get('rrg_benchmark', BENCHMARK)
                            
                            # Calculate RRG metrics
                            # Adjust window for daily vs weekly data
                            use_weekly = st.session_state.get('rrg_use_weekly', True)
                            window_param = WINDOW if use_weekly else WINDOW * 5 # 14 weeks = 70 days

                            rs, rsr, rsr_roc, rsm, failed_tickers = calculate_rrg_metrics(
                                st.session_state.rrg_tickers_data,
                                st.session_state.rrg_benchmark_data,
                                window_param
                            )
                            
                            st.session_state.rrg_rs_dict = rs
                            st.session_state.rrg_rsr_dict = rsr
                            st.session_state.rrg_rsr_roc_dict = rsr_roc
                            st.session_state.rrg_rsm_dict = rsm
                            
                            # Check which tickers have valid RRG data
                            tickers_with_data = [t for t in st.session_state.rrg_tickers if t in rsr and t in rsm]

                            # Update selected tickers to only include those with data
                            st.session_state.rrg_selected_tickers = tickers_with_data.copy()

                            first_ticker = tickers_with_data[0] if tickers_with_data else None
                            if first_ticker:
                                st.session_state.rrg_dates = rsr[first_ticker].index
                                st.session_state.rrg_current_date_idx = len(st.session_state.rrg_dates) - 1

                                st.session_state.rrg_data_loaded = True
                                use_weekly = st.session_state.get('rrg_use_weekly', True)
                                frequency_text = "weeks" if use_weekly else "days"
                                st.success(f"✓ Loaded {len(data)} {frequency_text} of data for {len(tickers_with_data)} tickers")

                            # Show detailed errors for failed tickers
                            if failed_tickers:
                                st.warning(f"⚠️ {len(failed_tickers)} ticker(s) failed RRG calculation:")
                                with st.expander("Show Details"):
                                    for ticker, reason in failed_tickers.items():
                                        st.write(f"**{ticker}:** {reason}")                                        
                        else:
                            st.error("No tickers have sufficient data for RRG analysis")
                            if failed_tickers:
                                with st.expander("Show why tickers failed"):
                                    for ticker, reason in failed_tickers.items():
                                        st.write(f"**{ticker}:** {reason}")

                    except Exception as e:
                        st.error(f"Failed to load data: {e}")
                        import traceback
                        with st.expander("Show Error Details"):
                            st.code(traceback.format_exc())
            else:
                st.warning("Please load tickers first")

# Main content
if st.session_state.rrg_data_loaded:
    
    # Ticker selection
    st.subheader("Select Tickers to Display")
    
    cols = st.columns(5)
    for idx, ticker in enumerate(st.session_state.rrg_tickers):
        col = cols[idx % 5]
        with col:
            if st.checkbox(ticker, value=True, key=f"rrg_cb_{ticker}"):
                if ticker not in st.session_state.rrg_selected_tickers:
                    st.session_state.rrg_selected_tickers.append(ticker)
            else:
                if ticker in st.session_state.rrg_selected_tickers:
                    st.session_state.rrg_selected_tickers.remove(ticker)
    
    st.markdown("---")
    
    # Controls
    col1, col2, col3, col4 = st.columns([2, 1, 1, 2])
    
    with col1:
        use_weekly = st.session_state.get('rrg_use_weekly', True)

        if use_weekly:
            tail = st.slider("Tail Length (weeks)", 1, 20, 10, key="rrg_tail_slider")
        else:
            tail = st.slider("Tail Length (days)", 5, 100, 50, key="rrg_tail_slider")
    
    with col2:
        st.write("")  # Spacing
        if st.button("◀ Previous"):
            if st.session_state.rrg_current_date_idx > tail:
                st.session_state.rrg_current_date_idx -= 1
                st.rerun()
    
    with col3:
        st.write("")  # Spacing
        if st.button("Next ▶"):
            if st.session_state.rrg_current_date_idx < len(st.session_state.rrg_dates) - 1:
                st.session_state.rrg_current_date_idx += 1
                st.rerun()
    
    with col4:
        current_date = st.session_state.rrg_dates[st.session_state.rrg_current_date_idx]
        date_options = [d.strftime('%Y-%m-%d') for d in st.session_state.rrg_dates[tail:]]
        current_date_str = current_date.strftime('%Y-%m-%d')
        
        selected_date = st.selectbox(
            "Date",
            options=date_options,
            index=date_options.index(current_date_str) if current_date_str in date_options else len(date_options) - 1,
            key="rrg_date_select"
        )
        
        new_idx = list(st.session_state.rrg_dates).index(pd.to_datetime(selected_date))
        if new_idx != st.session_state.rrg_current_date_idx:
            st.session_state.rrg_current_date_idx = new_idx
            st.rerun()
    
    # Create and display plot
    fig = create_rrg_plot(
        st.session_state.rrg_rsr_dict,
        st.session_state.rrg_rsm_dict,
        st.session_state.rrg_tickers,
        tail,
        st.session_state.rrg_current_date_idx,
        st.session_state.rrg_selected_tickers
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Performance table
    st.subheader("Current Positions")
    current_date = st.session_state.rrg_dates[st.session_state.rrg_current_date_idx]
    perf_table = create_performance_table(
        st.session_state.rrg_tickers_data,
        st.session_state.rrg_rsr_dict,
        st.session_state.rrg_rsm_dict,
        st.session_state.rrg_selected_tickers,
        current_date
    )
    
    if not perf_table.empty:
        def color_status(val):
            colors = {
                'Leading': 'background-color: #90EE90',
                'Improving': 'background-color: #ADD8E6',
                'Weakening': 'background-color: #FFFFE0',
                'Lagging': 'background-color: #FFB6C1'
            }
            return colors.get(val, '')
        
        styled_table = perf_table.style.map(
            color_status,
            subset=['Status']
        )
        
        st.dataframe(styled_table, use_container_width=True)
    
    # Info
    current_sheet = st.session_state.get('rrg_current_sheet', 'Unknown')
    sheet_description = TICKER_SHEETS.get(current_sheet, '')
    current_benchmark = st.session_state.get('rrg_benchmark', BENCHMARK)
    current_date = st.session_state.rrg_dates[st.session_state.rrg_current_date_idx].strftime('%Y-%m-%d')
    use_weekly = st.session_state.get('rrg_use_weekly', True)

    if use_weekly:
        window_display = f"{WINDOW} weeks"
        freq_display = "Weekly"
    else:
        window_display = f"{WINDOW * 5} days"
        freq_display = "Daily"    
   
    st.info(f"""
    **Watchlist:** {current_sheet} {sheet_description}
    **Date:** {current_date}
    **Frequency:** {freq_display}
    **Benchmark:** {current_benchmark}
    **Window:** {window_display}
    **Tickers:** {len(st.session_state.rrg_selected_tickers)} selected
    """)

else:
    st.info("👈 Configure settings in the sidebar and click 'Load Data' to get started")
    
    st.markdown("""
    ### RRG Quadrants
    
    - 🟢 **Leading** (top right): Strong and getting stronger
    - 🔵 **Improving** (top left): Weak but gaining momentum  
    - 🔴 **Lagging** (bottom left): Weak and getting weaker
    - 🟡 **Weakening** (bottom right): Strong but losing momentum
    """)
