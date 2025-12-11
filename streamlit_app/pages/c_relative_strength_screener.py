import streamlit as st
import pandas as pd
from datetime import datetime
import io
from pathlib import Path
import sys

# Add util to path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from util.to_postgres import PgHook

# Page configuration
st.set_page_config(
    page_title="Winners Screener",
    page_icon="🏆",
    layout="wide"
)

# Initialize shared database connection if not exists
if 'db' not in st.session_state:
    try:
        st.session_state.db = PgHook()
    except:
        st.session_state.db = None
        st.error("Failed to connect to database")

# Title and description
st.title("🏆 Winners Screener")
st.markdown("""
This screener identifies high-quality stocks based on:
- **Relative Strength**: Momentum over a variable lookback period
- **Fundamentals**: Market cap, EBIT, and revenue growth
- **Price**: Minimum stock price threshold
""")

# Sidebar for filters
st.sidebar.header("Filter Criteria")

# Minimum Stock Price
min_price = st.sidebar.slider(
    "Minimum Stock Price ($)",
    min_value=10,
    max_value=150,
    value=35,
    step=5,
    help="Filter stocks below this price"
)

# Minimum Market Cap
min_market_cap_millions = st.sidebar.slider(
    "Minimum Market Cap ($M)",
    min_value=10,
    max_value=1000,
    value=50,
    step=10,
    help="Filter companies with market cap below this threshold"
)
min_market_cap = min_market_cap_millions * 1_000_000

# RS Lookback Period
lookback_months = st.sidebar.slider(
    "RS Lookback Period (months)",
    min_value=6,
    max_value=36,
    value=12,
    step=3,
    help="Period over which to calculate relative strength"
)
# Convert months to approximate trading days (~21 per month)
lookback_days = int(lookback_months * 21)

# Minimum RS Percentile
min_rs_percentile = st.sidebar.slider(
    "Minimum RS Percentile",
    min_value=50,
    max_value=95,
    value=80,
    step=5,
    help="Only show stocks in this percentile or higher for relative strength"
)

# Minimum EBIT
min_ebit_millions = st.sidebar.slider(
    "Minimum EBIT ($M)",
    min_value=1,
    max_value=100,
    value=10,
    step=5,
    help="Minimum quarterly EBIT threshold"
)
min_ebit = min_ebit_millions * 1_000_000

# Minimum Revenue CAGR
min_revenue_cagr_pct = st.sidebar.slider(
    "Minimum Revenue 3Y CAGR (%)",
    min_value=0,
    max_value=50,
    value=10,
    step=5,
    help="Minimum 3-year compound annual revenue growth rate"
)
min_revenue_cagr = min_revenue_cagr_pct / 100.0

# Display current filter summary
st.sidebar.markdown("---")
st.sidebar.markdown("### Current Filters")
st.sidebar.markdown(f"""
- Price: **≥ ${min_price}**
- Market Cap: **≥ ${min_market_cap_millions}M**
- RS Lookback: **{lookback_months} months** ({lookback_days} days)
- RS Percentile: **≥ {min_rs_percentile}**
- EBIT: **≥ ${min_ebit_millions}M**
- Revenue CAGR: **≥ {min_revenue_cagr_pct}%**
""")

# Run screener button
st.sidebar.markdown("---")
run_button = st.sidebar.button("🚀 Run Screener", type="primary", use_container_width=True)

# Main content area
if run_button:
    if st.session_state.db is None:
        st.error("❌ Database connection not available. Please refresh the page.")
    else:
        try:
            with st.spinner("Running screener... This may take a moment."):
                # Call the stored procedure with parameters
                query = f"""
                    SELECT * FROM screener_winners(
                        {min_price},
                        {min_market_cap},
                        {lookback_days},
                        {min_ebit},
                        {min_revenue_cagr},
                        {min_rs_percentile}
                    )
                """
                
                # Execute query using PgHook
                df = st.session_state.db.psy_query(query)
            
            # Check if results returned
            if df.empty:
                st.warning("⚠️ No stocks match the current filter criteria. Try relaxing some filters.")
            else:
                # Display results count
                st.success(f"✅ Found **{len(df)}** stocks matching criteria")
                
                # Format the dataframe for display
                df_display = df.copy()
                
                # Format market cap in millions
                df_display['market_cap'] = df_display['market_cap'].apply(lambda x: f"${x/1_000_000:.1f}M")
                
                # Format EBIT in millions
                df_display['ebit'] = df_display['ebit'].apply(lambda x: f"${x/1_000_000:.1f}M")
                
                # Format price
                df_display['current_price'] = df_display['current_price'].apply(lambda x: f"${x:.2f}")
                
                # Format percentiles
                df_display['rs_percentile'] = df_display['rs_percentile'].apply(lambda x: f"{x:.1f}")
                
                # Format CAGR as percentage
                df_display['revenue_cagr_3y'] = df_display['revenue_cagr_3y'].apply(lambda x: f"{x*100:.1f}%")
                
                # Rename columns for display
                df_display.columns = [
                    'Ticker', 'Company', 'Sector', 'Industry', 'Exchange',
                    'Price', 'Market Cap', 'RS %ile', 'EBIT', 'Rev CAGR 3Y'
                ]
                
                # Display the dataframe
                st.dataframe(
                    df_display,
                    use_container_width=True,
                    height=600
                )
                
                # Export section
                st.markdown("---")
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    st.markdown("### Export Results")
                
                with col2:
                    # Excel export
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='Winners')
                    output.seek(0)
                    
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"winners_{timestamp}.xlsx"
                    
                    st.download_button(
                        label="📥 Download Excel",
                        data=output,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                
                with col3:
                    # CSV export as alternative
                    csv = df.to_csv(index=False)
                    filename_csv = f"winners_{timestamp}.csv"
                    
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv,
                        file_name=filename_csv,
                        mime="text/csv",
                        use_container_width=True
                    )
                
                # Summary statistics
                st.markdown("---")
                st.markdown("### Summary Statistics")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    avg_rs = df['rs_percentile'].mean()
                    st.metric("Avg RS Percentile", f"{avg_rs:.1f}")
                
                with col2:
                    median_price = df['current_price'].median()
                    st.metric("Median Price", f"${median_price:.2f}")
                
                with col3:
                    avg_cagr = df['revenue_cagr_3y'].mean() * 100
                    st.metric("Avg Revenue CAGR", f"{avg_cagr:.1f}%")
                
                with col4:
                    total_market_cap = df['market_cap'].sum() / 1_000_000_000
                    st.metric("Total Market Cap", f"${total_market_cap:.1f}B")
                
                # Sector distribution
                st.markdown("---")
                st.markdown("### Sector Distribution")
                sector_counts = df['sector'].value_counts()
                st.bar_chart(sector_counts)
    
        except Exception as e:
            st.error(f"❌ Error running screener: {str(e)}")
            st.markdown("**Troubleshooting:**")
            st.markdown("- Ensure the stored procedure `screener_winners()` is installed")
            st.markdown("- Check database connection")
            st.markdown("- Verify all required tables exist (stocks, ohlcv, daily_log_returns, fundamentals, fundamental_ratios)")

else:
    # Initial state - show instructions
    st.info("👈 Configure your filter criteria in the sidebar and click **Run Screener** to begin.")
    
    st.markdown("---")
    st.markdown("### How It Works")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Relative Strength (RS)**
        - Calculated as the sum of daily log returns over the lookback period
        - Ranked as a percentile (0-100) against all stocks
        - Higher percentile = stronger momentum
        """)
        
        st.markdown("""
        **Fundamental Filters**
        - **Market Cap**: Company size filter
        - **EBIT**: Profitability threshold (quarterly)
        - **Revenue CAGR**: 3-year growth rate
        """)
    
    with col2:
        st.markdown("""
        **Typical Lookback Periods**
        - **6 months**: Short-term momentum
        - **12 months**: Medium-term trend (default)
        - **24+ months**: Long-term strength
        """)
        
        st.markdown("""
        **Export Options**
        - Excel format for further analysis
        - CSV for data portability
        - Includes all fundamental data
        """)
