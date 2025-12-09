import streamlit as st
from pathlib import Path
import sys

# Add util to path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from util.to_postgres import PgHook

st.set_page_config(
    page_title="Your Trading App",
    page_icon="📊",
    layout="wide"
)

# Initialize shared database connection
if 'db' not in st.session_state:
    try:
        st.session_state.db = PgHook()
    except:
        st.session_state.db = None

st.title("Welcome to Your Trading Dashboard")
st.markdown("Select a page from the sidebar to get started.")

st.text('Main Launch Page - mostly as a bookmark for now')
