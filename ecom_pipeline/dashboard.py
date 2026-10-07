import sys

import pandas as pd

from ecom_pipeline.postgres import get_data_from_postgres


def _in_streamlit() -> bool:
    try:
        from streamlit.runtime import exists

        return exists()
    except ImportError:
        return False


def main():
    """Entry point: `uv run dashboard [args...]`"""
    from streamlit.web import cli as stcli

    sys.argv = ["streamlit", "run", __file__, *sys.argv[1:]]
    raise SystemExit(stcli.main())


def build_top_ten_products_section(st):
    st.header("Top Ten Products by Revenue of Yesterday")
    rows = get_data_from_postgres("top_ten_products")
    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "product": st.column_config.TextColumn("Product"),
            "revenue": st.column_config.NumberColumn(
                "Revenue",
                format="$%.2f",  # 11314.24 -> $11314.24
            ),
        },
    )


def build_low_stock_section(st):
    st.header("Low Stock")
    rows = get_data_from_postgres("low_stock_products")
    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", hide_index=True)


def build_alerts_section(st):
    st.header("Alerts")
    rows = get_data_from_postgres("alerts")
    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", hide_index=True)


# --- Streamlit UI runs here ---
if _in_streamlit():
    import streamlit as st
    from streamlit_autorefresh import st_autorefresh

    st_autorefresh(interval=2000, key="data_refresh")

    st.title("Ecommerce Dashboard")
    build_top_ten_products_section(st)
    build_low_stock_section(st)
    build_alerts_section(st)
