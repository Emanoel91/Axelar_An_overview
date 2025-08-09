import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px
import plotly.graph_objects as go

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar: An Overview",
    page_icon="https://img.cryptorank.io/coins/axelar1663924228506.png",
    layout="wide"
)

# --- Title  -----------------------------------------------------------------------------------------------------
st.title("📐Metrics")

# --- attention ---------------------------------------------------------------------------------------------------------
st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer Slightly Left-Aligned ---
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; # -- MOVE LEFT
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Snowflake Connection ----------------------------------------------------------------------------------------
conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],
    password=st.secrets["snowflake"]["password"],
    account=st.secrets["snowflake"]["account"],
    warehouse="SNOWFLAKE_LEARNING_WH",
    database="AXELAR",
    schema="PUBLIC"
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
start_date = st.date_input("Start Date", value=pd.to_datetime("2023-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))
# --- Query Function: Row1 --------------------------------------------------------------------------------------
@st.cache_data
def load_chain_stats(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH table1 AS (
        SELECT
            COUNT(TX_id) AS "Number of Transactions",
            COUNT(DISTINCT TX_FROM) AS "Number of Unique addresses",
            ROUND(SUM(fee / 1e6)) AS "Total Fees"
        FROM axelar.core.fact_transactions
        WHERE block_timestamp >= '{start_str}'
          AND block_timestamp <= '{end_str}'
    ),
    table2 AS (
        SELECT ROUND(AVG("Block Time Difference"), 2) AS "Average Block Time"
        FROM (
            SELECT
                BLOCK_ID,
                BLOCK_TIMESTAMP,
                LEAD(BLOCK_TIMESTAMP) OVER (ORDER BY BLOCK_ID) AS next_block_timestamp,
                DATEDIFF(second, BLOCK_TIMESTAMP, LEAD(BLOCK_TIMESTAMP) OVER (ORDER BY BLOCK_ID)) AS "Block Time Difference"
            FROM axelar.core.fact_blocks
            WHERE block_timestamp >= '{start_str}'
              AND block_timestamp <= '{end_str}'
        ) subquery
        WHERE "Block Time Difference" IS NOT NULL
    )
    SELECT 
        "Number of Transactions", 
        "Number of Unique addresses", 
        "Total Fees", 
        "Average Block Time"
    FROM table1, table2
    """
    df = pd.read_sql(query, conn)
    return df

df_chain_stats = load_chain_stats(start_date, end_date)

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    label="Number of Transactions",
    value=f"{df_chain_stats['Number of Transactions'][0]:,} Txns"
)

col2.metric(
    label="Number of Unique addresses",
    value=f"{df_chain_stats['Number of Unique addresses'][0]:,} Wallets"
)

col3.metric(
    label="Total Fees",
    value=f"{df_chain_stats['Total Fees'][0]:,} AXL"
)

col4.metric(
    label="Average Block Time",
    value=f"{df_chain_stats['Average Block Time'][0]:,} Sec"
)
