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

# --- Row 2, 3, 4 ---------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_txn_metrics(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH table1 AS (
        SELECT 
            DATE_TRUNC('{timeframe}', block_timestamp_hour) AS "Date", 
            SUM(transaction_count) AS "Number of Txns",
            SUM("Number of Txns") OVER (ORDER BY DATE_TRUNC('{timeframe}', block_timestamp_hour)) AS "Total Number of Txns",
            SUM(transaction_count_success) AS "Number of Successful Transactions",
            SUM(transaction_count_failed) AS "Number of Failed Transactions",
            SUM(total_fees_native) AS "Txn Fees (AXL)",
            SUM(total_fees_usd) AS "Txn Fees (USD)"
        FROM AXELAR.STATS.EZ_CORE_METRICS_HOURLY
        WHERE block_timestamp_hour::date >= '{start_str}'
          AND block_timestamp_hour::date <= '{end_str}'
        GROUP BY 1
    ),
    table2 AS (
        SELECT 
            DATE_TRUNC('{timeframe}', block_timestamp) AS "Date", 
            COUNT(DISTINCT tx_from) AS "Number of Users",
            ROUND(COUNT(DISTINCT tx_id) / COUNT(DISTINCT tx_from)) AS "Avg Txn per User",
            ROUND(AVG(fee / POW(10,6)), 3) AS "Avg Fee (AXL)",
            ROUND(MEDIAN(fee / POW(10,6)), 3) AS "Median Fee (AXL)",
            ROUND(MAX(fee / POW(10,6)), 3) AS "Max Fee (AXL)"
        FROM AXELAR.CORE.FACT_TRANSACTIONS
        WHERE tx_succeeded = 'TRUE'
          AND block_timestamp::date >= '{start_str}'
          AND block_timestamp::date <= '{end_str}'
        GROUP BY 1
    )
    SELECT 
        table1."Date" AS "Date", 
        "Number of Txns", 
        "Total Number of Txns", 
        "Number of Successful Transactions",
        "Number of Failed Transactions",
        "Txn Fees (AXL)", 
        "Txn Fees (USD)", 
        "Number of Users", 
        "Avg Txn per User",
        "Avg Fee (AXL)", 
        "Median Fee (AXL)", 
        "Max Fee (AXL)"
    FROM table1 
    LEFT JOIN table2 
        ON table1."Date" = table2."Date"
    ORDER BY 1
    """
    df = pd.read_sql(query, conn)
    return df

df_txn_metrics = load_txn_metrics(timeframe, start_date, end_date)

# ---- Row 2 ----------------------------------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Bar + Line: Number of Txns & Total Number of Txns
fig1 = go.Figure()
fig1.add_bar(x=df_txn_metrics["Date"], y=df_txn_metrics["Number of Txns"], name="Number of Txns", yaxis="y1")
fig1.add_trace(go.Scatter(x=df_txn_metrics["Date"], y=df_txn_metrics["Total Number of Txns"], name="Total Number of Txns", mode="lines", yaxis="y2"))
fig1.update_layout(
    title="Number of Transactions Over Time",
    yaxis=dict(title="Number of Txns"),
    yaxis2=dict(title="Total Number of Txns", overlaying="y", side="right"),
    barmode="group"
)
col1.plotly_chart(fig1, use_container_width=True)

# Stacked Bar: Successful vs Failed
fig2 = go.Figure()
fig2.add_bar(x=df_txn_metrics["Date"], y=df_txn_metrics["Number of Successful Transactions"], name="Successful Transactions")
fig2.add_bar(x=df_txn_metrics["Date"], y=df_txn_metrics["Number of Failed Transactions"], name="Failed Transactions")
fig2.update_layout(
    title="Successful vs Failed Transactions Over Time",
    barmode="stack",
    yaxis=dict(title="Transactions")
)
col2.plotly_chart(fig2, use_container_width=True)

# ---- Row 3 --------------------------------------------------------------------------------------------------------------------------------------
col3, col4 = st.columns(2)

# Bar + Line: Txn Fees (AXL) & Txn Fees (USD)
fig3 = go.Figure()
fig3.add_bar(x=df_txn_metrics["Date"], y=df_txn_metrics["Txn Fees (AXL)"], name="Txn Fees (AXL)", yaxis="y1")
fig3.add_trace(go.Scatter(x=df_txn_metrics["Date"], y=df_txn_metrics["Txn Fees (USD)"], name="Txn Fees (USD)", mode="lines", yaxis="y2"))
fig3.update_layout(
    title="Transaction Fees Over Time",
    yaxis=dict(title="Fees (AXL)"),
    yaxis2=dict(title="Fees (USD)", overlaying="y", side="right"),
    barmode="group"
)
col3.plotly_chart(fig3, use_container_width=True)

# Bar + Line: Number of Users & Avg Txn per User
fig4 = go.Figure()
fig4.add_bar(x=df_txn_metrics["Date"], y=df_txn_metrics["Number of Users"], name="Number of Users", yaxis="y1")
fig4.add_trace(go.Scatter(x=df_txn_metrics["Date"], y=df_txn_metrics["Avg Txn per User"], name="Avg Txn per User", mode="lines", yaxis="y2"))
fig4.update_layout(
    title="Number of Users Over Time",
    yaxis=dict(title="Number of Users"),
    yaxis2=dict(title="Avg Txn per User", overlaying="y", side="right"),
    barmode="group"
)
col4.plotly_chart(fig4, use_container_width=True)

# ---- Row 4: (Scatter) ----------------------------------------------------------------------------------
col5, col6, col7 = st.columns(3)

# Scatter: Median Gas Fee
fig5 = px.scatter(df_txn_metrics, x="Date", y="Median Fee (AXL)", size="Median Fee (AXL)", title="Median Gas Fee Over Time")
col5.plotly_chart(fig5, use_container_width=True)

# Scatter: Average Gas Fee
fig6 = px.scatter(df_txn_metrics, x="Date", y="Avg Fee (AXL)", size="Avg Fee (AXL)", title="Average Gas Fee Over Time")
col6.plotly_chart(fig6, use_container_width=True)

# Scatter: Max Gas Fee
fig7 = px.scatter(df_txn_metrics, x="Date", y="Max Fee (AXL)", size="Max Fee (AXL)", title="Max Gas Fee Over Time")
col7.plotly_chart(fig7, use_container_width=True)

