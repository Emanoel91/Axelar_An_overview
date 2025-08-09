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
st.title("🟡Squid")

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
start_date = st.date_input("Start Date", value=pd.to_datetime("2024-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))
# --- Query Function: Row1 --------------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_kpi = load_kpi_data(timeframe, start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

col1.metric(
    label="Volume of Transfers",
    value=f"${df_kpi['VOLUME_OF_TRANSFERS'][0]:,}"
)

col2.metric(
    label="Number of Transfers",
    value=f"{df_kpi['NUMBER_OF_TRANSFERS'][0]:,} Txns"
)

col3.metric(
    label="Number of Users",
    value=f"{df_kpi['NUMBER_OF_USERS'][0]:,} Addresses"
)

# --- Query Function: Row (2) --------------------------------------------------------------------------------------
@st.cache_data
def load_time_series_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        DATE_TRUNC('{timeframe}', created_at) AS Date,
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    GROUP BY 1
    ORDER BY 1
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_ts = load_time_series_data(timeframe, start_date, end_date)

# --- Charts in One Row ---------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    fig1 = px.bar(
        df_ts,
        x="DATE",
        y="VOLUME_OF_TRANSFERS",
        title="Squid Bridge Volume Over Time (USD)",
        labels={"VOLUME_OF_TRANSFERS": "Volume (USD)", "DATE": "Date"},
        color_discrete_sequence=["#e2fb43"]
    )
    fig1.update_layout(xaxis_title="", yaxis_title="USD", bargap=0.2)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(
        df_ts,
        x="DATE",
        y="NUMBER_OF_TRANSFERS",
        title="Squid Bridge Transactions Over Time",
        labels={"NUMBER_OF_TRANSFERS": "Transactions", "DATE": "Date"},
        color_discrete_sequence=["#e2fb43"]
    )
    fig2.update_layout(xaxis_title="", yaxis_title="Txns", bargap=0.2)
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    fig3 = px.bar(
        df_ts,
        x="DATE",
        y="NUMBER_OF_USERS",
        title="Squid Bridge Users Over Time",
        labels={"NUMBER_OF_USERS": "Users", "DATE": "Date"},
        color_discrete_sequence=["#e2fb43"]
    )
    fig3.update_layout(xaxis_title="", yaxis_title="Addresses", bargap=0.2)
    st.plotly_chart(fig3, use_container_width=True)

# --- Query Function: Row (3) ------------------------------------------------------------------------------------------------
@st.cache_data
def load_source_chain_data(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND created_at::date >= '{start_str}' 
          AND created_at::date <= '{end_str}'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND created_at::date >= '{start_str}' 
          AND created_at::date <= '{end_str}'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT source_chain AS "Source Chain", 
           COUNT(DISTINCT id) AS "Number of Transfers", 
           COUNT(DISTINCT user) AS "Number of Users", 
           ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM axelar_service
    GROUP BY 1
    ORDER BY 2 DESC
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_source = load_source_chain_data(start_date, end_date)

# --- Top 20 Horizontal Bar Charts ----------------------------------------------------------------------------------
top_vol = df_source.nlargest(20, "Volume of Transfers (USD)")
top_txn = df_source.nlargest(20, "Number of Transfers")
top_usr = df_source.nlargest(20, "Number of Users")

col1, col2, col3 = st.columns(3)

with col1:
    fig1 = px.bar(
        top_vol.sort_values("Volume of Transfers (USD)"),
        x="Volume of Transfers (USD)", y="Source Chain",
        orientation="h",
        title="Top 20 Source Chains by Volume (USD)",
        labels={"Volume of Transfers (USD)": "USD", "Source Chain": " "},
        color_discrete_sequence=["#ca99e5"]
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(
        top_txn.sort_values("Number of Transfers"),
        x="Number of Transfers", y="Source Chain",
        orientation="h",
        title="Top 20 Source Chains by Transfers",
        labels={"Number of Transfers": "Txns count", "Source Chain": " "},
        color_discrete_sequence=["#ca99e5"]
    )
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    fig3 = px.bar(
        top_usr.sort_values("Number of Users"),
        x="Number of Users", y="Source Chain",
        orientation="h",
        title="Top 20 Source Chains by Users",
        labels={"Number of Users": "Address count", "Source Chain": " "},
        color_discrete_sequence=["#ca99e5"]
    )
    st.plotly_chart(fig3, use_container_width=True)

# --- Row 4 --------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_destination_data(start_date, end_date):
    # ensure string format YYYY-MM-DD
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
      SELECT 
        created_at, 
        LOWER(data:send:original_source_chain) AS source_chain, 
        LOWER(data:send:original_destination_chain) AS destination_chain,
        recipient_address AS user, 
        CASE 
          WHEN IS_ARRAY(data:send:amount) THEN NULL
          WHEN IS_OBJECT(data:send:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
          ELSE NULL
        END AS amount,
        CASE 
          WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
          WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
          ELSE NULL
        END AS amount_usd,
        CASE 
          WHEN IS_ARRAY(data:send:fee_value) THEN NULL
          WHEN IS_OBJECT(data:send:fee_value) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
          ELSE NULL
        END AS fee,
        id, 
        'Token Transfers' AS "Service", 
        data:link:asset::STRING AS raw_asset
      FROM axelar.axelscan.fact_transfers
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
        AND (
          sender_address ILIKE '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
          OR sender_address ILIKE '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
          OR sender_address ILIKE '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
          OR sender_address ILIKE '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
          OR sender_address ILIKE '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
        )

      UNION ALL

      SELECT  
        created_at,
        data:call.chain::STRING AS source_chain,
        data:call.returnValues.destinationChain::STRING AS destination_chain,
        data:call.transaction.from::STRING AS user,
        CASE 
          WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
          ELSE NULL
        END AS amount,
        CASE 
          WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
          WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
          ELSE NULL
        END AS amount_usd,
        COALESCE(
          CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END,
          CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END
        ) AS fee,
        id, 
        'GMP' AS "Service", 
        data:symbol::STRING AS raw_asset
      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
        AND (
          data:approved:returnValues:contractAddress ILIKE '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
          OR data:approved:returnValues:contractAddress ILIKE '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
        )
    )

    SELECT 
      destination_chain AS "Destination Chain", 
      COUNT(DISTINCT id) AS "Number of Transfers", 
      COUNT(DISTINCT user) AS "Number of Users", 
      ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM axelar_service
    GROUP BY 1
    ORDER BY "Number of Transfers" DESC
    """

    df = pd.read_sql(query, conn)

    # normalize column names for easier downstream use
    df = df.rename(columns={
        "Destination Chain": "Destination Chain",
        "Number of Transfers": "Number of Transfers",
        "Number of Users": "Number of Users",
        "Volume of Transfers (USD)": "Volume of Transfers (USD)"
    })

    return df

# --- Use the cached loader ---------------------------------------------------------
df_dest = load_destination_data(start_date, end_date)

# --- prepare top-20s and charts (horizontal bars) ------------------------------------
top_vol_dest = df_dest.nlargest(20, "Volume of Transfers (USD)").sort_values("Volume of Transfers (USD)", ascending=False)
top_txn_dest = df_dest.nlargest(20, "Number of Transfers").sort_values("Number of Transfers", ascending=False)
top_usr_dest = df_dest.nlargest(20, "Number of Users").sort_values("Number of Users", ascending=False)

fig_vol_dest = px.bar(
    top_vol_dest,
    x="Volume of Transfers (USD)",
    y="Destination Chain",
    orientation="h",
    title="Top 20 Destination Chains by Volume (USD)",
    labels={"Volume of Transfers (USD)": "USD", "Destination Chain": " "},
    color_discrete_sequence=["#ca99e5"]
)
fig_vol_dest.update_xaxes(tickformat=",.0f")
fig_vol_dest.update_traces(hovertemplate="%{y}: $%{x:,.0f}<extra></extra>")
fig_vol_dest.update_yaxes(autorange="reversed")  

fig_txn_dest = px.bar(
    top_txn_dest,
    x="Number of Transfers",
    y="Destination Chain",
    orientation="h",
    title="Top 20 Destination Chains by Transfers",
    labels={"Number of Transfers": "Txns count", "Destination Chain": " "},
    color_discrete_sequence=["#ca99e5"]
)
fig_txn_dest.update_xaxes(tickformat=",.0f")
fig_txn_dest.update_traces(hovertemplate="%{y}: %{x:,}<extra></extra>")
fig_txn_dest.update_yaxes(autorange="reversed")

fig_usr_dest = px.bar(
    top_usr_dest,
    x="Number of Users",
    y="Destination Chain",
    orientation="h",
    title="Top 20 Destination Chains by Users",
    labels={"Number of Users": "Addresses count", "Destination Chain": " "},
    color_discrete_sequence=["#ca99e5"]
)
fig_usr_dest.update_xaxes(tickformat=",.0f")
fig_usr_dest.update_traces(hovertemplate="%{y}: %{x:,}<extra></extra>")
fig_usr_dest.update_yaxes(autorange="reversed")

# --- display three charts in one row -----------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.plotly_chart(fig_vol_dest, use_container_width=True)
with col2:
    st.plotly_chart(fig_txn_dest, use_container_width=True)
with col3:
    st.plotly_chart(fig_usr_dest, use_container_width=True)

# --- Row 5 --------------------------------------------------------------------------------------
@st.cache_data
def load_transfer_metrics(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    recipient_address AS user, 

    CASE 
      WHEN IS_ARRAY(data:send:amount) THEN NULL
      WHEN IS_OBJECT(data:send:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
      WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
      ELSE NULL
    END AS amount_usd,

    CASE 
      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
      ELSE NULL
    END AS fee,

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
    sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' -- Squid
    or sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' -- Squid-blast
    or sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' -- Squid-fraxtal
    or sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' -- Squid coral
    or sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' -- Squid coral hub
) 

  UNION ALL

  SELECT  
    created_at,
    data:call.chain::STRING AS source_chain,
    data:call.returnValues.destinationChain::STRING AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' -- Squid
        or data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' -- Squid-blast
        or data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' -- Squid-fraxtal
        or data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' -- Squid coral
        or data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' -- Squid coral hub
        ) 
)

SELECT source_chain as "Source Chain", CASE 
      WHEN raw_asset='arb-wei' THEN 'ARB'
      WHEN raw_asset='avalanche-uusdc' THEN 'Avalanche USDC'
      WHEN raw_asset='avax-wei' THEN 'AVAX'
      WHEN raw_asset='bnb-wei' THEN 'BNB'
      WHEN raw_asset='busd-wei' THEN 'BUSD'
      WHEN raw_asset='cbeth-wei' THEN 'cbETH'
      WHEN raw_asset='cusd-wei' THEN 'cUSD'
      WHEN raw_asset='dai-wei' THEN 'DAI'
      WHEN raw_asset='dot-planck' THEN 'DOT'
      WHEN raw_asset='eeur' THEN 'EURC'
      WHEN raw_asset='ern-wei' THEN 'ERN'
      WHEN raw_asset='eth-wei' THEN 'ETH'
      WHEN raw_asset ILIKE 'factory/sei10hub%' THEN 'SEILOR'
      WHEN raw_asset='fil-wei' THEN 'FIL'
      WHEN raw_asset='frax-wei' THEN 'FRAX'
      WHEN raw_asset='ftm-wei' THEN 'FTM'
      WHEN raw_asset='glmr-wei' THEN 'GLMR'
      WHEN raw_asset='hzn-wei' THEN 'HZN'
      WHEN raw_asset='link-wei' THEN 'LINK'
      WHEN raw_asset='matic-wei' THEN 'MATIC'
      WHEN raw_asset='mkr-wei' THEN 'MKR'
      WHEN raw_asset='mpx-wei' THEN 'MPX'
      WHEN raw_asset='oath-wei' THEN 'OATH'
      WHEN raw_asset='op-wei' THEN 'OP'
      WHEN raw_asset='orbs-wei' THEN 'ORBS'
      WHEN raw_asset='factory/sei10hud5e5er4aul2l7sp2u9qp2lag5u4xf8mvyx38cnjvqhlgsrcls5qn5ke/seilor' THEN 'SEILOR'
      WHEN raw_asset='pepe-wei' THEN 'PEPE'
      WHEN raw_asset='polygon-uusdc' THEN 'Polygon USDC'
      WHEN raw_asset='reth-wei' THEN 'rETH'
      WHEN raw_asset='ring-wei' THEN 'RING'
      WHEN raw_asset='shib-wei' THEN 'SHIB'
      WHEN raw_asset='sonne-wei' THEN 'SONNE'
      WHEN raw_asset='stuatom' THEN 'stATOM'
      WHEN raw_asset='uatom' THEN 'ATOM'
      WHEN raw_asset='uaxl' THEN 'AXL'
      WHEN raw_asset='ukuji' THEN 'KUJI'
      WHEN raw_asset='ulava' THEN 'LAVA'
      WHEN raw_asset='uluna' THEN 'LUNA'
      WHEN raw_asset='ungm' THEN 'NGM'
      WHEN raw_asset='uni-wei' THEN 'UNI'
      WHEN raw_asset='uosmo' THEN 'OSMO'
      WHEN raw_asset='usomm' THEN 'SOMM'
      WHEN raw_asset='ustrd' THEN 'STRD'
      WHEN raw_asset='utia' THEN 'TIA'
      WHEN raw_asset='uumee' THEN 'UMEE'
      WHEN raw_asset='uusd' THEN 'USTC'
      WHEN raw_asset='uusdc' THEN 'USDC'
      WHEN raw_asset='uusdt' THEN 'USDT'
      WHEN raw_asset='vela-wei' THEN 'VELA'
      WHEN raw_asset='wavax-wei' THEN 'WAVAX'
      WHEN raw_asset='wbnb-wei' THEN 'WBNB'
      WHEN raw_asset='wbtc-satoshi' THEN 'WBTC'
      WHEN raw_asset='weth-wei' THEN 'WETH'
      WHEN raw_asset='wfil-wei' THEN 'WFIL'
      WHEN raw_asset='wftm-wei' THEN 'WFTM'
      WHEN raw_asset='wglmr-wei' THEN 'WGLMR'
      WHEN raw_asset='wmai-wei' THEN 'WMAI'
      WHEN raw_asset='wmatic-wei' THEN 'WMATIC'
      WHEN raw_asset='wsteth-wei' THEN 'wstETH'
      WHEN raw_asset='yield-eth-wei' THEN 'yieldETH' 
      else raw_asset end as "Symbol",
     round(sum(amount_usd)) as "Volume of Transfers (USD)", 
     count(distinct id) as "Number of Transfers"

FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1, 2
order by 4 desc 
    """
    df = pd.read_sql(query, conn)
    return df

df_transfer_metrics = load_transfer_metrics(start_date, end_date)

# ---- ردیف اول چارت‌ها ----
col1, col2 = st.columns(2)

# Stacked Horizontal Bar: Normalized Number of Transfers
df_norm1 = df_transfer_metrics.copy()
df_norm1["Number of Transfers %"] = df_norm1.groupby("Source Chain")["Number of Transfers"].transform(lambda x: x / x.sum() * 100)
fig1 = px.bar(
    df_norm1,
    x="Number of Transfers %",
    y="Source Chain",
    color="Symbol",
    orientation="h",
    barmode="stack",
    title="Normalized Number of Transfers by Symbol per Source Chain"
)
col1.plotly_chart(fig1, use_container_width=True)

# Stacked Horizontal Bar: Normalized Volume of Transfers (USD)
df_norm2 = df_transfer_metrics.copy()
df_norm2["Volume %"] = df_norm2.groupby("Source Chain")["Volume of Transfers (USD)"].transform(lambda x: x / x.sum() * 100)
fig2 = px.bar(
    df_norm2,
    x="Volume %",
    y="Source Chain",
    color="Symbol",
    orientation="h",
    barmode="stack",
    title="Normalized Volume of Transfers (USD) by Symbol per Source Chain"
)
col2.plotly_chart(fig2, use_container_width=True)

