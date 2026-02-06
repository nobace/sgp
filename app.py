import streamlit as st
import pandas as pd
import plotly.express as px
import yfinance as yf
# Adicione esta importação explícita abaixo:
from streamlit_gsheets import GSheetsConnection

st.set_page_config(page_title="SGP - Monitor Patrimonial", layout="wide")

# Altere a chamada da conexão para passar a classe explicitamente:
conn = st.connection("gsheets", type=GSheetsConnection)

@st.cache_data(ttl=3600)
def load_data():
    assets = conn.read(worksheet="assets")
    trans = conn.read(worksheet="transactions")
    market = conn.read(worksheet="market_data")
    return assets, trans, market

df_assets, df_trans, df_market = load_data()

# Processamento
def get_positions(df_assets, df_trans, df_market):
    # Quantidade atual por ativo e instituição
    pos = df_trans.groupby(['ticker', 'institution'])['quantity'].sum().reset_index()
    pos = pos.merge(df_assets, on='ticker', how='left')
    pos = pos.merge(df_market[['ticker', 'close_price']], on='ticker', how='left')
    
    # Câmbio
    usd_quote = yf.Ticker("BRL=X").fast_info['last_price']
    
    pos['valor_brl'] = pos.apply(
        lambda x: (x['quantity'] * x['close_price'] * usd_quote) if x['currency'] == 'USD' 
        else (x['quantity'] * x['close_price']), axis=1
    )
    return pos, usd_quote

df_pos, dolar = get_positions(df_assets, df_trans, df_market)

# Métricas
total_brl = df_pos['valor_brl'].sum()
# Lógica de Dolarização: Moeda USD + BDRs + ETFs Internacionais
dolarizados = df_pos[
    (df_pos['currency'] == 'USD') | 
    (df_pos['type'] == 'BDR') | 
    (df_pos['ticker'].str.contains('IVVB11|XINA11|NDIV11|DIVD11'))
]['valor_brl'].sum()
idx_dolar = (dolarizados / total_brl) * 100

# Interface
st.title("📊 Gestão de Patrimônio Unificada")

c1, c2, c3 = st.columns(3)
c1.metric("Patrimônio Total", f"R$ {total_brl:,.2f}")
c2.metric("Índice de Dolarização", f"{idx_dolar:.2f}%")
c3.metric("Dólar Hoje", f"R$ {dolar:.2f}")

st.divider()

t1, t2, t3 = st.tabs(["Alocação por Produto", "Visão por Instituição", "Carteira Detalhada"])

with t1:
    fig = px.pie(df_pos, values='valor_brl', names='type', hole=0.5, title="Distribuição por Produto")
    st.plotly_chart(fig, use_container_width=True)

with t2:
    fig = px.pie(df_pos, values='valor_brl', names='institution', title="Onde meu dinheiro está custodiado?")
    st.plotly_chart(fig, use_container_width=True)

with t3:

    st.dataframe(df_pos[['ticker', 'institution', 'type', 'quantity', 'valor_brl']].style.format({'valor_brl': 'R$ {:,.2f}'}), use_container_width=True)

