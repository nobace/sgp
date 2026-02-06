import streamlit as st
import pandas as pd
import plotly.express as px
import yfinance as yf

# 1. Configuração da Página
st.set_page_config(page_title="SGP - Monitor Patrimonial", layout="wide")

ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"

@st.cache_data(ttl=600)
def load_all_data(id_planilha):
    def load_sheet(sheet_name):
        url = f"https://docs.google.com/spreadsheets/d/{id_planilha}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
        return pd.read_csv(url)

    return load_sheet("assets"), load_sheet("transactions"), load_sheet("market_data")

def get_positions(df_assets, df_trans, df_market):
    # Função de limpeza ultra-agressiva
    def clean_num(value):
        try:
            if pd.isna(value) or str(value).strip() == "": return 0.0
            # Remove tudo que não é número, vírgula ou ponto
            s = str(value).replace('R$', '').replace(' ', '').strip()
            # Se tiver ponto e vírgula (ex 1.234,56), remove o ponto
            if '.' in s and ',' in s: s = s.replace('.', '')
            # Troca vírgula por ponto
            s = s.replace(',', '.')
            return float(s)
        except:
            return 0.0

    # Forçar nomes de colunas para minúsculo para evitar erro de digitação
    df_trans.columns = [c.lower() for c in df_trans.columns]
    df_market.columns = [c.lower() for c in df_market.columns]
    df_assets.columns = [c.lower() for c in df_assets.columns]

    # Aplicar limpeza
    df_trans['quantity'] = df_trans['quantity'].apply(clean_num)
    df_market['close_price'] = df_market['close_price'].apply(clean_num)

    # Agrupar
    pos = df_trans.groupby(['ticker', 'institution'])['quantity'].sum().reset_index()
    pos = pos.merge(df_assets, on='ticker', how='left')
    pos = pos.merge(df_market[['ticker', 'close_price']], on='ticker', how='left')
    
    try:
        usd_quote = yf.Ticker("BRL=X").history(period="1d")['Close'].iloc[-1]
    except:
        usd_quote = 5.85
    
    def calc_brl(row):
        qtd = float(row['quantity'])
        preco = float(row['close_price'])
        # Se o ticker for em dólar ou a moeda for USD
        if str(row.get('currency', '')).upper() == 'USD':
            return qtd * preco * usd_quote
        return qtd * preco

    pos['valor_brl'] = pos.apply(calc_brl, axis=1)
    return pos, usd_quote

# --- EXECUÇÃO ---
df_assets, df_trans, df_market = load_all_data(ID_PLANILHA)

# DEBUG: Mostra se os dados chegaram (apenas para você ver)
if st.checkbox("Ver dados brutos da planilha"):
    st.write("Transações:", df_trans.head())
    st.write("Preços:", df_market.head())

df_pos, dolar = get_positions(df_assets, df_trans, df_market)

total_brl = df_pos['valor_brl'].sum()
dolarizados = df_pos[df_pos['currency'] == 'USD']['valor_brl'].sum()
idx_dolar = (dolarizados / total_brl * 100) if total_brl > 0 else 0.0

# --- INTERFACE ---
st.title("📊 Gestão de Patrimônio Unificada")

c1, c2, c3 = st.columns(3)
c1.metric("Patrimônio Total", f"R$ {total_brl:,.2f}")
c2.metric("Índice de Dolarização", f"{idx_dolar:.2f}%")
c3.metric("Dólar (Yahoo)", f"R$ {dolar:.2f}")

if total_brl == 0:
    st.error("⚠️ O patrimônio está zerado. Verifique se as colunas 'quantity' (na aba transactions) e 'close_price' (na aba market_data) contêm números.")

st.divider()
st.dataframe(df_pos)
