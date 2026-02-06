import streamlit as st
import pandas as pd
import plotly.express as px
import yfinance as yf
from streamlit_gsheets import GSheetsConnection

# 1. Configuração da Página
st.set_page_config(page_title="SGP - Monitor Patrimonial", layout="wide")

# ID da sua planilha (Extraído do link que você forneceu)
ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"

# 2. Funções de Carregamento e Limpeza
@st.cache_data(ttl=3600)
def load_all_data(id_planilha):
    def load_sheet(sheet_name):
        url = f"https://docs.google.com/spreadsheets/d/{id_planilha}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
        try:
            return pd.read_csv(url)
        except Exception as e:
            st.error(f"Erro ao ler a aba '{sheet_name}': {e}")
            return pd.DataFrame()

    assets = load_sheet("assets")
    trans = load_sheet("transactions")
    market = load_sheet("market_data")
    return assets, trans, market

def get_positions(df_assets, df_trans, df_market):
    # Função interna para limpar números (converte vírgula brasileira para ponto)
    def clean_num(value):
        try:
            if pd.isna(value) or value == "": return 0.0
            s = str(value).replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.').strip()
            return float(s)
        except:
            return 0.0

    # Limpeza de dados
    df_trans['quantity'] = df_trans['quantity'].apply(clean_num)
    if 'price' in df_trans.columns:
        df_trans['price'] = df_trans['price'].apply(clean_num)
    
    df_market['close_price'] = df_market['close_price'].apply(clean_num)

    # Consolidação das posições
    pos = df_trans.groupby(['ticker', 'institution'])['quantity'].sum().reset_index()
    pos = pos.merge(df_assets, on='ticker', how='left')
    pos = pos.merge(df_market[['ticker', 'close_price']], on='ticker', how='left')
    
    # Câmbio
    try:
        usd_quote = yf.Ticker("BRL=X").history(period="1d")['Close'].iloc[-1]
    except:
        usd_quote = 5.85 # Fallback
    
    # Cálculo do Valor em BRL
    def calc_brl(row):
        qtd = float(row['quantity'])
        preco = float(row['close_price'])
        if str(row['currency']).upper() == 'USD':
            return qtd * preco * usd_quote
        return qtd * preco

    pos['valor_brl'] = pos.apply(calc_brl, axis=1)
    return pos, usd_quote

# 3. Execução do Fluxo
df_assets, df_trans, df_market = load_all_data(ID_PLANILHA)

if df_assets.empty or df_trans.empty:
    st.warning("Aguardando preenchimento dos dados na planilha Google...")
    st.stop()

# Chama o processamento
df_pos, dolar = get_positions(df_assets, df_trans, df_market)

# 4. Cálculo de Métricas (Agora com os dados já processados)
total_brl = df_pos['valor_brl'].sum()

# Lógica de Dolarização
dolarizados_df = df_pos[
    (df_pos['currency'] == 'USD') | 
    (df_pos['type'] == 'BDR') | 
    (df_pos['ticker'].str.contains('IVVB11|XINA11|NDIV11|DIVD11', na=False))
]
dolarizados_valor = dolarizados_df['valor_brl'].sum()

if total_brl > 0:
    idx_dolar = (dolarizados_valor / total_brl) * 100
else:
    idx_dolar = 0.0

# 5. Interface Gráfica
st.title("📊 Gestão de Patrimônio Unificada")

col1, col2, col3 = st.columns(3)
col1.metric("Patrimônio Total", f"R$ {total_brl:,.2f}")
col2.metric("Índice de Dolarização", f"{idx_dolar:.2f}%")
col3.metric("Dólar (Yahoo)", f"R$ {dolar:.2f}")

st.divider()

t1, t2, t3 = st.tabs(["Alocação por Produto", "Visão por Instituição", "Carteira Detalhada"])

with t1:
    fig_prod = px.pie(df_pos, values='valor_brl', names='type', hole=0.5, title="Distribuição por Produto")
    st.plotly_chart(fig_prod, use_container_width=True)

with t2:
    fig_inst = px.pie(df_pos, values='valor_brl', names='institution', title="Patrimônio por Instituição")
    st.plotly_chart(fig_inst, use_container_width=True)

with t3:
    # Tabela formatada para o usuário
    df_tab = df_pos[['ticker', 'institution', 'type', 'quantity', 'valor_brl']].copy()
    df_tab.columns = ['Ticker', 'Instituição', 'Tipo', 'Qtd', 'Total (BRL)']
    st.dataframe(df_tab.style.format({'Total (BRL)': 'R$ {:,.2f}', 'Qtd': '{:.2f}'}), use_container_width=True)
