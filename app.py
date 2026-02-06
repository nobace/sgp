import streamlit as st
import pandas as pd
import plotly.express as px
import yfinance as yf
# Adicione esta importação explícita abaixo:
from streamlit_gsheets import GSheetsConnection

st.set_page_config(page_title="SGP - Monitor Patrimonial", layout="wide")

# Altere a chamada da conexão para passar a classe explicitamente:
conn = st.connection("gsheets", type=GSheetsConnection)

# Adicione esta linha logo abaixo para depurar se necessário
if "connections" not in st.secrets:
    st.error("Configuração de Secrets não encontrada no Streamlit Cloud!")



@st.cache_data(ttl=3600)
def load_data():
    assets = conn.read(worksheet="assets")
    trans = conn.read(worksheet="transactions")
    market = conn.read(worksheet="market_data")
    return assets, trans, market

#df_assets, df_trans, df_market = load_data()

# Use apenas o ID da planilha (aquela sequência longa de letras e números)
ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8" 
def load_data_from_google(sheet_name):
    # Este formato de URL é o mais aceite pelo Google para exportação de dados
    url = f"https://docs.google.com/spreadsheets/d/{ID_PLANILHA}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    # Adicionamos um tratamento de erro específico para cada aba
    try:
        return pd.read_csv(url)
    except Exception as e:
        st.error(f"Erro ao ler a aba '{sheet_name}': {e}")
        return pd.DataFrame()

# Carregamento dos dados
df_assets = load_data_from_google("assets")
df_trans = load_data_from_google("transactions")
df_market = load_data_from_google("market_data")

# Verificação de segurança: Se a aba assets estiver vazia, paramos aqui para diagnosticar
if df_assets.empty:
    st.warning("Aguardando carregamento da aba 'assets'...")
    st.stop()

# Processamento
def get_positions(df_assets, df_trans, df_market):
    # Função auxiliar para limpar números brasileiros (vírgula para ponto)
    def clean_num(value):
        try:
            if pd.isna(value) or value == "": return 0.0
            # Remove R$, pontos de milhar e troca vírgula por ponto
            s = str(value).replace('R$', '').replace(' ', '').strip()
            if '.' in s and ',' in s: # Caso tenha 1.234,56
                s = s.replace('.', '')
            s = s.replace(',', '.')
            return float(s)
        except:
            return 0.0

    # Aplicar limpeza nas colunas críticas
    for col in ['quantity', 'price']:
        if col in df_trans.columns:
            df_trans[col] = df_trans[col].apply(clean_num)
    
    if 'close_price' in df_market.columns:
        df_market['close_price'] = df_market['close_price'].apply(clean_num)

    # Agrupar quantidades
    pos = df_trans.groupby(['ticker', 'institution'])['quantity'].sum().reset_index()
    pos = pos.merge(df_assets, on='ticker', how='left')
    pos = pos.merge(df_market[['ticker', 'close_price']], on='ticker', how='left')
    
    # Câmbio com Fallback para não travar
    try:
        # Usando um período curto para evitar limite do Yahoo
        usd_quote = yf.Ticker("BRL=X").history(period="1d")['Close'].iloc[-1]
    except:
        usd_quote = 5.85
    
    # Cálculo do Valor BRL
    def calc_brl(row):
        qtd = float(row['quantity'])
        preco = float(row['close_price'])
        if str(row['currency']).upper() == 'USD':
            return qtd * preco * usd_quote
        return qtd * preco

    pos['valor_brl'] = pos.apply(calc_brl, axis=1)
    return pos, usd_quote

# No bloco de métricas, adicione uma proteção contra divisão por zero:
total_brl = df_pos['valor_brl'].sum()
if total_brl > 0:
    idx_dolar = (dolarizados / total_brl) * 100
else:
    idx_dolar = 0.0

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












