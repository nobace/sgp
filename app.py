import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import plotly.graph_objects as go

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="SGP - Sistema de Gestão de Patrimônio", layout="wide")

# --- UTILITÁRIOS ---
def clean_num(val):
    if val is None or val == "" or val == "-": return 0.0
    s = str(val).strip()
    if "," in s: s = s.replace(".", "").replace(",", ".")
    try: return float(s)
    except: return 0.0

@st.cache_data(ttl=600)
def load_data():
    try:
        # Tenta carregar dos secrets (Local ou Streamlit Cloud)
        if "GOOGLE_SHEETS_CREDS" in st.secrets:
            creds_json = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
        else:
            # Fallback para arquivo local (se estiver rodando na máquina)
            with open("credentials.json") as f:
                creds_json = json.load(f)
                
        creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8" # Seu ID
        sh = client.open_by_key(ID_PLANILHA)
        
        return {
            "assets": pd.DataFrame(sh.worksheet("assets").get_all_records()),
            "trans": pd.DataFrame(sh.worksheet("transactions").get_all_records()),
            "market": pd.DataFrame(sh.worksheet("market_data").get_all_records()),
            "calendar": pd.DataFrame(sh.worksheet("dividend_calendar").get_all_records()),
            "history": pd.DataFrame(sh.worksheet("dividend_history").get_all_records())
        }
    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        return None

def render_cash_flow(df_tr, df_hist):
    st.subheader("💰 Fluxo de Caixa (Entradas vs Saídas)")
    
    # 1. Processar Movimentações (Aportes e Retiradas)
    df_flow = df_tr.copy()
    df_flow['date'] = pd.to_datetime(df_flow['date'], dayfirst=True, errors='coerce')
    
    # Calcular valor financeiro da transação
    # COMPRA = Saída de caixa (Negativo)
    # VENDA = Entrada de caixa (Positivo)
    def get_flow(row):
        val = row['quantity'] * row['price']
        if str(row['type']).upper() == 'COMPRA': return -val
        if str(row['type']).upper() == 'VENDA': return val
        return 0.0

    df_flow['fluxo'] = df_flow.apply(get_flow, axis=1)
    df_flow['mes'] = df_flow['date'].dt.to_period('M')
    
    # Agrupar Aportes por Mês
    aportes = df_flow.groupby('mes')['fluxo'].sum()

    # 2. Processar Dividendos (Entradas)
    df_divs = df_hist.copy()
    # Limpar nomes das colunas (remover espaços extras e minúsculas)
    df_divs.columns = [c.lower().strip() for c in df_divs.columns]
    
    # Usar 'data ex' como referência de recebimento aproximado (ou data pagamento se tiver)
    df_divs['data'] = pd.to_datetime(df_divs['data ex'], dayfirst=True, errors='coerce')
    df_divs['total recebido'] = df_divs['total recebido'].apply(clean_num)
    df_divs['mes'] = df_divs['data'].dt.to_period('M')
    
    proventos = df_divs.groupby('mes')['total recebido'].sum()

    # 3. Consolidar
    timeline = pd.DataFrame({'Movimentação': aportes, 'Proventos': proventos}).fillna(0)
    timeline['Liquido'] = timeline['Movimentação'] + timeline['Proventos']
    timeline.index = timeline.index.astype(str)

    # 4. Gráfico
    fig = go.Figure()
    fig.add_trace(go.Bar(x=timeline.index, y=timeline['Movimentação'], name='Aportes/Vendas', marker_color='indianred'))
    fig.add_trace(go.Bar(x=timeline.index, y=timeline['Proventos'], name='Dividendos', marker_color='mediumseagreen'))
    fig.add_trace(go.Scatter(x=timeline.index, y=timeline['Liquido'], name='Fluxo Líquido', mode='lines+markers', line=dict(color='black', width=2)))
    
    fig.update_layout(barmode='relative', title="Evolução Temporal do Caixa", height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Métricas de Fluxo
    k1, k2, k3 = st.columns(3)
    k1.metric("Total Aportado (Líquido)", f"R$ {timeline['Movimentação'].sum():,.2f}")
    k2.metric("Total Dividendos Recebidos", f"R$ {timeline['Proventos'].sum():,.2f}")
    k3.metric("Fluxo Acumulado", f"R$ {timeline['Liquido'].sum():,.2f}")

def main():
    data = load_data()
    if not data: return

    # Padronização de nomes de colunas
    df_as = data["assets"]; df_as.columns = [c.lower().strip() for c in df_as.columns]
    df_tr = data["trans"]; df_tr.columns = [c.lower().strip() for c in df_tr.columns]
    df_mk = data["market"]; df_mk.columns = [c.lower().strip() for c in df_mk.columns]
    df_cl = data["calendar"]; df_cl.columns = [c.lower().strip() for c in df_cl.columns]
    df_hi = data["history"]; 
    
    # Processamento Numérico
    df_tr['quantity'] = df_tr['quantity'].apply(clean_num)
    df_tr['price'] = df_tr['price'].apply(clean_num)
    df_mk['close_price'] = df_mk['close_price'].apply(clean_num)
    
    # Taxa USD (Fallback seguro)
    try:
        usd_val = df_mk.loc[df_mk['ticker'] == 'USDBRL=X', 'close_price'].values[0]
    except:
        usd_val = 5.00

    # --- MENU LATERAL ---
    st.sidebar.title("SGP 📈")
    page = st.sidebar.radio("Navegação", ["Carteira Atual", "Fluxo de Caixa", "Agenda Dividendos"])

    if page == "Carteira Atual":
        st.title("🚀 Performance da Carteira")
        
        # Lógica de Posição Atual (Saldo acumulado)
        df_tr['custo_total'] = df_tr.apply(lambda r: r['quantity'] * r['price'] * (usd_val if str(r.get('currency','')).upper() == 'USD' else 1.0), axis=1)
        
        # Ajuste para VENDA diminuir quantidade
        def get_qty_signed(row):
            return row['quantity'] if str(row['type']).upper() != 'VENDA' else -row['quantity']
            
        def get_cost_signed(row):
            # Para custo médio simples, venda reduz custo proporcionalmente (simplificação)
            val = row['quantity'] * row['price'] * (usd_val if str(r.get('currency','')).upper() == 'USD' else 1.0)
            return val if str(row['type']).upper() != 'VENDA' else -val

        df_tr['qtd_ajustada'] = df_tr.apply(get_qty_signed, axis=1)
        # Nota: Cálculo de custo médio fiscal é complexo. Aqui usamos custo histórico simples.
        
        resumo = df_tr.groupby('ticker').agg({'qtd_ajustada': 'sum'}).reset_index()
        resumo = resumo[resumo['qtd_ajustada'] > 0] # Apenas carteira atual
        
        # Merge com Cotações
        resumo = resumo.merge(df_mk[['ticker', 'close_price']], on='ticker', how='left')
        # Merge com Cadastro (para saber moeda e tipo)
        resumo = resumo.merge(df_as[['ticker', 'type', 'currency']], on='ticker', how='left')
        
        # Valor Atual
        resumo['valor_atual_brl'] = resumo.apply(
            lambda r: (r['qtd_ajustada'] * r['close_price']) * (usd_val if str(r.get('currency')).upper() == 'USD' else 1.0), axis=1
        )
        
        # Exibição
        c1, c2 = st.columns(2)
        c1.metric("Patrimônio Bruto", f"R$ {resumo['valor_atual_brl'].sum():,.2f}")
        c2.metric("Dólar PTAX", f"R$ {usd_val:.2f}")
        
        st.dataframe(resumo[['ticker', 'qtd_ajustada', 'close_price', 'valor_atual_brl']].sort_values('valor_atual_brl', ascending=False), use_container_width=True)

    elif page == "Fluxo de Caixa":
        render_cash_flow(df_tr, df_hi)

    elif page == "Agenda Dividendos":
        st.title("📅 Próximos Dividendos")
        st.dataframe(df_cl, use_container_width=True)

if __name__ == "__main__":
    main()
