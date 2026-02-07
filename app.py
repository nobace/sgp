import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

# --- FUNÇÕES DE UTILIDADE ---

def clean_num(val):
    if val is None or val == "" or val == "-" or val == "A confirmar": return 0.0
    s = str(val).strip()
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

@st.cache_data(ttl=600) # Cache de 10 minutos para não estourar limite do Google
def load_data():
    try:
        creds_json = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
        creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        
        ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
        sh = client.open_by_key(ID_PLANILHA)
        
        assets = pd.DataFrame(sh.worksheet("assets").get_all_records(numericise_ignore=['all']))
        trans = pd.DataFrame(sh.worksheet("transactions").get_all_records(numericise_ignore=['all']))
        market = pd.DataFrame(sh.worksheet("market_data").get_all_records(numericise_ignore=['all']))
        
        try:
            calendar = pd.DataFrame(sh.worksheet("dividend_calendar").get_all_records(numericise_ignore=['all']))
        except:
            calendar = pd.DataFrame()
            
        return assets, trans, market, calendar
    except Exception as e:
        st.error(f"Erro na conexão com Google Sheets: {e}")
        return None, None, None, None

def main():
    st.set_page_config(page_title="Investimentos Dashboard", layout="wide", page_icon="💰")
    st.title("📊 Gestão de Patrimônio & Proventos")
    
    df_assets, df_trans, df_market, df_cal = load_data()
    
    if df_assets is not None:
        # Padronização de Colunas
        for df in [df_assets, df_trans, df_market, df_cal]:
            if not df.empty:
                df.columns = [c.lower().strip() for c in df.columns]

        # Processamento de Preços
        df_market['close_price'] = df_market['close_price'].apply(clean_num)
        
        # Cruzamento de Dados para Dashboard
        # 1. Calcular Qtd atual via Transactions
        for col in ['quantity', 'price', 'exchange_rate']:
            if col in df_trans.columns: df_trans[col] = df_trans[col].apply(clean_num)
        
        # Filtro de tickers manuais (Previdência, FGTS)
        tickers_manuais = df_assets[df_assets['manual_update'].astype(str).str.upper().isin(['S', 'SIM', '1', 'TRUE'])]['ticker'].unique()
        
        # Cálculo de Saldo
        resumo = df_trans.groupby('ticker').agg({'quantity': 'sum'}).reset_index()
        resumo = resumo.merge(df_market[['ticker', 'close_price']], on='ticker', how='left')
        resumo = resumo.merge(df_assets[['ticker', 'name', 'type', 'currency']], on='ticker', how='left')
        
        # Moeda e Câmbio
        usd_val = df_market[df_market['ticker'] == 'USDBRL=X']['close_price'].values[0] if 'USDBRL=X' in df_market['ticker'].values else 1.0
        
        resumo['saldo_brl'] = resumo.apply(
            lambda r: (r['quantity'] * r['close_price']) * (usd_val if str(r['currency']).upper() == 'USD' else 1.0), axis=1
        )

        # --- KPIs NO TOPO ---
        total_patrimonio = resumo['saldo_brl'].sum()
        col1, col2, col3 = st.columns(3)
        col1.metric("Patrimônio Total", f"R$ {total_patrimonio:,.2f}")
        col2.metric("Dólar Hoje", f"R$ {usd_val:.2f}")
        
        # --- SEÇÃO DE PROVENTOS (O CORAÇÃO DO SISTEMA) ---
        st.divider()
        st.subheader("📅 Calendário de Recebimentos (Brapi Data)")
        
        if not df_cal.empty:
            # Limpar valores de dividendos
            df_cal['valor'] = df_cal['valor'].apply(clean_num)
            
            # Filtra apenas o que você tem na carteira hoje
            ativos_carteira = resumo[resumo['quantity'] > 0]['ticker'].tolist()
            cal_filtrado = df_cal[df_cal['ticker'].isin(ativos_carteira)].copy()
            
            if not cal_filtrado.empty:
                # Merge com a quantidade para calcular o total a receber
                cal_filtrado = cal_filtrado.merge(resumo[['ticker', 'quantity']], on='ticker', how='left')
                cal_filtrado['total_receber'] = cal_filtrado['valor'] * cal_filtrado['quantity']
                
                # Divide entre Confirmados (Dinheiro certo) e Histórico
                confirmados = cal_filtrado[cal_filtrado['status'].isin(['Confirmado', 'Anunciado'])].copy()
                historicos = cal_filtrado[cal_filtrado['status'] == 'Histórico'].copy()
                
                if not confirmados.empty:
                    st.success(f"💰 Total a receber confirmado: **R$ {confirmados['total_receber'].sum():,.2f}**")
                    st.dataframe(
                        confirmados[['ticker', 'data ex', 'data pagamento', 'valor', 'quantity', 'total_receber']],
                        column_config={
                            "ticker": "Ativo",
                            "data ex": "Data Com",
                            "data pagamento": "Data Pagto",
                            "valor": "R$ / Cota",
                            "quantity": "Qtd Atual",
                            "total_receber": "Total Líquido"
                        },
                        hide_index=True, use_container_width=True
                    )
                else:
                    st.info("Nenhum novo provento anunciado para seus ativos nos últimos dias.")

                with st.expander("Ver últimos proventos pagos (Histórico)"):
                    st.table(historicos[['ticker', 'data ex', 'valor']].head(10))

        # --- TABELA DE ATIVOS ---
        st.divider()
        st.subheader("📋 Detalhamento da Carteira")
        resumo_view = resumo[resumo['quantity'] > 0][['ticker', 'name', 'type', 'quantity', 'close_price', 'saldo_brl']]
        st.dataframe(resumo_view.style.format({'close_price': '{:.2f}', 'saldo_brl': '{:,.2f}'}), use_container_width=True)

if __name__ == "__main__":
    main()
