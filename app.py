import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

# --- FUNÇÕES DE UTILIDADE ---

def clean_num(val):
    """Limpa strings financeiras e converte para float (BR e US)"""
    if val is None or val == "" or val == "-": return 0.0
    s = str(val).strip()
    # Se tiver vírgula, tratamos como padrão BR (remove ponto de milhar, troca vírgula por ponto decimal)
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def load_data():
    """Conecta ao Google Sheets e carrega as abas necessárias"""
    try:
        creds_json = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
        creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        
        ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
        sh = client.open_by_key(ID_PLANILHA)
        
        # Carregamos as abas (numericise_ignore para manter o controle manual das strings)
        assets = pd.DataFrame(sh.worksheet("assets").get_all_records(numericise_ignore=['all']))
        trans = pd.DataFrame(sh.worksheet("transactions").get_all_records(numericise_ignore=['all']))
        market = pd.DataFrame(sh.worksheet("market_data").get_all_records(numericise_ignore=['all']))
        
        # Tenta carregar o calendário, se não existir retorna vazio
        try:
            calendar = pd.DataFrame(sh.worksheet("dividend_calendar").get_all_records(numericise_ignore=['all']))
        except:
            calendar = pd.DataFrame()
            
        return assets, trans, market, calendar
    except Exception as e:
        st.error(f"Erro na conexão com Google Sheets: {e}")
        return None, None, None, None

def main():
    st.set_page_config(page_title="Gestão Patrimonial", layout="wide", page_icon="📈")
    st.title("🚀 Dashboard de Investimentos")
    
    df_assets, df_trans, df_market, df_cal = load_data()
    
    if df_assets is not None:
        # Padronização de nomes de colunas
        for df in [df_assets, df_trans, df_market, df_cal]:
            if not df.empty:
                df.columns = [c.lower().strip() for c in df.columns]

        # 1. IDENTIFICAÇÃO DE ATIVOS MANUAIS (Para compensação de 100x)
        tickers_manuais = df_assets[df_assets['manual_update'].astype(str).str.upper().isin(['S', 'SIM', '1', 'TRUE'])]['ticker'].unique()

        # 2. LIMPEZA E PROCESSAMENTO
        for col in ['quantity', 'price', 'costs', 'exchange_rate']:
            if col in df_trans.columns: df_trans[col] = df_trans[col].apply(clean_num)
        
        df_market['close_price'] = df_market['close_price'].apply(clean_num)
        
        # Compensação: Se o ativo é manual, multiplicamos por 100 (ajuste para entrada com vírgula)
        df_market.loc[df_market['ticker'].isin(tickers_manuais), 'close_price'] *= 100

        # 3. CÁLCULO DE INVESTIMENTO (BRL)
        df_trans['total_invested_brl'] = df_trans.apply(
            lambda r: r['costs'] if r['costs'] > 0 
            else (r['quantity'] * r['price'] * (r['exchange_rate'] if r['exchange_rate'] > 0 else 1.0)), 
            axis=1
        )

        # 4. CONSOLIDAÇÃO POR ATIVO
        resumo = df_trans.groupby('ticker').agg({'quantity': 'sum', 'total_invested_brl': 'sum'}).reset_index()
        resumo = resumo.merge(df_market[['ticker', 'close_price', 'last_update']], on='ticker', how='left')
        resumo = resumo.merge(df_assets[['ticker', 'name', 'type', 'currency']], on='ticker', how='left')
        
        # Valor do Dólar Atual
        usd_val = df_market[df_market['ticker'] == 'USDBRL=X']['close_price'].values[0] if 'USDBRL=X' in df_market['ticker'].values else 1.0
        st.sidebar.metric("Câmbio Dólar", f"R$ {usd_val:.2f}")

        # Saldo Atual considerando moeda
        resumo['saldo_atual'] = resumo.apply(
            lambda r: (r['quantity'] * r['close_price']) * (usd_val if str(r['currency']).upper() == 'USD' else 1.0), 
            axis=1
        )
        
        # Indicadores de Performance
        resumo['preco_medio'] = np.where(resumo['quantity'] > 0, resumo['total_invested_brl'] / resumo['quantity'], 0)
        resumo['lucro_abs'] = resumo['saldo_atual'] - resumo['total_invested_brl']
        resumo['rentabilidade'] = np.where(resumo['total_invested_brl'] > 0, (resumo['lucro_abs'] / resumo['total_invested_brl']) * 100, 0)
        
        # 5. KPI METRICS (Topo
