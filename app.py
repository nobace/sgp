import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def clean_num(val):
    if val is None or val == "": return 0.0
    s = str(val).strip()
    if "," in s: s = s.replace(".", "").replace(",", ".")
    try: return float(s)
    except: return 0.0

def load_data():
    creds_json = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    client = gspread.authorize(creds)
    sh = client.open_by_key("1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8")
    
    assets = pd.DataFrame(sh.worksheet("assets").get_all_records(numericise_ignore=['all']))
    trans = pd.DataFrame(sh.worksheet("transactions").get_all_records(numericise_ignore=['all']))
    market = pd.DataFrame(sh.worksheet("market_data").get_all_records(numericise_ignore=['all']))
    return assets, trans, market

def main():
    st.set_page_config(page_title="Gestão Patrimonial", layout="wide")
    st.title("🚀 Dashboard de Investimentos")
    
    try:
        df_assets, df_trans, df_market = load_data()
        for df in [df_assets, df_trans, df_market]:
            df.columns = [c.lower().strip() for c in df.columns]

        # Identifica ativos manuais via Flag para compensação de 100x
        # (Considerando que você usa a vírgula neles na planilha)
        tickers_manuais = df_assets[df_assets['manual_update'].astype(str).str.upper().isin(['S', 'SIM', '1', 'TRUE'])]['ticker'].unique()

        # Limpeza Numérica
        for col in ['quantity', 'price', 'costs', 'exchange_rate']:
            if col in df_trans.columns: df_trans[col] = df_trans[col].apply(clean_num)
        df_market['close_price'] = df_market['close_price'].apply(clean_num)

        # COMPENSAÇÃO DINÂMICA
        df_market.loc[df_market['ticker'].isin(tickers_manuais), 'close_price'] *= 100

        # 1. Investimento BRL
        df_trans['inv_brl'] = df_trans.apply(
            lambda r: r['costs'] if r['costs'] > 0 
            else (r['quantity'] * r['price'] * (r['exchange_rate'] if r['exchange_rate'] > 0 else 1.0)), axis=1
        )

        # 2. Consolidação
        resumo = df_trans.groupby('ticker').agg({'quantity': 'sum', 'inv_brl': 'sum'}).reset_index()
        resumo = resumo.merge(df_market[['ticker', 'close_price', 'last_update']], on='ticker', how='left')
        resumo = resumo.merge(df_assets[['ticker', 'name', 'type', 'currency']], on='ticker', how='left')
        
        # 3. Câmbio
        usd_val = df_market[df_market['ticker'] == 'USDBRL=X']['close_price'].values[0] if 'USDBRL=X' in df_market['ticker'].values else 1.0
        st.sidebar.metric("Câmbio Dólar", f"R$ {usd_val:.2f}")

        resumo['Saldo Atual'] = resumo.apply(
            lambda r: (r['quantity'] * r['close_price']) * (usd_val if str(r['currency']).upper() == 'USD' else 1.0), axis=1
        )
        
        # 4. Performance
        resumo['Preço Médio'] = np.where(resumo['quantity'] > 0, resumo['inv_brl'] / resumo['quantity'], 0)
        resumo['Lucro'] = resumo['Saldo Atual'] - resumo['inv_brl']
        resumo['Rentabilidade'] = np.where(resumo['inv_brl'] > 0, (resumo['Lucro'] / resumo['inv_brl']) * 100, 0)
        
        # 5. UI
        t_atual, t_inv = resumo['Saldo Atual'].sum(), resumo['inv_brl'].sum()
        m1, m2, m3 = st.columns(3)
        m1.metric("Patrimônio Total", f"R$ {t_atual:,.2f}")
        m2.metric("Total Investido", f"R$ {t_inv:,.2f}")
        m3.metric("Lucro Total", f"R$ {t_atual-t_inv:,.2f}")

        st.dataframe(resumo[['name', 'type', 'quantity', 'Preço Médio', 'Saldo Atual', 'Lucro', 'Rentabilidade']].style.format({
            'Preço Médio': 'R$ {:,.2f}', 'Saldo Atual': 'R$ {:,.2f}', 'Lucro': 'R$ {:,.2f}', 'Rentabilidade': '{:.2f}%'
        }))

    except Exception as e:
        st.error(f"Erro: {e}")

if __name__ == "__main__":
    main()
