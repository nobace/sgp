import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def clean_num(value):
    if value is None or value == "": return 0.0
    s = str(value).strip()
    if "." in s and "," in s: s = s.replace(".", "").replace(",", ".")
    elif "," in s: s = s.replace(",", ".")
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
    st.set_page_config(page_title="Gestão de Patrimônio", layout="wide")
    st.title("🚀 Dashboard Patrimonial")
    
    try:
        df_assets, df_trans, df_market = load_data()
        
        # Padronização de Colunas
        for df in [df_assets, df_trans, df_market]:
            df.columns = [c.lower().strip() for c in df.columns]

        # Limpeza Numérica
        for col in ['quantity', 'price', 'costs', 'exchange_rate']:
            if col in df_trans.columns: df_trans[col] = df_trans[col].apply(clean_num)
        df_market['close_price'] = df_market['close_price'].apply(clean_num)

        # 1. Investimento (BRL)
        df_trans['inv_brl'] = df_trans.apply(
            lambda r: r['costs'] if r['costs'] > 0 
            else (r['quantity'] * r['price'] * (r['exchange_rate'] if r['exchange_rate'] > 0 else 1.0)),
            axis=1
        )

        # 2. Consolidação
        resumo = df_trans.groupby('ticker').agg({'quantity': 'sum', 'inv_brl': 'sum'}).reset_index()
        resumo = resumo.merge(df_market[['ticker', 'close_price', 'last_update']], on='ticker', how='left')
        resumo = resumo.merge(df_assets[['ticker', 'name', 'type', 'currency']], on='ticker', how='left')
        
        # 3. Câmbio e Conversão
        usd_rate = df_market[df_market['ticker'] == 'USDBRL=X']['close_price'].values[0] if 'USDBRL=X' in df_market['ticker'].values else 1.0
        st.sidebar.metric("Cotação Dólar", f"R$ {usd_rate:.2f}")

        resumo['Saldo Atual'] = resumo.apply(
            lambda r: (r['quantity'] * r['close_price']) * (usd_rate if str(r['currency']).upper() == 'USD' else 1.0),
            axis=1
        )
        
        # 4. Preço Médio e Performance
        resumo['Preço Médio'] = np.where(resumo['quantity'] > 0, resumo['inv_brl'] / resumo['quantity'], 0)
        resumo['Lucro'] = resumo['Saldo Atual'] - resumo['inv_brl']
        resumo['Rentabilidade'] = np.where(resumo['inv_brl'] > 0, (resumo['Lucro'] / resumo['inv_brl']) * 100, 0)
        
        # 5. UI
        t_atual = resumo['Saldo Atual'].sum()
        t_inv = resumo['inv_brl'].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Patrimônio Total", f"R$ {t_atual:,.2f}")
        c2.metric("Total Investido", f"R$ {t_inv:,.2f}")
        c3.metric("Lucro Total", f"R$ {t_atual-t_inv:,.2f}", f"{((t_atual/t_inv)-1)*100:.2f}%")

        st.divider()
        st.subheader("📊 Performance por Ativo")
        
        view = resumo[['name', 'type', 'quantity', 'Preço Médio', 'inv_brl', 'Saldo Atual', 'Lucro', 'Rentabilidade']].copy()
        view.columns = ['Nome', 'Tipo', 'Qtd', 'PM (BRL)', 'Investimento', 'Saldo Atual', 'Lucro (R$)', 'Retorno (%)']
        
        st.dataframe(
            view.style.format({
                'PM (BRL)': 'R$ {:,.2f}', 'Investimento': 'R$ {:,.2f}',
                'Saldo Atual': 'R$ {:,.2f}', 'Lucro (R$)': 'R$ {:,.2f}', 'Retorno (%)': '{:.2f}%'
            }).map(lambda v: 'color: red' if v < 0 else 'color: green', subset=['Lucro (R$)', 'Retorno (%)']),
            use_container_width=True, hide_index=True
        )

    except Exception as e:
        st.error(f"Erro: {e}")

if __name__ == "__main__":
    main()
