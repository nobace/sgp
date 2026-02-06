import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def fix_br_numbers(series):
    """Converte strings no formato '1.234,56' ou zeros para float numérico."""
    def clean(val):
        if val is None or val == "" or str(val).strip() == "0":
            return 0.0
        val = str(val).replace('.', '').replace(',', '.')
        try:
            return float(val)
        except:
            return 0.0
    return series.apply(clean)

def load_data():
    creds_json = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    client = gspread.authorize(creds)
    sh = client.open_by_key("1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8")
    
    # Carregando abas como DataFrames brutos (sem converter números ainda)
    assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    trans = pd.DataFrame(sh.worksheet("transactions").get_all_records())
    market = pd.DataFrame(sh.worksheet("market_data").get_all_records())
    
    return assets, trans, market

def main():
    st.set_page_config(page_title="Dashboard Patrimonial", layout="wide")
    st.title("🚀 Gestão de Patrimônio")
    
    try:
        df_assets, df_trans, df_market = load_data()
        
        # Padroniza nomes de colunas
        df_trans.columns = [c.lower().strip() for c in df_trans.columns]
        df_assets.columns = [c.lower().strip() for c in df_assets.columns]
        df_market.columns = [c.lower().strip() for c in df_market.columns]

        # 1. Tratamento de Números Brasileiro (Vírgulas e Pontos)
        for col in ['quantity', 'price', 'costs', 'exchange_rate']:
            if col in df_trans.columns:
                df_trans[col] = fix_br_numbers(df_trans[col])
        
        if 'close_price' in df_market.columns:
            df_market['close_price'] = fix_br_numbers(df_market['close_price'])

        # 2. Lógica de Custo Inteligente (Se costs=0, calcula Qtd * Preço * Câmbio)
        df_trans['investimento_real'] = df_trans['costs']
        # Identifica linhas onde o custo está zerado mas temos preço e quantidade
        mask_zero_cost = (df_trans['investimento_real'] == 0) & (df_trans['price'] > 0)
        df_trans.loc[mask_zero_cost, 'investimento_real'] = (
            df_trans['quantity'] * df_trans['price'] * df_trans['exchange_rate'].replace(0, 1)
        )

        # 3. Consolidação por Ticker
        resumo = df_trans.groupby('ticker').agg({
            'quantity': 'sum', 
            'investimento_real': 'sum'
        }).reset_index()
        
        # Merge com Market Data e Assets
        resumo = resumo.merge(df_market, on='ticker', how='left')
        resumo = resumo.merge(df_assets[['ticker', 'name', 'type']], on='ticker', how='left')
        
        # Cálculos de Performance
        resumo['Saldo Atual'] = resumo['quantity'] * resumo['close_price'].fillna(0)
        resumo['Lucro'] = resumo['Saldo Atual'] - resumo['investimento_real']
        resumo['Rentabilidade'] = np.where(
            resumo['investimento_real'] != 0, 
            (resumo['Lucro'] / resumo['investimento_real']) * 100, 
            0
        )
        
        # 4. Métricas de Cabeçalho
        total_atual = resumo['Saldo Atual'].sum()
        total_investido = resumo['investimento_real'].sum()
        total_lucro = total_atual - total_investido
        retorno_global = (total_lucro / total_investido * 100) if total_investido != 0 else 0
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Patrimônio Total", f"R$ {total_atual:,.2f}")
        m2.metric("Total Investido", f"R$ {total_investido:,.2f}")
        m3.metric("Lucro Total", f"R$ {total_lucro:,.2f}", f"{retorno_global:.2f}%")

        st.divider()
        
        # 5. Tabela Consolidada
        st.subheader("📊 Performance por Ativo")
        if not df_market.empty and 'last_update' in df_market.columns:
            st.caption(f"🕒 Preços atualizados em: {df_market['last_update'].iloc[0]}")
            
        view = resumo[['name', 'type', 'quantity', 'investimento_real', 'Saldo Atual', 'Lucro', 'Rentabilidade']].copy()
        view.columns = ['Nome', 'Tipo', 'Qtd', 'Investimento', 'Saldo Atual', 'Lucro (R$)', 'Retorno (%)']
        
        st.dataframe(
            view.style.format({
                'Investimento': 'R$ {:,.2f}',
                'Saldo Atual': 'R$ {:,.2f}',
                'Lucro (R$)': 'R$ {:,.2f}',
                'Retorno (%)': '{:.2f}%'
            }).map(lambda v: 'color: red' if v < 0 else 'color: green', subset=['Lucro (R$)', 'Retorno (%)']),
            use_container_width=True, hide_index=True
        )

    except Exception as e:
        st.error(f"Erro ao carregar dashboard: {e}")
        st.exception(e)

if __name__ == "__main__":
    main()
