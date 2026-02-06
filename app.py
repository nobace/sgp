import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def load_data():
    # Carrega credenciais do Secrets do Streamlit
    creds_json = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    client = gspread.authorize(creds)
    sh = client.open_by_key("1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8")
    
    # Lê as abas
    assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    trans = pd.DataFrame(sh.worksheet("transactions").get_all_records())
    market = pd.DataFrame(sh.worksheet("market_data").get_all_records())
    
    return assets, trans, market

def main():
    st.set_page_config(page_title="Dashboard Patrimonial", layout="wide")
    st.title("🚀 Gestão de Patrimônio")
    
    try:
        df_assets, df_trans, df_market = load_data()
        
        # Padroniza nomes de colunas (minúsculo e sem espaço)
        df_trans.columns = [c.lower().strip() for c in df_trans.columns]
        df_assets.columns = [c.lower().strip() for c in df_assets.columns]
        df_market.columns = [c.lower().strip() for c in df_market.columns]

        # LÓGICA DE CUSTO: Mapeia 'costs' para 'cost' para o cálculo funcionar
        if 'costs' in df_trans.columns:
            df_trans = df_trans.rename(columns={'costs': 'cost'})
        
        if 'cost' not in df_trans.columns:
            st.error("Coluna 'costs' não encontrada na aba transactions.")
            return

        # 1. Processamento
        # Agrupa quantidade e custo por ticker
        resumo = df_trans.groupby('ticker').agg({'quantity': 'sum', 'cost': 'sum'}).reset_index()
        
        # Merge com preços e nomes
        resumo = resumo.merge(df_market, on='ticker', how='left')
        resumo = resumo.merge(df_assets[['ticker', 'name', 'type']], on='ticker', how='left')
        
        # Garante que os números são floats
        resumo['close_price'] = pd.to_numeric(resumo['close_price'], errors='coerce').fillna(0)
        resumo['cost'] = pd.to_numeric(resumo['cost'], errors='coerce').fillna(0)
        
        # Cálculos finais
        resumo['Saldo Atual'] = resumo['quantity'] * resumo['close_price']
        resumo['Lucro'] = resumo['Saldo Atual'] - resumo['cost']
        resumo['Rentabilidade'] = np.where(resumo['cost'] != 0, (resumo['Lucro'] / resumo['cost']) * 100, 0)
        
        # 2. Métricas de Cabeçalho
        m1, m2, m3 = st.columns(3)
        total_atual = resumo['Saldo Atual'].sum()
        total_investido = resumo['cost'].sum()
        total_lucro = total_atual - total_investido
        retorno_global = (total_lucro / total_investido * 100) if total_investido != 0 else 0
        
        m1.metric("Patrimônio Total", f"R$ {total_atual:,.2f}")
        m2.metric("Total Investido", f"R$ {total_investido:,.2f}")
        m3.metric("Lucro Total", f"R$ {total_lucro:,.2f}", f"{retorno_global:.2f}%")

        st.divider()
        
        # 3. Tabela de Ativos
        st.subheader("📊 Performance por Ativo")
        if not df_market.empty and 'last_update' in df_market.columns:
            st.caption(f"🕒 Preços atualizados em: {df_market['last_update'].iloc[0]}")
            
        view = resumo[['name', 'type', 'quantity', 'cost', 'Saldo Atual', 'Lucro', 'Rentabilidade']].copy()
        view.columns = ['Nome', 'Tipo', 'Qtd', 'Custo Total', 'Saldo Atual', 'Lucro (R$)', 'Retorno (%)']
        
        st.dataframe(
            view.style.format({
                'Custo Total': 'R$ {:,.2f}',
                'Saldo Atual': 'R$ {:,.2f}',
                'Lucro (R$)': 'R$ {:,.2f}',
                'Retorno (%)': '{:.2f}%'
            }).map(lambda v: 'color: red' if v < 0 else 'color: green', subset=['Lucro (R$)', 'Retorno (%)']),
            use_container_width=True, hide_index=True
        )

    except Exception as e:
        st.error(f"Erro ao carregar dashboard: {e}")

if __name__ == "__main__":
    main()
