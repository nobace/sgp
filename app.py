import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json

def load_data():
    info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
    creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    client = gspread.authorize(creds)
    sh = client.open_by_key("1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8")
    
    # Carregar abas
    assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    trans = pd.DataFrame(sh.worksheet("transactions").get_all_records())
    market = pd.DataFrame(sh.worksheet("market_data").get_all_records())
    
    return assets, trans, market

def main():
    st.set_page_config(page_title="Meu Dashboard de Investimentos", layout="wide")
    st.title("🚀 Gestão de Patrimônio")
    
    try:
        df_assets, df_trans, df_market = load_data()
        
        # 1. Processamento de Dados
        # Agrupar transações por ticker
        resumo = df_trans.groupby('ticker').agg({'quantity': 'sum', 'cost': 'sum'}).reset_index()
        
        # Merge com preços atuais e nomes
        resumo = resumo.merge(df_market, on='ticker', how='left')
        resumo = resumo.merge(df_assets[['ticker', 'name', 'type']], on='ticker', how='left')
        
        # Cálculos
        resumo['Saldo Atual'] = resumo['quantity'] * resumo['close_price'].astype(float)
        resumo['Lucro'] = resumo['Saldo Atual'] - resumo['cost']
        resumo['Rentabilidade'] = (resumo['Lucro'] / resumo['cost']) * 100
        
        # 2. Layout do Dashboard
        m1, m2, m3 = st.columns(3)
        total_atual = resumo['Saldo Atual'].sum()
        total_investido = resumo['cost'].sum()
        total_lucro = total_atual - total_investido
        
        m1.metric("Patrimônio Total", f"R$ {total_atual:,.2f}")
        m2.metric("Total Investido", f"R$ {total_investido:,.2f}")
        m3.metric("Lucro Total", f"R$ {total_lucro:,.2f}", f"{(total_lucro/total_investido)*100:.2f}%")

        st.divider()
        
        # 3. Tabela Consolidada
        st.subheader("📊 Performance por Ativo")
        if not df_market.empty:
            st.caption(f"🕒 Preços atualizados em: {df_market['last_update'].iloc[0]}")
            
        df_exibir = resumo[['name', 'type', 'quantity', 'cost', 'Saldo Atual', 'Lucro', 'Rentabilidade']]
        df_exibir.columns = ['Nome', 'Tipo', 'Qtd', 'Custo Total', 'Saldo Atual', 'Lucro (R$)', 'Retorno (%)']
        
        st.dataframe(
            df_exibir.style.format({
                'Custo Total': 'R$ {:,.2f}',
                'Saldo Atual': 'R$ {:,.2f}',
                'Lucro (R$)': 'R$ {:,.2f}',
                'Retorno (%)': '{:.2f}%'
            }).map(lambda v: 'color: red' if v < 0 else 'color: green', subset=['Lucro (R$)', 'Retorno (%)']),
            use_container_width=True, hide_index=True
        )
        
        # 4. Gráfico de Alocação
        st.subheader("🏗️ Alocação por Classe")
        setores = resumo.groupby('type')['Saldo Atual'].sum()
        st.pydeck_chart # (Opcional: substituir por st.plotly_chart se quiser gráfico de pizza)
        st.bar_chart(setores)

    except Exception as e:
        st.error(f"Erro ao carregar dashboard: {e}")

if __name__ == "__main__":
    main()
