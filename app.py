import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def clean_financial_v2(value):
    """
    Remove pontos de milhar e converte vírgula decimal para ponto.
    Ex: '1.957,00' -> '1957.00' -> 1957.0
    """
    if value is None or value == "":
        return 0.0
    
    # Transforma em string e remove espaços
    s = str(value).strip()
    
    # Se o número vier no formato 1.957,00
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    # Se vier apenas com vírgula (580,00)
    elif "," in s:
        s = s.replace(",", ".")
    
    try:
        return float(s)
    except:
        return 0.0

def load_data():
    creds_json = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    client = gspread.authorize(creds)
    sh = client.open_by_key("1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8")
    
    # Forçamos a leitura como string para o Pandas não tentar 'adivinhar' e errar
    assets = pd.DataFrame(sh.worksheet("assets").get_all_records(numericise_ignore=['all']))
    trans = pd.DataFrame(sh.worksheet("transactions").get_all_records(numericise_ignore=['all']))
    market = pd.DataFrame(sh.worksheet("market_data").get_all_records(numericise_ignore=['all']))
    
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

        # 1. Aplica limpeza pesada nas colunas numéricas
        cols_financeiras = ['quantity', 'price', 'costs', 'exchange_rate']
        for col in cols_financeiras:
            if col in df_trans.columns:
                df_trans[col] = df_trans[col].apply(clean_financial_v2)
        
        if 'close_price' in df_market.columns:
            df_market['close_price'] = df_market['close_price'].apply(clean_financial_v2)

        # 2. Lógica de Custo de Aquisição (Custo Médio / Investimento Inicial)
        # Se 'costs' estiver zerado (como vimos na sua amostra), usamos Qtd * Preço * Câmbio
        df_trans['investimento_real'] = df_trans.apply(
            lambda r: r['costs'] if r['costs'] > 0 
            else (r['quantity'] * r['price'] * (r['exchange_rate'] if r['exchange_rate'] > 0 else 1.0)),
            axis=1
        )

        # 3. Consolidação por Ativo
        resumo = df_trans.groupby('ticker').agg({
            'quantity': 'sum', 
            'investimento_real': 'sum'
        }).reset_index()
        
        # Merge com Preços de Mercado e Informações de Ativos
        resumo = resumo.merge(df_market[['ticker', 'close_price', 'last_update']], on='ticker', how='left')
        resumo = resumo.merge(df_assets[['ticker', 'name', 'type']], on='ticker', how='left')
        
        # 4. Cálculos de Performance
        resumo['Saldo Atual'] = resumo['quantity'] * resumo['close_price'].fillna(0)
        resumo['Lucro'] = resumo['Saldo Atual'] - resumo['investimento_real']
        resumo['Rentabilidade'] = np.where(
            resumo['investimento_real'] > 0.01, # Evita divisão por zero
            (resumo['Lucro'] / resumo['investimento_real']) * 100, 
            0
        )
        
        # 5. Métricas de Resumo
        m1, m2, m3 = st.columns(3)
        total_atual = resumo['Saldo Atual'].sum()
        total_investido = resumo['investimento_real'].sum()
        total_lucro = total_atual - total_investido
        retorno_global = (total_lucro / total_investido * 100) if total_investido > 0 else 0
        
        m1.metric("Patrimônio Total", f"R$ {total_atual:,.2f}")
        m2.metric("Total Investido", f"R$ {total_investido:,.2f}")
        m3.metric("Lucro Total", f"R$ {total_lucro:,.2f}", f"{retorno_global:.2f}%")

        st.divider()
        
        # 6. Exibição da Tabela Consolidada
        st.subheader("📊 Performance por Ativo")
        if not df_market.empty and 'last_update' in df_market.columns:
            st.caption(f"🕒 Dados de mercado atualizados em: {df_market['last_update'].iloc[0]}")
            
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
        st.error(f"Erro ao processar dados: {e}")
        st.exception(e) # Mostra o erro detalhado para debug

if __name__ == "__main__":
    main()
