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
        
        # 5. KPI METRICS (Topo)
        t_atual = resumo['saldo_atual'].sum()
        t_inv = resumo['total_invested_brl'].sum()
        lucro_total = t_atual - t_inv
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Patrimônio Total", f"R$ {t_atual:,.2f}")
        m2.metric("Total Investido", f"R$ {t_inv:,.2f}")
        m3.metric("Lucro/Prejuízo Total", f"R$ {lucro_total:,.2f}", f"{(lucro_total/t_inv)*100:.2f}%" if t_inv > 0 else "0%")

        # 6. TABELA PRINCIPAL DE CARTEIRA
        st.subheader("📊 Minha Carteira")
        view_carteira = resumo[['name', 'type', 'quantity', 'preco_medio', 'saldo_atual', 'lucro_abs', 'rentabilidade']].copy()
        view_carteira.columns = ['Nome', 'Tipo', 'Qtd', 'Preço Médio', 'Saldo Atual', 'Lucro (R$)', 'Retorno (%)']
        
        st.dataframe(
            view_carteira.style.format({
                'Preço Médio': 'R$ {:,.2f}', 'Saldo Atual': 'R$ {:,.2f}', 
                'Lucro (R$)': 'R$ {:,.2f}', 'Retorno (%)': '{:.2f}%'
            }).map(lambda v: 'color: red' if v < 0 else 'color: green', subset=['Lucro (R$)', 'Retorno (%)']),
            use_container_width=True, hide_index=True
        )

        # 7. SEÇÃO DE PROVENTOS ESTIMADOS (Calendário)
        st.divider()
        st.subheader("💰 Próximos Recebimentos Estimados")
        
        if not df_cal.empty:
            # Filtramos os proventos confirmados para cruzar com a carteira
            df_cal['valor'] = df_cal['valor'].apply(clean_num)
            df_futuro = df_cal[df_cal['status'].str.contains('Confirmado', case=False, na=False)].copy()
            
            if not df_futuro.empty:
                # Merge com o resumo para saber quanto o usuário tem do ativo
                previsao = df_futuro.merge(resumo[['ticker', 'quantity']], left_on='ticker', right_on='ticker', how='inner')
                previsao['recebimento_estimado'] = previsao['valor'] * previsao['quantity']
                
                total_previsto = previsao['recebimento_estimado'].sum()
                if total_previsto > 0:
                    st.success(f"💵 Total previsto para os próximos pagamentos: **R$ {total_previsto:,.2f}**")
                    
                    df_show_previsao = previsao[['ticker', 'data (pagto/ex)', 'valor', 'quantity', 'recebimento_estimado']].copy()
                    df_show_previsao.columns = ['Ticker', 'Data Pagto/Ex', 'Valor Un.', 'Qtd Possuída', 'Total a Receber']
                    
                    st.dataframe(df_show_previsao.style.format({
                        'Valor Un.': 'R$ {:,.4f}', 'Qtd Possuída': '{:,.0f}', 'Total a Receber': 'R$ {:,.2f}'
                    }), use_container_width=True, hide_index=True)
                else:
                    st.info("Você possui os ativos do calendário, mas as quantidades registradas são zero.")
            else:
                st.write("Sem novos anúncios confirmados. Veja o histórico recente capturado:")
                st.dataframe(df_cal.head(10), use_container_width=True)
        else:
            st.info("Nenhum dado de calendário disponível. Verifique se o script update_dividends.py rodou.")

    else:
        st.warning("Carregando dados...")

if __name__ == "__main__":
    main()
