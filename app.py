import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime

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
        creds_json = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
        creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
        sh = client.open_by_key(ID_PLANILHA)
        
        return {
            "assets": pd.DataFrame(sh.worksheet("assets").get_all_records()),
            "trans": pd.DataFrame(sh.worksheet("transactions").get_all_records()),
            "market": pd.DataFrame(sh.worksheet("market_data").get_all_records()),
            "calendar": pd.DataFrame(sh.worksheet("dividend_calendar").get_all_records())
        }
    except Exception as e:
        st.error(f"Erro: {e}")
        return None

def main():
    st.set_page_config(page_title="Rentabilidade Real", layout="wide")
    data = load_data()
    if not data: return

    # Padronização
    df_as = data["assets"]; df_as.columns = [c.lower().strip() for c in df_as.columns]
    df_tr = data["trans"]; df_tr.columns = [c.lower().strip() for c in df_tr.columns]
    df_mk = data["market"]; df_mk.columns = [c.lower().strip() for c in df_mk.columns]
    df_cl = data["calendar"]; df_cl.columns = [c.lower().strip() for c in df_cl.columns]

    # Processamento de Valores
    df_tr['quantity'] = df_tr['quantity'].apply(clean_num)
    df_tr['price'] = df_tr['price'].apply(clean_num)
    df_mk['close_price'] = df_mk['close_price'].apply(clean_num)
    usd_val = df_mk[df_mk['ticker'] == 'USDBRL=X']['close_price'].values[0] if 'USDBRL=X' in df_mk['ticker'].values else 1.0

    # 1. CÁLCULO DE CUSTO E POSIÇÃO
    # Agrupamos por ticker para achar preço médio e custo total
    df_tr['custo_total_brl'] = df_tr.apply(lambda r: r['quantity'] * r['price'] * (usd_val if "USD" in str(r.get('currency','')).upper() else 1.0), axis=1)
    
    resumo = df_tr.groupby('ticker').agg({
        'quantity': 'sum',
        'custo_total_brl': 'sum'
    }).reset_index()
    
    resumo = resumo[resumo['quantity'] > 0] # Apenas o que temos em carteira
    resumo = resumo.merge(df_mk[['ticker', 'close_price']], on='ticker', how='left')
    resumo = resumo.merge(df_as[['ticker', 'type', 'currency']], on='ticker', how='left')

    # Valor Atual
    resumo['valor_atual_brl'] = resumo.apply(
        lambda r: (r['quantity'] * r['close_price']) * (usd_val if str(r['currency']).upper() == 'USD' else 1.0), axis=1
    )

    # 2. CÁLCULO DE LUCRO DE CAPITAL (Variação de Preço)
    resumo['lucro_capital'] = resumo['valor_atual_brl'] - resumo['custo_total_brl']

    # 3. CÁLCULO DE PROVENTOS RECEBIDOS (Simulação baseada no histórico da aba Calendar)
    # Aqui somamos o que o robô já baixou para a sua planilha
    proventos_por_ticker = df_cl.groupby('ticker')['valor'].apply(lambda x: x.apply(clean_num).sum()).to_dict()
    
    # Estimativa de proventos totais (Valor unitário acumulado * sua quantidade atual)
    resumo['proventos_totais'] = resumo.apply(lambda r: proventos_por_ticker.get(r['ticker'], 0) * r['quantity'], axis=1)

    # 4. RENTABILIDADE TOTAL (Yield on Cost + Valorização)
    resumo['resultado_total'] = resumo['lucro_capital'] + resumo['proventos_totais']
    resumo['rentabilidade_pct'] = (resumo['resultado_total'] / resumo['custo_total_brl']) * 100

    # --- EXIBIÇÃO ---
    st.title("🚀 Performance Real da Carteira")
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Patrimônio Atual", f"R$ {resumo['valor_atual_brl'].sum():,.2f}")
    c2.metric("Total de Proventos", f"R$ {resumo['proventos_totais'].sum():,.2f}", delta_color="normal")
    c3.metric("Lucro de Capital", f"R$ {resumo['lucro_capital'].sum():,.2f}")
    
    rent_total_consolidada = (resumo['resultado_total'].sum() / resumo['custo_total_brl'].sum()) * 100
    c4.metric("Rentabilidade Real", f"{rent_total_consolidada:.2f}%")

    st.divider()
    st.subheader("📊 Detalhamento por Ativo")
    
    view = resumo[['ticker', 'type', 'quantity', 'custo_total_brl', 'valor_atual_brl', 'proventos_totais', 'resultado_total', 'rentabilidade_pct']]
    
    st.dataframe(
        view.sort_values('rentabilidade_pct', ascending=False),
        column_config={
            "custo_total_brl": st.column_config.NumberColumn("Investido", format="R$ %.2f"),
            "valor_atual_brl": st.column_config.NumberColumn("Valor Atual", format="R$ %.2f"),
            "proventos_totais": st.column_config.NumberColumn("Proventos", format="R$ %.2f"),
            "resultado_total": st.column_config.NumberColumn("Lucro Total", format="R$ %.2f"),
            "rentabilidade_pct": st.column_config.NumberColumn("Retorno %", format="%.2f%%")
        },
        use_container_width=True, hide_index=True
    )

if __name__ == "__main__":
    main()
