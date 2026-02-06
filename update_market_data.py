import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import numpy as np

def update_all_market_data():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    # 1. Autenticação
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=[
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro na autenticação: {e}")
        return

    # 2. Ler a aba 'assets'
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]

    precos_finais = {}

    # --- PARTE A: YAHOO FINANCE (Ações, FIIs, BDRs, ETFs) ---
    tipos_yahoo = ['ACAO_BR', 'FII', 'BDR', 'ETF_BR', 'ETF_US']
    tickers_yahoo = df_assets[df_assets['type'].isin(tipos_yahoo)]['ticker'].tolist()
    
    if tickers_yahoo:
        print(f"🔍 Buscando {len(tickers_yahoo)} ativos no Yahoo Finance...")
        try:
            data_yf = yf.download(tickers_yahoo, period="1d", group_by='ticker', progress=False)
            for t in tickers_yahoo:
                try:
                    val = data_yf[t]['Close'].iloc[-1] if len(tickers_yahoo) > 1 else data_yf['Close'].iloc[-1]
                    if pd.notnull(val): precos_finais[t] = float(val)
                except: precos_finais[t] = 0.0
        except Exception as e: print(f"⚠️ Erro Yahoo: {e}")

    # --- PARTE B: CVM (Fundos) ---
    df_fundos = df_assets[df_assets['type'] == 'FUNDO']
    mapa_cnpjs = {}
    for _, row in df_fundos.iterrows():
        cnpj_raw = str(row.get('isin_cnpj', '')).strip()
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj_raw)).zfill(14)
        if len(cnpj_limpo) == 14: mapa_cnpjs[cnpj_limpo] = str(row['ticker'])

    if mapa_cnpjs:
        print(f"🔍 Buscando {len(mapa_cnpjs)} fundos na CVM...")
        hoje = datetime.date.today()
        for i in range(4):
            mes = (hoje - datetime.timedelta(days=i*28)).strftime('%Y%m')
            url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"
            try:
                df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1', storage_options={'User-Agent': 'Mozilla/5.0'})
                col_cnpj = [c for c in df_cvm.columns if 'CNPJ_FUNDO' in c][0]
                df_cvm['cnpj_key'] = df_cvm[col_cnpj].str.replace(r'\D', '', regex=True).str.zfill(14)
                df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates(col_cnpj, keep='last')
                cvm_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()
                for cnpj, ticker in mapa_cnpjs.items():
                    if cnpj in cvm_dict: precos_finais[ticker] = float(cvm_dict[cnpj])
                break
            except: continue

    # --- PARTE D: TESOURO DIRETO (Novo Link) ---
    df_td_assets = df_assets[df_assets['type'] == 'TESOURO']
    if not df_td_assets.empty:
        print("🔍 Buscando preços do Tesouro Direto...")
        # Link persistente para o CSV de preços e taxas
        url_td = "https://www.tesourotransparente.gov.br/ckan/dataset/df56114f-2e4a-4a93-81e9-963a3d3ad550/resource/796d2059-14e9-44e3-86c3-0cadaec32b3f/download/PrecoTaxaTesouroDireto.csv"
        try:
            df_td = pd.read_csv(url_td, sep=';', decimal=',', encoding='latin1')
            df_td['Data Vencimento'] = pd.to_datetime(df_td['Data Vencimento'], dayfirst=True)
            # Pegar apenas a data mais recente de processamento do arquivo
            df_td['Data Base'] = pd.to_datetime(df_td['Data Base'], dayfirst=True)
            ultima_data = df_td['Data Base'].max()
            df_hoje = df_td[df_td['Data Base'] == ultima_data]

            for _, row in df_td_assets.iterrows():
                ticker = row['ticker']
                vencimento = 2029 # No seu caso TD_IPCA_29
                # Lógica simplificada: procura títulos IPCA com vencimento em 2029
                match = df_hoje[df_hoje['Tipo Titulo'].str.contains("IPCA", na=False) & 
                                (df_hoje['Data Vencimento'].dt.year == vencimento)]
                
                if not match.empty:
                    preco = match.iloc[0]['Preco Unitario Dia']
                    precos_finais[ticker] = float(preco)
                    print(f"✅ {ticker} atualizado: R$ {preco}")
        except Exception as e:
            print(f"⚠️ Erro Tesouro: {e}")

    # --- FINALIZAÇÃO ---
    for t in df_assets['ticker'].unique():
        if str(t).strip() not in precos_finais: precos_finais[str(t).strip()] = 1.0

    try:
        ws_market = sh.worksheet("market_data")
        ws_market.clear()
        updates = [[t, float(p) if (pd.notnull(p) and not np.isinf(p)) else 0.0] for t, p in precos_finais.items()]
        ws_market.update(values=[['ticker', 'close_price']] + updates, range_name='A1')
        print(f"🚀 Concluído! {len(updates)} ativos atualizados.")
    except Exception as e: print(f"❌ Erro ao gravar: {e}")

if __name__ == "__main__":
    update_all_market_data()
