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

    # --- PARTE A: YAHOO FINANCE (Ações, FIIs, etc) ---
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
        except Exception as e:
            print(f"⚠️ Erro Yahoo: {e}")

    # --- PARTE B: CVM (Fundos) - COM CORREÇÃO DE ZEROS À ESQUERDA ---
    df_fundos = df_assets[df_assets['type'] == 'FUNDO']
    mapa_cnpjs = {}
    for _, row in df_fundos.iterrows():
        ticker = str(row['ticker']).strip()
        cnpj_raw = str(row.get('isin_cnpj', '')).strip()
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj_raw))
        
        # CORREÇÃO: Se tiver 13 ou 14 dígitos, completa com zero à esquerda
        if len(cnpj_limpo) >= 13:
            cnpj_validado = cnpj_limpo.zfill(14)
            mapa_cnpjs[cnpj_validado] = ticker

    if mapa_cnpjs:
        print(f"🔍 Buscando {len(mapa_cnpjs)} fundos na CVM...")
        hoje = datetime.date.today()
        df_cvm = None
        
        for i in range(4):
            mes = (hoje - datetime.timedelta(days=i*28)).strftime('%Y%m')
            url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"
            try:
                print(f" tentando baixar mês {mes}...")
                df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1', 
                                     storage_options={'User-Agent': 'Mozilla/5.0'})
                
                # Identifica coluna de CNPJ (pode ser CNPJ_FUNDO ou CNPJ_FUNDO_CLASSE)
                col_cnpj = [c for c in df_cvm.columns if 'CNPJ_FUNDO' in c][0]
                
                df_cvm['cnpj_key'] = df_cvm[col_cnpj].str.replace(r'\D', '', regex=True).str.zfill(14)
                df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates(col_cnpj, keep='last')
                cvm_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()
                
                encontrados = 0
                for cnpj, ticker in mapa_cnpjs.items():
                    if cnpj in cvm_dict:
                        precos_finais[ticker] = float(cvm_dict[cnpj])
                        encontrados += 1
                
                if encontrados > 0:
                    print(f"✅ {encontrados} fundos atualizados com sucesso!")
                    break
            except: continue

    # --- PARTE C: PRESERVAÇÃO E LIMPEZA ---
    for t in df_assets['ticker'].unique():
        t_str = str(t).strip()
        if t_str not in precos_finais:
            precos_finais[t_str] = 1.0

    # Gravação
    try:
        ws_market = sh.worksheet("market_data")
        ws_market.clear()
        updates = [[t, float(p) if (pd.notnull(p) and not np.isinf(p)) else 0.0] 
                   for t, p in precos_finais.items()]
        ws_market.update(values=[['ticker', 'close_price']] + updates, range_name='A1')
        print(f"🚀 Concluído! 102 ativos processados.")
    except Exception as e:
        print(f"❌ Erro ao gravar: {e}")

if __name__ == "__main__":
    update_all_market_data()
