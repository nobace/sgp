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

    # 2. Ler a aba 'assets' para classificar o que buscar em cada lugar
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]

    # Dicionário final de preços que iremos montar
    precos_finais = {}

    # --- PARTE A: AÇÕES, FIIs, BDRs e ETFs (Yahoo Finance) ---
    # Filtramos pelos tipos que sabemos que estão no Yahoo
    tipos_yahoo = ['ACAO_BR', 'FII', 'BDR', 'ETF_BR', 'ETF_US']
    tickers_yahoo = df_assets[df_assets['type'].isin(tipos_yahoo)]['ticker'].tolist()
    
    if tickers_yahoo:
        print(f"🔍 Buscando {len(tickers_yahoo)} ativos no Yahoo Finance...")
        try:
            # group_by='ticker' garante que conseguimos extrair mesmo se um falhar
            data_yf = yf.download(tickers_yahoo, period="1d", group_by='ticker', progress=False)
            for t in tickers_yahoo:
                try:
                    val = data_yf[t]['Close'].iloc[-1] if len(tickers_yahoo) > 1 else data_yf['Close'].iloc[-1]
                    if pd.notnull(val):
                        precos_finais[t] = float(val)
                except:
                    precos_finais[t] = 0.0
        except Exception as e:
            print(f"⚠️ Erro ao acessar Yahoo: {e}")

    # --- PARTE B: FUNDOS DE INVESTIMENTO (CVM) ---
    # Mapeamos tickers que têm CNPJ na coluna isin_cnpj
    df_fundos = df_assets[df_assets['type'] == 'FUNDO']
    mapa_cnpjs = {}
    for _, row in df_fundos.iterrows():
        cnpj_limpo = ''.join(filter(str.isdigit, str(row['isin_cnpj'])))
        if len(cnpj_limpo) == 14:
            mapa_cnpjs[cnpj_limpo] = str(row['ticker'])

    if mapa_cnpjs:
        print(f"🔍 Buscando {len(mapa_cnpjs)} fundos na CVM...")
        hoje = datetime.date.today()
        for i in range(2): # Tenta mês atual e anterior
            mes = (hoje - datetime.timedelta(days=i*28)).strftime('%Y%m')
            url = f"https://dados.cvm.gov.br/dados/FIE/MED/DIARIO/DADOS/inf_diario_fie_{mes}.zip"
            try:
                df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1')
                df_cvm['cnpj_key'] = df_cvm['CNPJ_FUNDO'].str.replace(r'\D', '', regex=True)
                df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates('CNPJ_FUNDO', keep='last')
                cvm_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()
                
                for cnpj, ticker in mapa_cnpjs.items():
                    if cnpj in cvm_dict:
                        precos_finais[ticker] = float(cvm_dict[cnpj])
                break
            except: continue

    # --- PARTE C: RENDA FIXA E OUTROS (Preço Manual/Fixo) ---
    # Para o que sobrar (LCA, FGTS, Tesouro), garantimos que o preço não seja zero
    # para não quebrar o cálculo de patrimônio (Quantidade x 1.0)
    for t in df_assets['ticker'].unique():
        if t not in precos_finais:
            precos_finais[t] = 1.0

    # 3. Gravação Final na aba 'market_data'
    try:
        ws_market = sh.worksheet("market_data")
        ws_market.clear()
        
        # Preparamos a lista garantindo que não há valores inválidos (NaN)
        updates = [[t, float(p) if (pd.notnull(p) and not np.isinf(p)) else 0.0] 
                   for t, p in precos_finais.items()]
        
        ws_market.update(values=[['ticker', 'close_price']] + updates, range_name='A1')
        print(f"🚀 Sucesso! {len(updates)} ativos atualizados na market_data.")
    except Exception as e:
        print(f"❌ Erro ao gravar: {e}")

if __name__ == "__main__":
    update_all_market_data()
