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

    # 2. Ler a aba 'assets' para mapear os ativos
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]

    # Dicionário onde guardaremos todos os preços encontrados
    precos_finais = {}

    # --- PARTE A: AÇÕES, FIIs, BDRs e ETFs (Yahoo Finance) ---
    tipos_yahoo = ['ACAO_BR', 'FII', 'BDR', 'ETF_BR', 'ETF_US']
    tickers_yahoo = df_assets[df_assets['type'].isin(tipos_yahoo)]['ticker'].tolist()
    
    if tickers_yahoo:
        print(f"🔍 Buscando {len(tickers_yahoo)} ativos no Yahoo Finance...")
        try:
            data_yf = yf.download(tickers_yahoo, period="1d", group_by='ticker', progress=False)
            for t in tickers_yahoo:
                try:
                    # Lógica para tratar um ou múltiplos tickers no retorno do Yahoo
                    if len(tickers_yahoo) > 1:
                        val = data_yf[t]['Close'].iloc[-1]
                    else:
                        val = data_yf['Close'].iloc[-1]
                    
                    if pd.notnull(val):
                        precos_finais[t] = float(val)
                except:
                    precos_finais[t] = 0.0
        except Exception as e:
            print(f"⚠️ Erro no Yahoo Finance: {e}")

    # --- PARTE B: FUNDOS DE INVESTIMENTO (CVM - Link Validado) ---
    df_fundos = df_assets[df_assets['type'] == 'FUNDO']
    mapa_cnpjs = {}
    for _, row in df_fundos.iterrows():
        cnpj_raw = str(row.get('isin_cnpj', '')).strip()
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj_raw))
        if len(cnpj_limpo) == 14:
            mapa_cnpjs[cnpj_limpo] = str(row['ticker'])

    if mapa_cnpjs:
        print(f"🔍 Buscando {len(mapa_cnpjs)} fundos na CVM...")
        hoje = datetime.date.today()
        df_cvm = None
        
        # Tenta os últimos 4 meses para garantir que ache o arquivo mais recente
        for i in range(4):
            data_alvo = hoje - datetime.timedelta(days=i*28)
            mes = data_alvo.strftime('%Y%m')
            # Link validado por você
            url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"
            
            try:
                print(f" tentando baixar mês {mes}...")
                # storage_options com User-Agent para evitar bloqueios
                df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1', 
                                     storage_options={'User-Agent': 'Mozilla/5.0'})
                print(f"✅ Arquivo de {mes} baixado com sucesso!")
                break
            except Exception as e:
                print(f"⚠️ Mês {mes} indisponível ou link quebrado.")
                continue

        if df_cvm is not None:
            # Limpa e processa a base da CVM
            df_cvm['cnpj_key'] = df_cvm['CNPJ_FUNDO'].str.replace(r'\D', '', regex=True)
            df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates('CNPJ_FUNDO', keep='last')
            cvm_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()
            
            contagem_fundos = 0
            for cnpj, ticker in mapa_cnpjs.items():
                if cnpj in cvm_dict:
                    precos_finais[ticker] = float(cvm_dict[cnpj])
                    contagem_fundos += 1
            print(f"📊 {contagem_fundos} fundos atualizados via CVM.")

    # --- PARTE C: ATIVOS MANUAIS OU RENDA FIXA ---
    # Para o que sobrar (LCA, FGTS, Tesouro), mantemos 1.0 para manter o valor original do saldo
    for t in df_assets['ticker'].unique():
        t_str = str(t).strip()
        if t_str not in precos_finais:
            precos_finais[t_str] = 1.0

    # 3. Gravação Final na aba 'market_data'
    try:
        ws_market = sh.worksheet("market_data")
        ws_market.clear()
        
        # Prepara a lista final removendo qualquer valor inválido para o JSON (NaN/Inf)
        final_updates = []
        for t, p in precos_finais.items():
            clean_p = float(p) if (pd.notnull(p) and not np.isinf(p)) else 0.0
            final_updates.append([str(t), clean_p])
        
        ws_market.update(values=[['ticker', 'close_price']] + final_updates, range_name='A1')
        print(f"🚀 Sucesso! {len(final_updates)} ativos consolidados na market_data.")
        
    except Exception as e:
        print(f"❌ Erro ao gravar na planilha: {e}")

if __name__ == "__main__":
    update_all_market_data()
