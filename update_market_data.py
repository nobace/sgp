import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import numpy as np
import requests

def get_tesouro_url():
    api_url = "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=taxas-do-tesouro-direto"
    try:
        response = requests.get(api_url, timeout=30)
        data = response.json()
        resources = data['result']['resources']
        for res in resources:
            if "Preco" in res['name'] and "Taxa" in res['name'] and res['format'].lower() == "csv":
                return res['url']
    except: pass
    return "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv"

def update_all_market_data():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    # Lendo as abas
    df_assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    df_trans = pd.DataFrame(sh.worksheet("transactions").get_all_records())
    df_trans.columns = [c.lower().strip() for c in df_trans.columns]

    precos_finais = {}
    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    # --- PARTE A: YAHOO FINANCE ---
    tipos_yahoo = ['ACAO_BR', 'FII', 'BDR', 'ETF_BR', 'ETF_US']
    tickers_yahoo = df_assets[df_assets['type'].isin(tipos_yahoo)]['ticker'].unique().tolist()
    if tickers_yahoo:
        try:
            data_yf = yf.download(tickers_yahoo, period="1d", group_by='ticker', progress=False)
            for t in tickers_yahoo:
                try:
                    val = data_yf[t]['Close'].iloc[-1] if len(tickers_yahoo) > 1 else data_yf['Close'].iloc[-1]
                    if pd.notnull(val): precos_finais[str(t).strip()] = float(val)
                except: pass
        except: pass

    # --- PARTE B: CVM (Fundos) ---
    df_fundos = df_assets[df_assets['type'] == 'FUNDO']
    mapa_cnpjs = {str(r['isin_cnpj']).replace('.','').replace('-','').replace('/','').zfill(14): str(r['ticker']).strip() 
                  for _, r in df_fundos.iterrows() if r.get('isin_cnpj')}
    if mapa_cnpjs:
        hoje = datetime.date.today()
        for i in range(3):
            mes = (hoje - datetime.timedelta(days=i*28)).strftime('%Y%m')
            url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"
            try:
                df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1', timeout=60)
                df_cvm['cnpj_key'] = df_cvm['CNPJ_FUNDO_CLASSE'].str.replace(r'\D', '', regex=True).str.zfill(14)
                df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates('cnpj_key', keep='last')
                cvm_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()
                for cnpj, ticker in mapa_cnpjs.items():
                    if cnpj in cvm_dict: precos_finais[ticker] = float(cvm_dict[cnpj])
                break
            except: continue

    # --- PARTE C: TESOURO DIRETO ---
    df_td_assets = df_assets[df_assets['type'] == 'TESOURO']
    if not df_td_assets.empty:
        try:
            df_td = pd.read_csv(get_tesouro_url(), sep=';', decimal=',', encoding='latin1')
            df_td['Data Base'] = pd.to_datetime(df_td['Data Base'], dayfirst=True)
            df_hoje = df_td[df_td['Data Base'] == df_td['Data Base'].max()]
            for _, row in df_td_assets.iterrows():
                ticker = str(row['ticker']).strip().upper()
                ano = 2000 + int(''.join(filter(str.isdigit, ticker)))
                tipo = "IPCA+" if "IPCA" in ticker else "Selic" if "SELIC" in ticker else "Prefixado"
                mask = df_hoje['Tipo Titulo'].str.contains(tipo, case=False) & (pd.to_datetime(df_hoje['Data Vencimento'], dayfirst=True).dt.year == ano)
                if not df_hoje[mask].empty:
                    precos_finais[str(row['ticker']).strip()] = float(df_hoje[mask].iloc[0]['PU Base Manha'])
        except: pass

    # --- PARTE E: ATIVOS DE SALDO FIXO (FGTS, PREVIDÊNCIA, LCA, CDB) ---
    # Para estes, pegamos o preço da última transação registrada para o ticker
    tipos_fixos = ['FGTS', 'PREVIDENCIA', 'LCA', 'CDB']
    df_fixos = df_assets[df_assets['type'].isin(tipos_fixos)]
    for _, row in df_fixos.iterrows():
        t = str(row['ticker']).strip()
        # Busca na aba transactions o último 'price' para este ticker
        match_trans = df_trans[df_trans['ticker'] == t]
        if not match_trans.empty:
            # Pega o preço da última linha (mais recente)
            ultimo_preco_str = str(match_trans.iloc[-1]['price']).replace('.', '').replace(',', '.')
            precos_finais[t] = float(ultimo_preco_str)
        else:
            precos_finais[t] = 1.0

    # Gravação Final
    ws_market = sh.worksheet("market_data")
    ws_market.clear()
    
    final_rows = []
    # Usamos o df_assets como base de ordem para evitar deslocamentos
    for t in df_assets['ticker'].unique():
        t_str = str(t).strip()
        if t_str == "": continue
        preco = precos_finais.get(t_str, 1.0)
        final_rows.append([t_str, float(preco), agora])
    
    ws_market.update(values=[['ticker', 'close_price', 'last_update']] + final_rows, range_name='A1')
    print(f"🚀 Concluído! {len(final_rows)} ativos atualizados.")

if __name__ == "__main__":
    update_all_market_data()
