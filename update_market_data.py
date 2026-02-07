import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import requests
import io

def get_tesouro_url():
    api_url = "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=taxas-do-tesouro-direto"
    try:
        response = requests.get(api_url, timeout=30)
        data = response.json()
        resources = data['result']['resources']
        for res in resources:
            if "PrecoTaxa" in res['name'] or ("Preco" in res['name'] and res['format'].lower() == "csv"):
                return res['url']
    except: pass
    return "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv"

def update_prices():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    tickers_manuais = df_assets[df_assets['manual_update'].astype(str).str.upper().isin(['S', 'SIM', '1', 'TRUE'])]['ticker'].unique().tolist()
    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    ws_market = sh.worksheet("market_data")
    dados_market_atuais = ws_market.get_all_values()
    
    def clean_val(val):
        if val is None or val == "" or val == "close_price": return 1.0
        s = str(val).strip()
        if "," in s: s = s.replace(".", "").replace(",", ".")
        try: return float(s)
        except: return 1.0

    precos_preservados = {str(row[0]).strip(): clean_val(row[1]) for row in dados_market_atuais[1:]} if len(dados_market_atuais) > 1 else {}
    precos_finais = {}

    # --- MAPEAMENTO PARA YAHOO FINANCE ---
    tipos_br = ['ACAO_BR', 'FII', 'ETF_BR']
    tipos_us_bdr = ['BDR', 'ETF_US']
    mapa_tickers = {} # Yahoo_Ticker -> Planilha_Ticker

    for _, row in df_assets.iterrows():
        t_orig = str(row['ticker']).strip()
        if not t_orig or t_orig in tickers_manuais: continue
        
        if row['type'] in tipos_br:
            t_yahoo = f"{t_orig}.SA" if not t_orig.endswith('.SA') else t_orig
            mapa_tickers[t_yahoo] = t_orig
        elif row['type'] in tipos_us_bdr:
            mapa_tickers[t_orig] = t_orig

    tickers_yahoo = list(mapa_tickers.keys())
    tickers_yahoo.append('USDBRL=X')
    mapa_tickers['USDBRL=X'] = 'USDBRL=X'

    if tickers_yahoo:
        try:
            data_yf = yf.download(tickers_yahoo, period="1d", group_by='ticker', progress=False)
            for ty in tickers_yahoo:
                try:
                    tp = mapa_tickers[ty]
                    val = data_yf[ty]['Close'].iloc[-1] if len(tickers_yahoo) > 1 else data_yf['Close'].iloc[-1]
                    if pd.notnull(val): precos_finais[tp] = float(val)
                except: pass
        except Exception as e: print(f"⚠️ Erro Yahoo: {e}")

    # --- CVM (FUNDOS) ---
    df_fundos = df_assets[(df_assets['type'] == 'FUNDO') & (~df_assets['ticker'].isin(tickers_manuais))]
    mapa_cnpjs = {str(r['isin_cnpj']).replace('.','').replace('-','').replace('/','').zfill(14): str(r['ticker']).strip() for _, r in df_fundos.iterrows() if r.get('isin_cnpj')}
    if mapa_cnpjs:
        for i in range(2):
            mes = (datetime.date.today() - datetime.timedelta(days=i*28)).strftime('%Y%m')
            url = f"
