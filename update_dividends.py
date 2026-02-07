import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import requests

def update_dividends():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    BRAPI_TOKEN = os.environ.get('BRAPI_TOKEN') # Você deve adicionar isso nos Secrets do GitHub
    
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação Google: {e}")
        return

    df_assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    ws_calendar = sh.worksheet("dividend_calendar")

    agora_dt = datetime.datetime.now()
    proventos = []

    # Separar ativos BR para consulta em lote na Brapi (mais rápido)
    ativos_br = df_assets[df_assets['type'].isin(['ACAO_BR', 'FII', 'ETF_BR'])]['ticker'].tolist()
    
    # --- CONSULTA BRAPI (Ativos BR) ---
    if ativos_br and BRAPI_TOKEN:
        print(f"🔎 Consultando Brapi para {len(ativos_br)} ativos brasileiros...")
        tickers_str = ",".join(ativos_br)
        url = f"https://brapi.dev/api/quote/{tickers_str}?token={BRAPI_TOKEN}&fundamental=true&dividends=true"
        
        try:
            res = requests.get(url, timeout=20).json()
            for stock in res.get('results', []):
                t = stock.get('symbol')
                divs = stock.get('dividendsData', {}).get('cashDividends', [])
                if divs:
                    # Pegamos o mais recente
                    last_div = divs[0]
                    d_ex = datetime.datetime.fromisoformat(last_div['lastDateCom'].replace('Z', '')).strftime('%d/%m/%Y')
                    d_pg = datetime.datetime.fromisoformat(last_div['paymentDate'].replace('Z', '')).strftime('%d/%m/%Y') if last_div.get('paymentDate') else "A confirmar"
                    valor = float(last_div['rate'])
                    
                    # Status baseado na data de pagamento
                    status = "Confirmado" if d_pg != "A confirmar" else "Anunciado"
                    proventos.append([t, d_ex, d_pg, valor, status, agora_dt.strftime('%d/%m/%Y %H:%M')])
        except Exception as e:
            print(f"⚠️ Erro Brapi: {e}")

    # --- CONSULTA YAHOO (Ativos US / Fallback) ---
    tickers_processados = [p[0] for p in proventos]
    ativos_restantes = df_assets[~df_assets['ticker'].isin(tickers_processados)]
    
    for _, row in ativos_restantes.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['BDR', 'ETF_US']: continue
        
        try:
            asset = yf.Ticker(t)
            hist = asset.dividends
            if not hist.empty:
                u_ex = hist.index[-1]
                val = float(hist.iloc[-1])
                proventos.append([t, u_ex.strftime('%d/%m/%Y'), "Consultar", val, "Histórico", agora_dt.strftime('%d/%m/%Y %H:%M')])
        except: continue

    # Gravação
    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"✅ Processo Brapi/Yahoo concluído.")

if __name__ == "__main__":
    update_dividends()
