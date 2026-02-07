import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime

def update_dividends():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except: return

    df_assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    try:
        ws_calendar = sh.worksheet("dividend_calendar")
    except:
        ws_calendar = sh.add_worksheet(title="dividend_calendar", rows="100", cols="5")

    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    tipos_pagadores = ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']
    tickers = df_assets[df_assets['type'].isin(tipos_pagadores)]['ticker'].unique().tolist()

    proventos = []
    for t in tickers:
        try:
            asset = yf.Ticker(t)
            cal = asset.calendar
            if cal is not None and 'Dividend Date' in cal:
                proventos.append([t, cal['Dividend Date'].strftime('%d/%m/%Y'), cal.get('Dividend', 0), "Previsto", agora])
            else:
                info = asset.info
                if info.get('lastDividendValue'):
                    proventos.append([t, "Último Pago", info['lastDividendValue'], "Histórico", agora])
        except: continue

    ws_calendar.clear()
    headers = [['Ticker', 'Data Ref', 'Valor', 'Status', 'Consultado em']]
    ws_calendar.update(values=headers + (proventos if proventos else [['-', '-', '-', '-', agora]]), range_name='A1')
    print("✅ Agenda de dividendos atualizada.")

if __name__ == "__main__":
    update_dividends()
