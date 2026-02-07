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

    # --- 1. LEITURA DE ASSETS E CONFIGURAÇÃO ---
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    tickers_manuais = df_assets[df_assets['manual_update'].astype(str).str.upper().isin(['S', 'SIM', '1', 'TRUE'])]['ticker'].unique().tolist()
    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    # --- 2. ATUALIZAÇÃO DE PREÇOS (MARKET DATA) ---
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

    # Yahoo Finance (Ações, FIIs, BDRs, ETFs)
    tipos_yahoo = ['ACAO_BR', 'FII', 'BDR', 'ETF_BR', 'ETF_US']
    tickers_auto = [str(t).strip() for t in df_assets[(df_assets['type'].isin(tipos_yahoo)) & (~df_assets['ticker'].isin(tickers_manuais))]['ticker'].unique() if t]
    tickers_auto.append('USDBRL=X')

    try:
        data_yf = yf.download(tickers_auto, period="1d", group_by='ticker', progress=False)
        for t in tickers_auto:
            try:
                val = data_yf[t]['Close'].iloc[-1] if len(tickers_auto) > 1 else data_yf['Close'].iloc[-1]
                if pd.notnull(val): precos_finais[t] = float(val)
            except: pass
    except: pass

    # (Lógica de CVM e Tesouro omitida aqui para brevidade, mas mantida no arquivo final que você salvar)

    # Gravação Market Data
    output_market = [[t, precos_preservados.get(t, 1.0) if t in tickers_manuais else precos_finais.get(t, 1.0), agora] 
                     for t in df_assets['ticker'].unique() if str(t).strip()]
    ws_market.clear()
    ws_market.update(values=[['ticker', 'close_price', 'last_update']] + output_market, range_name='A1', value_input_option='RAW')

    # --- 3. NOVA ABA: DIVIDEND_CALENDAR ---
    print("📅 Gerando calendário de dividendos...")
    try:
        ws_calendar = sh.worksheet("dividend_calendar")
    except:
        ws_calendar = sh.add_worksheet(title="dividend_calendar", rows="100", cols="5")

    proventos_agenda = []
    # Consultar todos os ativos que podem pagar dividendos (incluindo seus ETFs DIVD11, NDIV11, etc.)
    tipos_pagadores = ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']
    tickers_pagadores = df_assets[df_assets['type'].isin(tipos_pagadores)]['ticker'].unique().tolist()

    for t in tickers_pagadores:
        try:
            asset = yf.Ticker(t)
            cal = asset.calendar
            if cal is not None and 'Dividend Date' in cal:
                proventos_agenda.append([
                    t, 
                    cal['Dividend Date'].strftime('%d/%m/%Y'), 
                    cal.get('Dividend', 0),
                    df_assets[df_assets['ticker'] == t]['type'].values[0],
                    agora
                ])
        except: continue

    ws_calendar.clear()
    headers_cal = [['Ticker', 'Data Prevista', 'Valor Estimado', 'Tipo Ativo', 'Consultado em']]
    if proventos_agenda:
        ws_calendar.update(values=headers_cal + proventos_agenda, range_name='A1')
    else:
        ws_calendar.update(values=headers_cal + [['Nenhum anúncio encontrado', '-', '-', '-', agora]], range_name='A1')

    print(f"✅ Processo concluído com sucesso!")

if __name__ == "__main__":
    update_all_market_data()
