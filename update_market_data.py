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
    """Consulta a API do Tesouro para encontrar o link atualizado do CSV"""
    api_url = "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=taxas-do-tesouro-direto"
    try:
        response = requests.get(api_url, timeout=30)
        data = response.json()
        resources = data['result']['resources']
        for res in resources:
            if "Preco" in res['name'] and "Taxa" in res['name'] and res['format'].lower() == "csv":
                return res['url']
    except Exception as e:
        print(f"⚠️ Erro API Tesouro: {e}")
    return "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv"

def update_all_market_data():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=[
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]

    precos_finais = {}
    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    # --- PARTE A: YAHOO FINANCE ---
    tipos_yahoo = ['ACAO_BR', 'FII', 'BDR', 'ETF_BR', 'ETF_US']
    tickers_yahoo = df_assets[df_assets['type'].isin(tipos_yahoo)]['ticker'].tolist()
    if tickers_yahoo:
        try:
            data_yf = yf.download(tickers_yahoo, period="1d", group_by='ticker', progress=False)
            for t in tickers_yahoo:
                try:
                    val = data_yf[t]['Close'].iloc[-1] if len(tickers_yahoo) > 1 else data_yf['Close'].iloc[-1]
                    if pd.notnull(val): precos_finais[t] = float(val)
