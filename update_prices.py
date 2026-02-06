import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json

def update_portfolio_prices():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    # Autenticação com a Service Account
    info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ])
    client = gspread.authorize(creds)
    sh = client.open_by_key(ID_PLANILHA)
    
    # 1. Obter Tickers únicos da aba 'transactions'
    ws_trans = sh.worksheet("transactions")
    df_trans = pd.DataFrame(ws_trans.get_all_records())
    
    # Normalizar nomes de colunas para evitar erros
    df_trans.columns = [c.lower().strip() for c in df_trans.columns]
    tickers = [t for t in df_trans['ticker'].unique() if str(t).strip() != ""]
    
    # 2. Buscar Preços no Yahoo Finance
    print(f"Buscando preços para: {tickers}")
    data = yf.download(tickers, period="1d")['Close'].iloc[-1]
    
    # 3. Preparar dados para a aba market_data
    updates = []
    for t in tickers:
        try:
            # Se for um ticker único, o yfinance retorna um float. Se forem vários, uma Série.
            price = data[t] if len(tickers) > 1 else data
            updates.append([t, float(price)])
        except:
            updates.append([t, 0.0])
    
    # 4. Limpar e escrever na aba market_data
    ws_market = sh.worksheet("market_data")
    ws_market.clear()
    ws_market.update('A1', [['ticker', 'close_price']] + updates)
    print("Planilha atualizada com sucesso!")

if __name__ == "__main__":
    update_portfolio_prices()
