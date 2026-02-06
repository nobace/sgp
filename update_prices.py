import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def update_portfolio_prices():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    # Autenticação
    info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ])
    client = gspread.authorize(creds)
    sh = client.open_by_key(ID_PLANILHA)
    
    # 1. Obter Tickers da aba 'transactions'
    ws_trans = sh.worksheet("transactions")
    df_trans = pd.DataFrame(ws_trans.get_all_records())
    df_trans.columns = [c.lower().strip() for c in df_trans.columns]
    
    # Filtro: Só buscar no Yahoo o que parece ser Ação, FII, BDR ou ETF (contém .SA ou Ticker curto)
    # Ignora LCAs, FGTS, Previdência e Tesouro Direto
    all_tickers = [t for t in df_trans['ticker'].unique() if str(t).strip() != ""]
    tickers_to_fetch = [t for t in all_tickers if ".SA" in str(t) or len(str(t)) <= 6]
    
    # 2. Buscar Preços
    print(f"Buscando preços para: {tickers_to_fetch}")
    try:
        data = yf.download(tickers_to_fetch, period="1d")['Close'].iloc[-1]
    except Exception as e:
        print(f"Erro ao baixar dados: {e}")
        return

    # 3. Preparar dados para market_data
    updates = []
    for t in all_tickers:
        val = 0.0
        if t in tickers_to_fetch:
            try:
                # Se o yfinance retornar NaN ou falhar, usamos 0.0
                raw_val = data[t] if len(tickers_to_fetch) > 1 else data
                val = float(raw_val) if not np.isnan(raw_val) else 0.0
            except:
                val = 0.0
        
        # Se for um ativo de renda fixa (LCA, FGTS), mantemos 1.0 ou 0.0 
        # para não zerar o cálculo se o usuário não preencher manualmente
        if val == 0.0 and ("LCA" in str(t) or "FGTS" in str(t) or "TD_" in str(t)):
            val = 0.01 # Valor simbólico para ativos manuais
            
        updates.append([t, val])
    
    # 4. Escrever na market_data (Usando nova sintaxe do gspread)
    ws_market = sh.worksheet("market_data")
    ws_market.clear()
    # A sintaxe correta para evitar o DeprecationWarning:
    ws_market.update(values=[['ticker', 'close_price']] + updates, range_name='A1')
    print("Planilha atualizada com sucesso!")

if __name__ == "__main__":
    update_portfolio_prices()
