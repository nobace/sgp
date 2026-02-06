import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import numpy as np

def update_portfolio_prices():
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
        print(f"Erro na autenticação: {e}")
        return

    # 2. Obter Tickers
    ws_trans = sh.worksheet("transactions")
    df_trans = pd.DataFrame(ws_trans.get_all_records())
    df_trans.columns = [c.lower().strip() for c in df_trans.columns]
    
    all_tickers = [str(t).strip() for t in df_trans['ticker'].unique() if str(t).strip() != ""]
    
    # Filtra apenas o que o Yahoo provavelmente conhece
    tickers_to_fetch = [t for t in all_tickers if ".SA" in t or len(t) <= 6]
    
    # 3. Buscar Preços
    print(f"Buscando preços para {len(tickers_to_fetch)} ativos...")
    price_dict = {}
    
    if tickers_to_fetch:
        try:
            # Baixando dados
            data = yf.download(tickers_to_fetch, period="1d", group_by='ticker', progress=False)
            
            for t in tickers_to_fetch:
                try:
                    if len(tickers_to_fetch) > 1:
                        val = data[t]['Close'].iloc[-1]
                    else:
                        val = data['Close'].iloc[-1]
                    
                    # AQUI ESTÁ A CORREÇÃO: Se for NaN ou Infinito, vira 0.0
                    if pd.isna(val) or np.isinf(val):
                        price_dict[t] = 0.0
                    else:
                        price_dict[t] = float(val)
                except:
                    price_dict[t] = 0.0
        except Exception as e:
            print(f"Erro no download: {e}")

    # 4. Preparar lista Final (Limpando qualquer valor inválido para JSON)
    updates = []
    for t in all_tickers:
        price = price_dict.get(t, 0.0)
        
        # Se for renda fixa que o Yahoo não achou, colocamos 1.0 para manter o saldo
        if price == 0.0:
            if any(x in t.upper() for x in ["LCA", "FGTS", "PREV", "TD_"]):
                price = 1.0
        
        updates.append([t, price])
    
    # 5. Escrever na market_data
    try:
        ws_market = sh.worksheet("market_data")
        ws_market.clear()
        
        # Cabeçalho + Dados
        final_values = [['ticker', 'close_price']] + updates
        
        # Usando a sintaxe que não gera erro de JSON
        ws_market.update(values=final_values, range_name='A1')
        print(f"Sucesso! {len(updates)} tickers atualizados.")
    except Exception as e:
        print(f"Erro ao gravar: {e}")

if __name__ == "__main__":
    update_portfolio_prices()
