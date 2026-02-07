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
        # Autenticação com o Google Sheets
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro de Autenticação: {e}")
        return

    # 1. Carrega os ativos da aba assets
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    # Prepara (ou cria) a aba dividend_calendar
    try:
        ws_calendar = sh.worksheet("dividend_calendar")
    except:
        ws_calendar = sh.add_worksheet(title="dividend_calendar", rows="100", cols="5")

    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    
    # Filtra apenas ativos que distribuem proventos
    tipos_pagadores = ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']
    tickers = df_assets[df_assets['type'].isin(tipos_pagadores)]['ticker'].unique().tolist()

    proventos = []
    print(f"🔎 Analisando {len(tickers)} ativos em busca de proventos...")

    for t in tickers:
        try:
            asset = yf.Ticker(t)
            
            # Estratégia A: Buscar Calendário (Datas Futuras/Confirmadas)
            cal = asset.calendar
            if cal is not None and 'Dividend Date' in cal:
                data_dt = cal['Dividend Date']
                # Verifica se a data é válida e formatável
                if hasattr(data_dt, 'strftime'):
                    proventos.append([
                        t, 
                        data_dt.strftime('%d/%m/%Y'), 
                        cal.get('Dividend', 0), 
                        "Confirmado/Futuro", 
                        agora
                    ])
                    continue # Se achou data futura, pula para o próximo ticker
            
            # Estratégia B: Buscar no Histórico (Último evento ocorrido)
            hist_divs = asset.dividends
            if not hist_divs.empty:
                # Pegamos a data do índice (Timestamp) e o valor da última linha
                ultima_data = hist_divs.index[-1].strftime('%d/%m/%Y')
                ultimo_valor = float(hist_divs.iloc[-1])
                
                proventos.append([
                    t, 
                    ultima_data, 
                    ultimo_valor, 
                    "Histórico (Último)", 
                    agora
                ])
                
        except Exception as e:
            print(f"⚠️ Erro ao consultar {t}: {e}")
            continue

    # 2. Gravação na Planilha
    ws_calendar.clear()
    headers = [['Ticker', 'Data (Pagto/Ex)', 'Valor por Cota', 'Status', 'Consultado em']]
    
    if proventos:
        # Ordenamos a lista para que os "Confirmados" ou datas mais recentes fiquem no topo
        proventos.sort(key=lambda x: x[3], reverse=False) 
        ws_calendar.update(values=headers + proventos, range_name='A1')
        print(f"✅ Calendário atualizado com {len(proventos)} registros.")
    else:
        ws_calendar.update(values=headers + [['-', '-', '-', '-', agora]], range_name='A1')

if __name__ == "__main__":
    update_dividends()
