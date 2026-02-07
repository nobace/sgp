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
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    try:
        ws_calendar = sh.worksheet("dividend_calendar")
    except:
        ws_calendar = sh.add_worksheet(title="dividend_calendar", rows="100", cols="6")

    agora_dt = datetime.datetime.now()
    proventos = []

    # Tipos que monitoramos para dividendos
    tipos_pagadores = ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']
    df_p = df_assets[df_assets['type'].isin(tipos_pagadores)]

    print(f"🔎 Analisando dividendos para {len(df_p)} ativos...")

    for _, row in df_p.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        
        try:
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            
            # Usamos hist.dividends porque é imune ao erro 404 de quoteSummary do Yahoo
            hist = asset.dividends
            
            if not hist.empty:
                data_ex = hist.index[-1]
                valor = float(hist.iloc[-1])
                diff_dias = (agora_dt - data_ex.replace(tzinfo=None)).days
                
                # Se foi anunciado nos últimos 20 dias, consideramos recente/confirmado
                status = "Confirmado/Recente" if diff_dias <= 20 else "Histórico"
                
                # Estimativa de pagamento (FIIs costumam pagar 10 dias após a Data Ex)
                data_pg = (data_ex + datetime.timedelta(days=10)).strftime('%d/%m/%Y') if tipo == 'FII' else "Consultar"
                
                proventos.append([
                    t, 
                    data_ex.strftime('%d/%m/%Y'), 
                    data_pg, 
                    valor, 
                    status, 
                    agora_dt.strftime('%d/%m/%Y %H:%M')
                ])
        except: continue

    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento (Est)', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        # Ordena pelos confirmados primeiro
        proventos.sort(key=lambda x: x[4], reverse=False)
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"✅ Calendário de dividendos atualizado em {agora_dt.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    update_dividends()
