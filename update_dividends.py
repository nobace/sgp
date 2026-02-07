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
    
    ws_calendar = sh.worksheet("dividend_calendar")

    agora = datetime.datetime.now()
    proventos = []

    print(f"🔎 Coletando dados via Engine Google/Yahoo...")

    for _, row in df_assets.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue

        try:
            # Formatamos o ticker para o padrão Yahoo (que o Google Finance também reconhece)
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            
            # Pegamos os dividendos históricos
            hist = asset.dividends
            if not hist.empty:
                data_ex_dt = hist.index[-1].replace(tzinfo=None)
                data_ex_str = data_ex_dt.strftime('%d/%m/%Y')
                valor = float(hist.iloc[-1])
                
                # Inteligência de Data: Se for FII e a data_ex for deste mês, 
                # a data de pagamento é geralmente 10-15 dias depois.
                if tipo == 'FII':
                    data_pg = (data_ex_dt + datetime.timedelta(days=10)).strftime('%d/%m/%Y')
                    status = "Confirmado (Est.)" if (agora - data_ex_dt).days < 30 else "Histórico"
                else:
                    data_pg = "Consultar"
                    status = "Histórico"
                
                proventos.append([t, data_ex_str, data_pg, valor, status, agora.strftime('%d/%m/%Y %H:%M:%S')])
        except:
            continue

    # Limpa e atualiza a aba
    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print("✅ Planilha atualizada com sucesso via Google/Yahoo Engine.")

if __name__ == "__main__":
    update_dividends()
