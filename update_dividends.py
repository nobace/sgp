import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import requests
from io import StringIO

def get_fundamentus_data(ticker, is_fii=True):
    """Web scraping otimizado para Fundamentus"""
    ticker = str(ticker).strip().upper()
    url = f"https://www.fundamentus.com.br/{'fii_' if is_fii else ''}proventos.php?papel={ticker}&tipo=2"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Referer': 'https://www.fundamentus.com.br/'
    }
    
    try:
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=20)
        if response.status_code == 200:
            response.encoding = 'ISO-8859-1'
            # Lemos todas as tabelas e focamos na que tem dados
            tables = pd.read_html(StringIO(response.text), decimal=',', thousands='.')
            for df in tables:
                if len(df) > 0 and 'Valor' in df.columns:
                    # Removemos possíveis linhas de lixo ou strings não conversíveis
                    df = df.dropna(subset=['Valor'])
                    
                    if is_fii:
                        # Ordem FII: [Última Data Com (0), Tipo (1), Data de Pagamento (2), Valor (3)]
                        return str(df.iloc[0, 0]), str(df.iloc[0, 2]), float(df.iloc[0, 3]), "Fundamentus"
                    else:
                        # Ordem Ação: [Data (0), Valor (1), Tipo (2), Data de Pagamento (3), Qtd (4)]
                        return str(df.iloc[0, 0]), str(df.iloc[0, 3]), float(df.iloc[0, 1]), "Fundamentus"
    except: pass
    return None, None, None, None

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

    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    proventos = []

    for _, row in df_assets.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue

        # 1. TENTA FUNDAMENTUS
        d_ex, d_pg, val, src = None, None, None, None
        if tipo == 'FII': d_ex, d_pg, val, src = get_fundamentus_data(t, is_fii=True)
        elif tipo == 'ACAO_BR': d_ex, d_pg, val, src = get_fundamentus_data(t, is_fii=False)

        # 2. YAHOO FALLBACK
        if not val:
            try:
                t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
                asset = yf.Ticker(t_yf)
                hist = asset.dividends
                if not hist.empty:
                    d_ex, d_pg, val, src = hist.index[-1].strftime('%d/%m/%Y'), "Histórico", float(hist.iloc[-1]), "Yahoo"
                    if tipo != 'ETF_US':
                        try:
                            cal = asset.calendar
                            if cal is not None and 'Dividend Date' in cal:
                                d_ex, d_pg, val, src = cal['Dividend Date'].strftime('%d/%m/%Y'), "Confirmado", cal.get('Dividend', 0), "Yahoo"
                        except: pass
            except: continue

        if val:
            status = "Confirmado" if ("/" in str(d_pg) or d_pg == "Confirmado") else "Histórico"
            proventos.append([t, d_ex, d_pg, val, status, agora])

    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    ws_calendar.update(values=headers + proventos, range_name='A1')

if __name__ == "__main__":
    update_dividends()
