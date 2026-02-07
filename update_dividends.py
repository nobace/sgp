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
    """Web scraping do Fundamentus para Ações e FIIs"""
    if is_fii:
        url = f"https://www.fundamentus.com.br/fii_proventos.php?papel={ticker}&tipo=2"
    else:
        url = f"https://www.fundamentus.com.br/proventos.php?papel={ticker}&tipo=2"
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            tables = pd.read_html(StringIO(response.text), decimal=',', thousands='.')
            if tables and len(tables[0]) > 0:
                df = tables[0]
                # FII: [Data Com, Tipo, Data Pagto, Valor]
                # Ação: [Data Com, Valor, Tipo, Data Pagto, Qtd]
                if is_fii:
                    return str(df.iloc[0, 0]), str(df.iloc[0, 2]), float(df.iloc[0, 3]), "Fundamentus"
                else:
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
    
    try:
        ws_calendar = sh.worksheet("dividend_calendar")
    except:
        ws_calendar = sh.add_worksheet(title="dividend_calendar", rows="100", cols="6")

    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    proventos = []

    for _, row in df_assets.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if not t or tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue

        data_ex, data_pg, valor, fonte = None, None, None, None

        # 1. TENTA FUNDAMENTUS (Apenas para Ações e FIIs)
        if tipo == 'FII':
            data_ex, data_pg, valor, fonte = get_fundamentus_data(t, is_fii=True)
        elif tipo == 'ACAO_BR':
            data_ex, data_pg, valor, fonte = get_fundamentus_data(t, is_fii=False)

        # 2. TENTA YAHOO FINANCE (Para ETFs, BDRs ou se o Fundamentus falhar)
        if not valor:
            try:
                # Adiciona .SA para ativos brasileiros no Yahoo
                t_yahoo = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
                asset = yf.Ticker(t_yahoo)
                
                # Prioridade: Calendário (Datas Futuras)
                cal = asset.calendar
                if cal is not None and 'Dividend Date' in cal:
                    data_ex = cal['Dividend Date'].strftime('%d/%m/%Y')
                    data_pg = "Confirmado"
                    valor = cal.get('Dividend', 0)
                    fonte = "Yahoo (Cal)"
                # Backup: Histórico (Último pago)
                else:
                    hist = asset.dividends
                    if not hist.empty:
                        data_ex = hist.index[-1].strftime('%d/%m/%Y')
                        data_pg = "Histórico"
                        valor = float(hist.iloc[-1])
                        fonte = "Yahoo (Hist)"
            except: continue

        if valor:
            status = "Confirmado" if ("/" in str(data_pg) or data_pg == "Confirmado") else "Histórico"
            proventos.append([t, data_ex, data_pg, valor, status, agora])

    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"✅ Calendário atualizado (Fundamentus para Ações/FIIs + Yahoo para o restante).")

if __name__ == "__main__":
    update_dividends()
