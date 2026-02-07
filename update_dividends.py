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
    """Web scraping do Fundamentus para Ações e FIIs brasileiros"""
    ticker = str(ticker).strip().upper()
    if is_fii:
        url = f"https://www.fundamentus.com.br/fii_proventos.php?papel={ticker}&tipo=2"
    else:
        url = f"https://www.fundamentus.com.br/proventos.php?papel={ticker}&tipo=2"
    
    # Headers simulando um navegador Chrome real para evitar bloqueio 403
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            # Forçamos o encoding para evitar erros de caracteres especiais
            response.encoding = 'ISO-8859-1' 
            tables = pd.read_html(StringIO(response.text), decimal=',', thousands='.')
            
            # O Fundamentus costuma ter a tabela principal no índice 0
            if tables and len(tables[0]) > 0:
                df = tables[0]
                # Se a tabela tiver a estrutura esperada
                if is_fii:
                    # FII: [Data Com, Tipo, Data Pagto, Valor]
                    d_ex = str(df.iloc[0, 0])
                    d_pg = str(df.iloc[0, 2])
                    val = float(df.iloc[0, 3])
                    print(f"✅ {ticker}: Dados obtidos via Fundamentus (FII)")
                    return d_ex, d_pg, val, "Fundamentus"
                else:
                    # Ação: [Data Com, Valor, Tipo, Data Pagto, Qtd]
                    d_ex = str(df.iloc[0, 0])
                    d_pg = str(df.iloc[0, 3])
                    val = float(df.iloc[0, 1])
                    print(f"✅ {ticker}: Dados obtidos via Fundamentus (Ação)")
                    return d_ex, d_pg, val, "Fundamentus"
    except Exception as e:
        print(f"⚠️ {ticker}: Falha no Fundamentus: {e}")
    return None, None, None, None

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

    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    proventos = []

    for _, row in df_assets.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if not t or tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue

        data_ex, data_pg, valor, fonte = None, None, None, None

        # 1. TENTA FUNDAMENTUS (Ações e FIIs BR)
        if tipo == 'FII' or tipo == 'ETF_BR': # Alguns ETFs BR aparecem na busca de FIIs
            data_ex, data_pg, valor, fonte = get_fundamentus_data(t, is_fii=True)
        elif tipo == 'ACAO_BR':
            data_ex, data_pg, valor, fonte = get_fundamentus_data(t, is_fii=False)

        # 2. TENTA YAHOO (Fallback para BR e Primário para US/BDR)
        if not valor:
            try:
                t_yahoo = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
                asset = yf.Ticker(t_yahoo)
                
                # Para evitar 404 de fundamentus data no Yahoo (quoteSummary)
                hist = asset.dividends
                if not hist.empty:
                    data_ex = hist.index[-1].strftime('%d/%m/%Y')
                    data_pg = "Histórico"
                    valor = float(hist.iloc[-1])
                    fonte = "Yahoo (Hist)"
                    
                    if tipo != 'ETF_US': # Evita 404 em ETFs americanos
                        try:
                            cal = asset.calendar
                            if cal is not None and 'Dividend Date' in cal:
                                data_ex = cal['Dividend Date'].strftime('%d/%m/%Y')
                                data_pg = "Confirmado"
                                valor = cal.get('Dividend', 0)
                                fonte = "Yahoo (Cal)"
                        except: pass
            except: continue

        if valor:
            status = "Confirmado" if ("/" in str(data_pg) or data_pg == "Confirmado") else "Histórico"
            proventos.append([t, data_ex, data_pg, valor, status, agora])

    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        proventos.sort(key=lambda x: x[4], reverse=False)
        ws_calendar.update(values=headers + proventos, range_name='A1')
    else:
        ws_calendar.update(values=headers + [['-', '-', '-', '-', '-', agora]], range_name='A1')
    
    print(f"✅ Processo concluído em {agora}")

if __name__ == "__main__":
    update_dividends()
