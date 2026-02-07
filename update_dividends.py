import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import requests

def update_dividends():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    # O GitHub Actions injeta o Secret aqui como variável de ambiente
    BRAPI_TOKEN = os.environ.get('BRAPI_TOKEN') 
    
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação Google: {e}")
        return

    # Carrega os ativos
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    ws_calendar = sh.worksheet("dividend_calendar")

    agora_dt = datetime.datetime.now()
    proventos = []

    # Separar ativos brasileiros (Brapi funciona melhor com eles)
    ativos_br = df_assets[df_assets['type'].isin(['ACAO_BR', 'FII', 'ETF_BR'])]['ticker'].tolist()
    
    # --- 1. CONSULTA BRAPI (Ativos BR) ---
    if ativos_br and BRAPI_TOKEN:
        print(f"🔎 Consultando Brapi para {len(ativos_br)} ativos...")
        tickers_str = ",".join([str(t).strip() for t in ativos_br])
        url = f"https://brapi.dev/api/quote/{tickers_str}?token={BRAPI_TOKEN}&fundamental=true&dividends=true"
        
        try:
            res = requests.get(url, timeout=30).json()
            for stock in res.get('results', []):
                ticker_name = stock.get('symbol')
                divs = stock.get('dividendsData', {}).get('cashDividends', [])
                
                if divs:
                    # A Brapi ordena do mais recente para o mais antigo
                    last_div = divs[0]
                    
                    # Formatação de datas (ISO para DD/MM/YYYY)
                    try:
                        d_ex = datetime.datetime.fromisoformat(last_div['lastDateCom'].split('T')[0]).strftime('%d/%m/%Y')
                        d_pg = datetime.datetime.fromisoformat(last_div['paymentDate'].split('T')[0]).strftime('%d/%m/%Y') if last_div.get('paymentDate') else "A confirmar"
                    except:
                        d_ex = "Erro Data"
                        d_pg = "A confirmar"
                        
                    valor = float(last_div.get('rate', 0))
                    status = "Confirmado" if "/" in d_pg else "Anunciado"
                    
                    proventos.append([ticker_name, d_ex, d_pg, valor, status, agora_dt.strftime('%d/%m/%Y %H:%M')])
        except Exception as e:
            print(f"⚠️ Falha na Brapi: {e}")

    # --- 2. CONSULTA YAHOO (BDRs, ETFs US e Fallback) ---
    processados = [p[0] for p in proventos]
    # Filtra o que a Brapi não pegou (ou o que não é BR)
    restantes = df_assets[~df_assets['ticker'].isin(processados)]
    
    for _, row in restantes.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue
        
        try:
            # Sufixo .SA apenas se for brasileiro e não tiver
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            hist = asset.dividends
            
            if not hist.empty:
                u_ex = hist.index[-1]
                val = float(hist.iloc[-1])
                # No Yahoo não temos a data de pagamento fácil, então usamos "Histórico"
                proventos.append([t, u_ex.strftime('%d/%m/%Y'), "Histórico", val, "Histórico", agora_dt.strftime('%d/%m/%Y %H:%M')])
        except: continue

    # Gravação Final
    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        # Ordena para colocar os Confirmados no topo
        proventos.sort(key=lambda x: x[4], reverse=False)
        ws_calendar.update(values=headers + proventos, range_name='A1')
    else:
        ws_calendar.update(values=headers + [['-', '-', '-', '-', '-', agora_dt.strftime('%d/%m/%Y %H:%M')]], range_name='A1')
    
    print(f"✅ Sincronização Brapi/Yahoo finalizada.")

if __name__ == "__main__":
    update_dividends()
