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
    BRAPI_TOKEN = os.environ.get('BRAPI_TOKEN')
    
    if not BRAPI_TOKEN:
        print("⚠️ AVISO: BRAPI_TOKEN não encontrada.")
    else:
        print("✅ BRAPI_TOKEN detectada. Iniciando consulta profissional.")

    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação Google: {e}")
        return

    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    ws_calendar = sh.worksheet("dividend_calendar")

    agora_dt = datetime.datetime.now()
    proventos = []
    tickers_com_sucesso = set()

    # 1. CONSULTA BRAPI (Ativos BR)
    ativos_br = df_assets[df_assets['type'].isin(['ACAO_BR', 'FII', 'ETF_BR'])]['ticker'].tolist()
    
    if ativos_br and BRAPI_TOKEN:
        tickers_str = ",".join([str(t).strip() for t in ativos_br])
        url = f"https://brapi.dev/api/quote/{tickers_str}?token={BRAPI_TOKEN}&fundamental=true&dividends=true"
        
        try:
            res = requests.get(url, timeout=30).json()
            for stock in res.get('results', []):
                t = stock.get('symbol')
                divs_data = stock.get('dividendsData', {})
                if not divs_data: continue
                
                divs = divs_data.get('cashDividends', [])
                if divs:
                    last_div = divs[0]
                    
                    # Tentativa resiliente de pegar a Data Ex (lastDateCom ou date)
                    d_ex_raw = last_div.get('lastDateCom') or last_div.get('date')
                    if not d_ex_raw: continue
                    
                    try:
                        d_ex = datetime.datetime.fromisoformat(d_ex_raw.split('T')[0]).strftime('%d/%m/%Y')
                        
                        d_pg = "A confirmar"
                        if last_div.get('paymentDate'):
                            d_pg_raw = last_div['paymentDate'].split('T')[0]
                            d_pg = datetime.datetime.strptime(d_pg_raw, '%Y-%m-%d').strftime('%d/%m/%Y')
                        
                        valor = float(last_div.get('rate', 0))
                        status = "Confirmado" if "/" in d_pg else "Anunciado"
                        
                        proventos.append([t, d_ex, d_pg, valor, status, agora_dt.strftime('%d/%m/%Y %H:%M')])
                        tickers_com_sucesso.add(t)
                    except: continue
        except Exception as e:
            print(f"⚠️ Erro no processamento da Brapi: {e}")

    # 2. CONSULTA YAHOO (BDRs, ETFs US e Fallback para os que falharam na Brapi)
    restantes = df_assets[~df_assets['ticker'].isin(tickers_com_sucesso)]
    
    for _, row in restantes.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue
        
        try:
            # Garante sufixo .SA para ativos negociados na B3 (BR e BDR)
            t_yf = t
            if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA'):
                t_yf = f"{t}.SA"
            
            asset = yf.Ticker(t_yf)
            # Uso do .dividends para evitar 404 de quoteSummary
            hist = asset.dividends
            if not hist.empty:
                u_ex = hist.index[-1]
                val = float(hist.iloc[-1])
                proventos.append([t, u_ex.strftime('%d/%m/%Y'), "Histórico", val, "Histórico", agora_dt.strftime('%d/%m/%Y %H:%M')])
        except: continue

    # Gravação Final
    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        # Ordenação: Confirmado < Anunciado < Histórico
        order = {"Confirmado": 0, "Anunciado": 1, "Histórico": 2}
        proventos.sort(key=lambda x: order.get(x[4], 3))
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"✅ Sincronização Finalizada. Total de ativos: {len(proventos)}")

if __name__ == "__main__":
    update_dividends()
