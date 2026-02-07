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
    
    # PEGA AS VARIÁVEIS DO GITHUB
    BRAPI_TOKEN = os.environ.get('BRAPI_TOKEN')
    SHEETS_CREDS = os.environ.get('GOOGLE_SHEETS_CREDS')
    
    print("--- DIAGNÓSTICO DE AMBIENTE ---")
    if BRAPI_TOKEN:
        print(f"✅ BRAPI_TOKEN encontrada (Inicia com: {BRAPI_TOKEN[:3]}...)")
    else:
        print("❌ ERRO: BRAPI_TOKEN não encontrada nas variáveis de ambiente.")
        
    if SHEETS_CREDS:
        print("✅ GOOGLE_SHEETS_CREDS encontrada.")
    else:
        print("❌ ERRO: GOOGLE_SHEETS_CREDS não encontrada.")

    try:
        info = json.loads(SHEETS_CREDS)
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro na conexão inicial: {e}")
        return

    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    ws_calendar = sh.worksheet("dividend_calendar")

    agora_dt = datetime.datetime.now()
    proventos = []
    tickers_brapi_sucesso = set()

    ativos_br = df_assets[df_assets['type'].isin(['ACAO_BR', 'FII', 'ETF_BR'])]['ticker'].tolist()
    
    # CONSULTA BRAPI
    if ativos_br and BRAPI_TOKEN:
        print(f"🔎 Brapi: Consultando {len(ativos_br)} ativos...")
        for i in range(0, len(ativos_br), 15):
            lote = ativos_br[i:i+15]
            tickers_str = ",".join([str(t).strip() for t in lote])
            url = f"https://brapi.dev/api/quote/{tickers_str}?token={BRAPI_TOKEN}&dividends=true"
            
            try:
                res = requests.get(url, timeout=30).json()
                results = res.get('results', [])
                for stock in results:
                    t = stock.get('symbol')
                    divs_data = stock.get('dividendsData', {})
                    divs_list = divs_data.get('cashDividends', [])
                    if divs_list:
                        item = divs_list[0]
                        d_ex_raw = item.get('lastDateCom') or item.get('date') or item.get('exDate')
                        if d_ex_raw:
                            d_ex = datetime.datetime.fromisoformat(d_ex_raw.split('T')[0]).strftime('%d/%m/%Y')
                            d_pg_raw = item.get('paymentDate') or item.get('payDate')
                            d_pg = datetime.datetime.fromisoformat(d_pg_raw.split('T')[0]).strftime('%d/%m/%Y') if (d_pg_raw and d_pg_raw != "0000-00-00") else "A confirmar"
                            
                            proventos.append([t, d_ex, d_pg, float(item.get('rate', 0)), "Confirmado" if "/" in d_pg else "Anunciado", agora_dt.strftime('%d/%m/%Y %H:%M')])
                            tickers_brapi_sucesso.add(t)
                            print(f"✅ {t}: OK")
            except Exception as e:
                print(f"⚠️ Erro no lote: {e}")

    # YAHOO FALLBACK
    restantes = df_assets[~df_assets['ticker'].isin(tickers_brapi_sucesso)]
    print(f"🔎 Yahoo: Consultando {len(restantes)} remanescentes...")
    for _, row in restantes.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue
        try:
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            hist = asset.dividends
            if not hist.empty:
                proventos.append([t, hist.index[-1].strftime('%d/%m/%Y'), "Histórico", float(hist.iloc[-1]), "Histórico", agora_dt.strftime('%d/%m/%Y %H:%M')])
        except: continue

    ws_calendar.clear()
    if proventos:
        headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
        prioridade = {"Confirmado": 0, "Anunciado": 1, "Histórico": 2}
        proventos.sort(key=lambda x: prioridade.get(x[4], 3))
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"--- FIM DA EXECUÇÃO: {len(proventos)} ativos atualizados ---")

if __name__ == "__main__":
    update_dividends()
