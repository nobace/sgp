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
    
    print("--- INÍCIO DA EXECUÇÃO ---")
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
    ws_calendar = sh.worksheet("dividend_calendar")

    agora_dt = datetime.datetime.now()
    proventos = []
    tickers_com_sucesso = set()

    ativos_br = df_assets[df_assets['type'].isin(['ACAO_BR', 'FII', 'ETF_BR'])]['ticker'].tolist()
    
    if ativos_br and BRAPI_TOKEN:
        print(f"🔎 Brapi: Consultando {len(ativos_br)} ativos em mini-lotes...")
        # Lotes de 5 para garantir que a API processe os dividendos de cada um
        for i in range(0, len(ativos_br), 5):
            lote = ativos_br[i:i+5]
            tickers_str = ",".join([str(t).strip() for t in lote])
            # Adicionamos fundamental=true e dividends=true
            url = f"https://brapi.dev/api/quote/{tickers_str}?token={BRAPI_TOKEN}&fundamental=true&dividends=true"
            
            try:
                res = requests.get(url, timeout=30).json()
                for stock in res.get('results', []):
                    t = stock.get('symbol')
                    # Buscamos em todas as chaves possíveis de dividendos
                    divs_data = stock.get('dividendsData', {})
                    divs_list = divs_data.get('cashDividends', [])
                    
                    if divs_list:
                        # Pegamos o mais recente que tenha data
                        item = divs_list[0]
                        d_ex_raw = item.get('lastDateCom') or item.get('date')
                        
                        if d_ex_raw:
                            d_ex = datetime.datetime.fromisoformat(d_ex_raw.split('T')[0]).strftime('%d/%m/%Y')
                            d_pg_raw = item.get('paymentDate')
                            
                            if d_pg_raw and d_pg_raw != "0000-00-00":
                                d_pg = datetime.datetime.fromisoformat(d_pg_raw.split('T')[0]).strftime('%d/%m/%Y')
                                status = "Confirmado"
                            else:
                                d_pg = "A confirmar"
                                status = "Anunciado"
                            
                            proventos.append([t, d_ex, d_pg, float(item.get('rate', 0)), status, agora_dt.strftime('%d/%m/%Y %H:%M')])
                            tickers_com_sucesso.add(t)
                            print(f"✅ {t}: {status}")
            except: continue

    # 2. YAHOO FALLBACK
    restantes = df_assets[~df_assets['ticker'].isin(tickers_com_sucesso)]
    print(f"🔎 Yahoo: Consultando {len(restantes)} ativos...")
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

    # 3. LIMPEZA E GRAVAÇÃO (Forçando a atualização)
    ws_calendar.clear()
    if proventos:
        headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
        # Ordenação: Confirmado > Anunciado > Histórico
        prioridade = {"Confirmado": 0, "Anunciado": 1, "Histórico": 2}
        proventos.sort(key=lambda x: prioridade.get(x[4], 3))
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"--- FIM: {len(proventos)} ativos processados ---")

if __name__ == "__main__":
    update_dividends()
