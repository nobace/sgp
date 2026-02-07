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
        print(f"❌ Erro Autenticação Google: {e}")
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
        print(f"🔎 Brapi: Iniciando consulta de {len(ativos_br)} ativos...")
        
        # Lotes de 10 ativos para balancear performance e segurança
        for i in range(0, len(ativos_br), 10):
            lote = [str(t).strip() for t in ativos_br[i:i+10]]
            tickers_str = ",".join(lote)
            url = f"https://brapi.dev/api/quote/{tickers_str}?token={BRAPI_TOKEN}&dividends=true"
            
            try:
                response = requests.get(url, timeout=30, verify=True)
                if response.status_code != 200:
                    print(f"⚠️ Brapi retornou erro {response.status_code}: {response.text}")
                    continue
                
                res = response.json()
                results = res.get('results', [])
                
                for stock in results:
                    t = stock.get('symbol')
                    # Tenta extrair dividendos
                    divs_data = stock.get('dividendsData', {})
                    divs_list = divs_data.get('cashDividends', []) if divs_data else []
                    
                    if not divs_list:
                        continue # Pula se não tiver dados de proventos
                        
                    item = divs_list[0]
                    # Busca Data Ex
                    d_ex_raw = item.get('lastDateCom') or item.get('date') or item.get('exDate')
                    if not d_ex_raw: continue
                    
                    try:
                        d_ex = datetime.datetime.fromisoformat(d_ex_raw.split('T')[0]).strftime('%d/%m/%Y')
                        d_pg_raw = item.get('paymentDate') or item.get('payDate')
                        
                        if d_pg_raw and d_pg_raw != "0000-00-00":
                            d_pg = datetime.datetime.fromisoformat(d_pg_raw.split('T')[0]).strftime('%d/%m/%Y')
                            status = "Confirmado"
                        else:
                            d_pg = "A confirmar"
                            status = "Anunciado"
                        
                        valor = float(item.get('rate', 0))
                        proventos.append([t, d_ex, d_pg, valor, status, agora_dt.strftime('%d/%m/%Y %H:%M')])
                        tickers_com_sucesso.add(t)
                        print(f"✅ {t}: {status} (R$ {valor})")
                    except Exception as e:
                        print(f"⚠️ Erro ao processar ticker {t}: {e}")
            except Exception as e:
                print(f"❌ Falha na conexão com a Brapi: {e}")

    # 2. YAHOO FALLBACK (BDRs e ativos que a Brapi ignorou)
    restantes = df_assets[~df_assets['ticker'].isin(tickers_com_sucesso)]
    print(f"🔎 Yahoo: Consultando {len(restantes)} ativos remanescentes...")
    
    for _, row in restantes.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue
        
        try:
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            hist = asset.dividends
            if not hist.empty:
                val = float(hist.iloc[-1])
                proventos.append([t, hist.index[-1].strftime('%d/%m/%Y'), "Histórico", val, "Histórico", agora_dt.strftime('%d/%m/%Y %H:%M')])
        except: continue

    # 3. GRAVAÇÃO
    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        # Ordenação por Status
        rank = {"Confirmado": 0, "Anunciado": 1, "Histórico": 2}
        proventos.sort(key=lambda x: rank.get(x[4], 3))
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"--- FIM: {len(proventos)} ativos processados ---")

if __name__ == "__main__":
    update_dividends()
