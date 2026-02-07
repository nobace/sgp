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
    if not BRAPI_TOKEN:
        print("❌ ERRO: A variável BRAPI_TOKEN está vazia ou não foi carregada no Workflow.")
    else:
        print(f"✅ TOKEN DETECTADO: {BRAPI_TOKEN[:4]}*** (protegido)")

    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação Google: {e}")
        return

    df_assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    ws_calendar = sh.worksheet("dividend_calendar")

    agora_dt = datetime.datetime.now()
    proventos = []
    processados_brapi = set()

    # 1. CONSULTA BRAPI (Ações, FIIs e ETFs BR)
    ativos_br = df_assets[df_assets['type'].isin(['ACAO_BR', 'FII', 'ETF_BR'])]['ticker'].tolist()
    
    if ativos_br and BRAPI_TOKEN:
        print(f"🔎 Consultando Brapi para {len(ativos_br)} ativos...")
        # Dividindo em lotes de 15 para não estourar a URL da Brapi
        for i in range(0, len(ativos_br), 15):
            lote = ativos_br[i:i+15]
            tickers_str = ",".join([str(t).strip() for t in lote])
            url = f"https://brapi.dev/api/quote/{tickers_str}?token={BRAPI_TOKEN}&fundamental=true&dividends=true"
            
            try:
                res = requests.get(url, timeout=30).json()
                if 'results' not in res:
                    print(f"⚠️ Brapi retornou algo inesperado para o lote {i}: {res}")
                    continue

                for stock in res.get('results', []):
                    t = stock.get('symbol')
                    divs_data = stock.get('dividendsData', {})
                    divs_list = divs_data.get('cashDividends', [])
                    
                    if divs_list:
                        item = divs_list[0] # Mais recente
                        d_ex_raw = item.get('lastDateCom') or item.get('date') or item.get('exDate')
                        if not d_ex_raw: continue
                        
                        try:
                            d_ex = datetime.datetime.fromisoformat(d_ex_raw.split('T')[0]).strftime('%d/%m/%Y')
                            d_pg_raw = item.get('paymentDate')
                            if d_pg_raw and d_pg_raw != "0000-00-00":
                                d_pg = datetime.datetime.fromisoformat(d_pg_raw.split('T')[0]).strftime('%d/%m/%Y')
                                status = "Confirmado"
                            else:
                                d_pg = "A confirmar"
                                status = "Anunciado"
                            
                            valor = float(item.get('rate', 0))
                            proventos.append([t, d_ex, d_pg, valor, status, agora_dt.strftime('%d/%m/%Y %H:%M')])
                            processados_brapi.add(t)
                        except: continue
            except Exception as e:
                print(f"⚠️ Erro no lote {i}: {e}")

    # 2. YAHOO FALLBACK (BDRs, ETFs US e Falhas BR)
    print(f"🔎 Consultando Yahoo para os demais ativos...")
    restantes = df_assets[~df_assets['ticker'].isin(processados_brapi)]
    
    for _, row in restantes.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue
        
        try:
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            hist = asset.dividends
            if not hist.empty:
                u_ex = hist.index[-1]
                val = float(hist.iloc[-1])
                proventos.append([t, u_ex.strftime('%d/%m/%Y'), "Histórico", val, "Histórico", agora_dt.strftime('%d/%m/%Y %H:%M')])
        except: continue

    # Gravação
    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        prioridade = {"Confirmado": 0, "Anunciado": 1, "Histórico": 2}
        proventos.sort(key=lambda x: prioridade.get(x[4], 3))
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"✅ FIM: {len(proventos)} ativos atualizados.")

if __name__ == "__main__":
    update_dividends()
