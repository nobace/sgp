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
        print("⚠️ AVISO: BRAPI_TOKEN não encontrada. O script usará apenas Yahoo Finance.")
    else:
        print("✅ BRAPI_TOKEN detectada. Consultando Brapi API...")

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

    # 1. CONSULTA BRAPI (Ações, FIIs e ETFs BR)
    ativos_br = df_assets[df_assets['type'].isin(['ACAO_BR', 'FII', 'ETF_BR'])]['ticker'].tolist()
    
    if ativos_br and BRAPI_TOKEN:
        # A Brapi permite múltiplos tickers separados por vírgula
        tickers_str = ",".join([str(t).strip() for t in ativos_br])
        url = f"https://brapi.dev/api/quote/{tickers_str}?token={BRAPI_TOKEN}&fundamental=true&dividends=true"
        
        try:
            res = requests.get(url, timeout=30).json()
            for stock in res.get('results', []):
                t = stock.get('symbol')
                divs_data = stock.get('dividendsData', {})
                if not divs_data: continue
                
                # Brapi separa em cashDividends e stockDividends. Focamos no caixa.
                divs_list = divs_data.get('cashDividends', [])
                if divs_list:
                    # O primeiro item é o mais recente
                    item = divs_list[0]
                    
                    # Tentativa resiliente de obter a Data Ex (lastDateCom)
                    # Campos possíveis segundo a doc: lastDateCom, date, ou exDate
                    d_ex_raw = item.get('lastDateCom') or item.get('date') or item.get('exDate')
                    if not d_ex_raw: continue
                    
                    try:
                        # Limpeza da string de data (YYYY-MM-DD)
                        d_ex = datetime.datetime.fromisoformat(d_ex_raw.split('T')[0]).strftime('%d/%m/%Y')
                        
                        # Data de Pagamento
                        d_pg_raw = item.get('paymentDate')
                        if d_pg_raw and d_pg_raw != "0000-00-00":
                            d_pg = datetime.datetime.fromisoformat(d_pg_raw.split('T')[0]).strftime('%d/%m/%Y')
                            status = "Confirmado"
                        else:
                            d_pg = "A confirmar"
                            status = "Anunciado"
                        
                        valor = float(item.get('rate', 0))
                        
                        proventos.append([t, d_ex, d_pg, valor, status, agora_dt.strftime('%d/%m/%Y %H:%M')])
                        tickers_com_sucesso.add(t)
                    except Exception as e:
                        print(f"⚠️ Erro ao processar dados de {t}: {e}")
                        continue
        except Exception as e:
            print(f"⚠️ Falha na requisição Brapi: {e}")

    # 2. CONSULTA YAHOO (BDRs, ETFs US e Fallback para falhas da Brapi)
    restantes = df_assets[~df_assets['ticker'].isin(tickers_com_sucesso)]
    
    for _, row in restantes.iterrows():
        t = str(row['ticker']).strip()
        tipo = str(row['type']).upper()
        if tipo not in ['ACAO_BR', 'FII', 'BDR', 'ETF_US', 'ETF_BR']: continue
        
        try:
            # Sufixo .SA para ativos B3 (BR e BDRs) no Yahoo
            t_yf = t
            if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA'):
                t_yf = f"{t}.SA"
            
            asset = yf.Ticker(t_yf)
            hist = asset.dividends
            if not hist.empty:
                u_ex = hist.index[-1]
                val = float(hist.iloc[-1])
                # No Yahoo usamos "Histórico" pois ele raramente provê a data de pagamento BR correta
                proventos.append([t, u_ex.strftime('%d/%m/%Y'), "Histórico", val, "Histórico", agora_dt.strftime('%d/%m/%Y %H:%M')])
        except: continue

    # 3. ATUALIZAÇÃO DA PLANILHA
    ws_calendar.clear()
    headers = [['Ticker', 'Data Ex', 'Data Pagamento', 'Valor', 'Status', 'Atualizado em']]
    if proventos:
        # Ordenação por Status (Confirmado > Anunciado > Histórico)
        prioridade = {"Confirmado": 0, "Anunciado": 1, "Histórico": 2}
        proventos.sort(key=lambda x: prioridade.get(x[4], 3))
        ws_calendar.update(values=headers + proventos, range_name='A1')
    
    print(f"✅ Sincronização Finalizada. Total de ativos com proventos: {len(proventos)}")

if __name__ == "__main__":
    update_dividends()
