import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import time
import requests
from datetime import timedelta, datetime

# --- CONFIGURAÇÃO ---
SHEET_ID = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"

def get_google_sheet_client():
    creds_json_str = os.environ.get("GOOGLE_SHEETS_CREDS")
    if not creds_json_str:
        raise ValueError("A secret GOOGLE_SHEETS_CREDS não foi encontrada.")
    try:
        creds_json = json.loads(creds_json_str)
        creds = Credentials.from_service_account_info(creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        return gspread.authorize(creds)
    except Exception as e:
        raise ValueError(f"Erro ao carregar credenciais JSON: {e}")

def clean_float(val):
    if isinstance(val, (int, float)): return float(val)
    try:
        clean = str(val).replace('"', '').strip()
        if ',' in clean and '.' in clean: 
            clean = clean.replace('.', '').replace(',', '.')
        elif ',' in clean: 
            clean = clean.replace(',', '.')
        return float(clean)
    except: return 0.0

def calcular_posicao_na_data(df_trans, ticker, data_corte):
    # data_corte deve ser timestamp
    mask = (df_trans['ticker'] == ticker) & (df_trans['date'] <= data_corte)
    transacoes = df_trans[mask]
    
    qtd = 0.0
    for _, row in transacoes.iterrows():
        q = clean_float(row['quantity'])
        t = str(row['type']).upper()
        
        if t in ['COMPRA', 'BONIFICACAO', 'DESDOBRAMENTO', 'BUY', 'ENTRADA']:
            qtd += q
        elif t in ['VENDA', 'AGRUPAMENTO', 'SELL', 'SAIDA']:
            qtd -= q
            
    return max(0.0, qtd)

# --- BRAPI (Melhor para dados históricos BR, inclusive alguns deslistados) ---
def get_dividends_brapi(ticker):
    token = os.environ.get("BRAPI_TOKEN")
    if not token: return None

    # BRAPI não usa .SA
    symbol = ticker.replace(".SA", "").strip().upper()
    
    url = f"https://brapi.dev/api/quote/{symbol}"
    params = {
        'range': 'max',
        'interval': '1d',
        'fundamental': 'false', 
        'dividends': 'true',
        'token': token
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200: return None
        
        data = resp.json()
        if 'results' not in data or not data['results']: return None
        
        result = data['results'][0]
        if 'dividendsData' not in result: return None
        
        divs = result['dividendsData']['cashDividends']
        
        div_dict = {}
        for d in divs:
            if 'lastDatePrior' in d:
                dt_str = d['lastDatePrior'].split('T')[0]
                val = d['rate']
                dt_obj = pd.to_datetime(dt_str)
                
                if dt_obj in div_dict:
                    div_dict[dt_obj] += val
                else:
                    div_dict[dt_obj] = val
                    
        return pd.Series(div_dict).sort_index()
    except:
        return None

# --- YAHOO (Fallback para internacionais ou falha BRAPI) ---
def get_dividends_yahoo(ticker):
    # Yahoo precisa de .SA para BR
    symbol = ticker.strip().upper()
    if len(symbol) >= 4 and not symbol.endswith('.SA') and not symbol.endswith('34'):
         # Regra simples: tem número e não é BDR (34), adiciona .SA
         if any(char.isdigit() for char in symbol): 
             symbol += ".SA"
             
    try:
        # Tenta silenciar warnings de deslistagem capturando stderr se necessário,
        # mas yfinance imprime direto. O try/except segura o crash.
        stock = yf.Ticker(symbol)
        divs = stock.dividends
        if divs.empty: return None
        divs.index = divs.index.tz_localize(None)
        return divs
    except: return None

def main():
    print("--- 🚀 INICIANDO AUDITORIA DE DIVIDENDOS (FIX DATAS + HÍBRIDO) ---")
    
    try:
        gc = get_google_sheet_client()
        sh = gc.open_by_key(SHEET_ID)
        
        ws_trans = sh.worksheet("transactions")
        dados = ws_trans.get_all_records()
        df = pd.DataFrame(dados)
        
        # --- CORREÇÃO DE DATA ---
        # dayfirst=False pois o formato do CSV é YYYY-MM-DD
        df['date'] = pd.to_datetime(df['date'], dayfirst=False, errors='coerce')
        
        # Limpar colunas e tickers
        df.columns = [c.lower().strip() for c in df.columns]
        tickers = df['ticker'].unique()
        print(f"✅ Conectado. {len(tickers)} ativos na carteira.")
        
    except Exception as e:
        print(f"❌ Erro Google Sheets: {e}")
        return

    historico_final = []

    for ticker in tickers:
        # Filtros de lixo
        if not ticker or str(ticker) in ["UNKNOWN", "nan", "", "None", "USDBRL=X"]: continue
        if "FUNDO" in str(ticker) or "LCA" in str(ticker) or "CDB" in str(ticker): continue

        ticker_clean = str(ticker).strip().upper().replace(".SA", "")
        
        # Data mínima para otimizar busca
        min_date = df[df['ticker'] == ticker]['date'].min()
        if pd.isnull(min_date): min_date = datetime(2008, 1, 1)
        
        print(f"🔍 {ticker_clean}: ", end="")
        
        # 1. Tentar BRAPI (Prioridade)
        divs = get_dividends_brapi(ticker_clean)
        source = "BRAPI"
        
        # 2. Se vazio, Tentar Yahoo (Fallback)
        if divs is None or divs.empty:
            divs = get_dividends_yahoo(ticker_clean)
            source = "YAHOO"
            
        if divs is None or divs.empty:
            print(f"Sem proventos (Ativo pode estar deslistado ou ser recente).")
            continue
            
        # Filtra datas irrelevantes
        divs = divs[divs.index >= min_date]
        
        if divs.empty:
            print(f"Sem proventos após {min_date.year}.")
            continue

        print(f"Encontrados via {source}.")
        
        for data_ref, valor in divs.items():
            # Unificar lógica de Data Com
            data_com = data_ref
            if source == "YAHOO":
                # Yahoo usa Data EX no índice. Com = Ex - 1 dia útil (aprox)
                data_com = data_ref - timedelta(days=1)
            
            qtd = calcular_posicao_na_data(df, ticker, data_com)
            
            if qtd > 0:
                total = qtd * valor
                pagto = data_com + timedelta(days=15) # Estimativa
                
                historico_final.append([
                    ticker_clean,
                    data_com.strftime('%Y-%m-%d'),
                    pagto.strftime('%Y-%m-%d'),
                    float(f"{valor:.8f}"),
                    float(f"{qtd:.4f}"),
                    float(f"{total:.2f}"),
                    f"{datetime.now().strftime('%Y-%m-%d')} ({source})"
                ])
        
        time.sleep(0.1) 

    # 3. Salvar
    print(f"💾 Salvando {len(historico_final)} registros...")
    try:
        try:
            ws_hist = sh.worksheet("dividend_history")
            ws_hist.clear()
        except:
            ws_hist = sh.add_worksheet(title="dividend_history", rows=2000, cols=10)
        
        ws_hist.append_row(["Ticker", "Data Ref", "Data Pagamento", "Valor Unitario", "Qtd na Epoca", "Total Recebido", "Fonte/Data"])
        
        if historico_final:
            historico_final.sort(key=lambda x: x[1]) # Ordenar por data
            ws_hist.append_rows(historico_final)
            
        print("✅ Sucesso!")
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")

if __name__ == "__main__":
    main()
