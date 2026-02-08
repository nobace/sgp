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
# ID da planilha SGP_Database
SHEET_ID = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"

def get_google_sheet_client():
    # Pega o JSON completo da variável de ambiente (exatamente como no seu script antigo)
    creds_json_str = os.environ.get("GOOGLE_SHEETS_CREDS")
    if not creds_json_str:
        raise ValueError("A variável de ambiente GOOGLE_SHEETS_CREDS não foi encontrada.")
    
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
        
        # Tipos que AUMENTAM a posição
        if t in ['COMPRA', 'BONIFICACAO', 'DESDOBRAMENTO', 'BUY', 'ENTRADA']:
            qtd += q
        # Tipos que DIMINUEM a posição
        elif t in ['VENDA', 'AGRUPAMENTO', 'SELL', 'SAIDA']:
            qtd -= q
            
    return max(0.0, qtd)

# --- INTEGRAÇÃO BRAPI (Fonte Primária) ---
def get_dividends_brapi(ticker):
    """Busca dividendos na BRAPI. Retorna Series {Data: Valor} ou None."""
    token = os.environ.get("BRAPI_TOKEN")
    if not token: return None

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
            # BRAPI 'lastDatePrior' é a Data Com
            if 'lastDatePrior' in d:
                dt_str = d['lastDatePrior'].split('T')[0]
                val = d['rate']
                dt_obj = pd.to_datetime(dt_str)
                
                # Somar múltiplos proventos no mesmo dia
                if dt_obj in div_dict:
                    div_dict[dt_obj] += val
                else:
                    div_dict[dt_obj] = val
                    
        return pd.Series(div_dict).sort_index()
        
    except Exception as e:
        print(f"   [BRAPI Error] {symbol}: {e}")
        return None

# --- INTEGRAÇÃO YAHOO (Fonte Secundária/Fallback) ---
def get_dividends_yahoo(ticker):
    symbol = ticker.strip().upper()
    # Adiciona .SA se for ação BR comum
    if len(symbol) >= 4 and not symbol.endswith('.SA') and not symbol.endswith('34'):
        if any(char.isdigit() for char in symbol): 
             symbol += ".SA"
             
    try:
        stock = yf.Ticker(symbol)
        divs = stock.dividends
        if divs.empty: return None
        divs.index = divs.index.tz_localize(None)
        return divs
    except: return None

def main():
    print("--- 🚀 INICIANDO AUDITORIA DE DIVIDENDOS (DESDE 2008) ---")
    
    try:
        gc = get_google_sheet_client()
        sh = gc.open_by_key(SHEET_ID)
        
        # Carregar Transações
        ws_trans = sh.worksheet("transactions")
        dados = ws_trans.get_all_records()
        df = pd.DataFrame(dados)
        
        # Converter datas (Gspread lê strings, forçamos datetime)
        # O script antigo usava dayfirst=True, vamos manter mas com coerção
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        # Limpar espaços nos nomes das colunas
        df.columns = [c.lower().strip() for c in df.columns]
        
        tickers = df['ticker'].unique()
        print(f"✅ Conectado. {len(tickers)} ativos na carteira.")
        
    except Exception as e:
        print(f"❌ Erro Crítico Google Sheets: {e}")
        return

    historico_final = []

    for ticker in tickers:
        if not ticker or str(ticker) in ["UNKNOWN", "nan", "", "None", "USDBRL=X"]: continue
        if "FUNDO" in str(ticker) or "LCA" in str(ticker): continue

        ticker_clean = str(ticker).strip().upper()
        
        # Definir data mínima de busca (O script antigo fazia isso, é uma boa prática)
        min_date = df[df['ticker'] == ticker]['date'].min()
        if pd.isnull(min_date): min_date = datetime(2008, 1, 1)
        
        print(f"🔍 {ticker_clean} (Desde {min_date.year}): ", end="")
        
        # 1. Tentar BRAPI
        divs = get_dividends_brapi(ticker_clean)
        source = "BRAPI"
        
        # 2. Fallback Yahoo
        if divs is None or divs.empty:
            divs = get_dividends_yahoo(ticker_clean)
            source = "YAHOO"
            
        if divs is None or divs.empty:
            print("Sem proventos encontrados.")
            continue
            
        # Filtrar apenas dividendos após a primeira compra (Otimização do script antigo)
        divs = divs[divs.index >= min_date]
        
        print(f"Encontrados via {source}.")
        
        count_asset = 0
        for data_ref, valor in divs.items():
            # BRAPI: Chave é Data Com.
            # Yahoo: Chave é Data Ex. Data Com = Ex - 1.
            
            data_com = data_ref
            if source == "YAHOO":
                data_com = data_ref - timedelta(days=1)
            
            # Calcular Posição na Data Com
            qtd = calcular_posicao_na_data(df, ticker_clean, data_com)
            
            if qtd > 0:
                total_recebido = qtd * valor
                # Estimativa de pagamento
                data_pagto = data_com + timedelta(days=15)
                
                historico_final.append([
                    ticker_clean,
                    data_com.strftime('%Y-%m-%d'), # Data Com/Ex
                    data_pagto.strftime('%Y-%m-%d'),
                    float(f"{valor:.8f}"),
                    float(f"{qtd:.4f}"),
                    float(f"{total_recebido:.2f}"),
                    f"{datetime.now().strftime('%Y-%m-%d')} ({source})"
                ])
                count_asset += 1
        
        time.sleep(0.2) 

    # 3. Salvar
    print(f"💾 Salvando {len(historico_final)} registros na aba 'dividend_history'...")
    try:
        try:
            ws_hist = sh.worksheet("dividend_history")
            ws_hist.clear()
        except:
            ws_hist = sh.add_worksheet(title="dividend_history", rows=2000, cols=10)
        
        ws_hist.append_row(["Ticker", "Data Ref", "Data Pagamento", "Valor Unitario", "Qtd na Epoca", "Total Recebido", "Fonte/Data"])
        if historico_final:
            # Ordenar por data
            historico_final.sort(key=lambda x: x[1])
            ws_hist.append_rows(historico_final)
        print("✅ Sucesso!")
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")

if __name__ == "__main__":
    main()
