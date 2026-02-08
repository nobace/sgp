import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import requests
import io
import time

# --- MAPEAMENTO DE MUDANÇAS DE TICKER ---
RENAME_MAP = {
    "CESP6": "AURE3",
    "BTOW3": "AMER3",
    "ENAT3": "BRAV3",
    "LCAM3": "RENT3",
    "BKBR3": "ZAMP3",
    "IGTA3": "IGTI11",
    "BRDT3": "VBBR3",
    "JPSA3": "ALOS3",
    "SULA11": "RDOR3",
    "TRPL4": "ISAE4"
}

def get_tesouro_url():
    api_url = "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=taxas-do-tesouro-direto"
    try:
        response = requests.get(api_url, timeout=30)
        data = response.json()
        resources = data['result']['resources']
        for res in resources:
            if "PrecoTaxa" in res['name'] or ("Preco" in res['name'] and res['format'].lower() == "csv"):
                return res['url']
    except: pass
    return "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv"

# --- BRAPI (COTAÇÃO) ---
def get_prices_brapi(tickers):
    """
    Busca preços em lote na BRAPI.
    Retorna dicionário {TICKER: PRECO}
    """
    token = os.environ.get("BRAPI_TOKEN")
    if not token or not tickers: return {}

    chunk_size = 20
    precos_encontrados = {}
    
    # Limpar .SA para BRAPI
    tickers_clean = [t.replace(".SA", "").strip().upper() for t in tickers]
    
    # Processar em lotes
    for i in range(0, len(tickers_clean), chunk_size):
        batch = tickers_clean[i:i+chunk_size]
        try:
            url = f"https://brapi.dev/api/quote/{','.join(batch)}"
            params = {'token': token, 'fundamental': 'false'}
            
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if 'results' in data:
                    for item in data['results']:
                        sym = item['symbol'].upper()
                        price = item.get('regularMarketPrice')
                        if price:
                            precos_encontrados[sym] = float(price)
            time.sleep(0.5) # Respeitar rate limit
        except Exception as e:
            print(f"   ⚠️ Erro BRAPI batch: {e}")
            
    return precos_encontrados

def update_prices():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    try:
        creds_str = os.environ.get("GOOGLE_SHEETS_CREDS")
        if not creds_str: raise Exception("Secret GOOGLE_SHEETS_CREDS não encontrada")
        
        info = json.loads(creds_str)
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    # 1. Carrega Assets
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    def clean_val(val):
        if val is None or val == "" or val == "close_price": return 0.0
        s = str(val).strip()
        if "," in s: s = s.replace(".", "").replace(",", ".")
        try: return float(s)
        except: return 0.0

    # Backup Google Finance
    precos_google_backup = {str(r['ticker']).strip(): clean_val(r.get('price_google', 0)) for _, r in df_assets.iterrows()}
    
    tickers_manuais = df_assets[df_assets['manual_update'].astype(str).str.upper().isin(['S', 'SIM', '1', 'TRUE'])]['ticker'].unique().tolist()
    
    # --- CORREÇÃO DO FUSO HORÁRIO (GMT -3) ---
    fuso_gmt3 = datetime.timezone(datetime.timedelta(hours=-3))
    agora = datetime.datetime.now(fuso_gmt3).strftime('%d/%m/%Y %H:%M:%S')
    # ----------------------------------------

    ws_market = sh.worksheet("market_data")
    dados_market_atuais = ws_market.get_all_values()
    # Preserva valores anteriores caso a atualização falhe
    precos_preservados = {str(row[0]).strip(): clean_val(row[1]) for row in dados_market_atuais[1:]} if len(dados_market_atuais) > 1 else {}
    
    precos_finais = {}
    
    # Separação de Ativos
    lista_brapi = []
    lista_yahoo_only = []
    
    tipos_br = ['ACAO_BR', 'FII', 'ETF_BR', 'BDR']

    for _, row in df_assets.iterrows():
        t_orig = str(row['ticker']).strip().upper()
        if not t_orig or t_orig in tickers_manuais: continue
        
        # Verifica se ticker mudou
        if t_orig in RENAME_MAP:
            print(f"   ℹ️ Redirecionando {t_orig} -> {RENAME_MAP[t_orig]}")
            t_orig = RENAME_MAP[t_orig]
        
        if row['type'] in tipos_br:
            lista_brapi.append(t_orig)
        elif row['type'] == 'ETF_US':
            lista_yahoo_only.append(t_orig)
            
    # Remover duplicatas
    lista_brapi = list(set(lista_brapi))
    lista_yahoo_only = list(set(lista_yahoo_only))

    # --- 1. BRAPI (Prioridade B3) ---
    print(f"--- 🔍 BRAPI ({len(lista_brapi)} ativos) ---")
    dict_brapi = get_prices_brapi(lista_brapi)
    
    for t in lista_brapi:
        price = dict_brapi.get(t) or dict_brapi.get(f"{t}.SA")
        if price:
            precos_finais[t] = price
        else:
            lista_yahoo_only.append(f"{t}.SA" if not t.endswith(".SA") else t)

    # --- 2. YAHOO FINANCE (Fallback + Internacional) ---
    print(f"--- 🔍 Yahoo Finance ({len(lista_yahoo_only)} ativos) ---")
    if 'USDBRL=X' not in lista_yahoo_only: lista_yahoo_only.append('USDBRL=X')
    
    for t in lista_yahoo_only:
        t_clean = t.replace(".SA", "")
        if t_clean in precos_finais: continue

        try:
            ticker_obj = yf.Ticker(t)
            hist = ticker_obj.history(period="1d")
            if not hist.empty:
                val = float(hist['Close'].iloc[-1])
                precos_finais[t_clean] = val
            else:
                if t == 'USDBRL=X':
                     info = ticker_obj.fast_info
                     if hasattr(info, 'last_price'):
                         precos_finais['USDBRL=X'] = float(info.last_price)
        except: pass
            
    # --- 3. REDUNDÂNCIA (Google Finance) ---
    for t in df_assets['ticker'].unique():
        ts = str(t).strip()
        ts_lookup = RENAME_MAP.get(ts, ts)
        
        if ts_lookup not in precos_finais and ts not in tickers_manuais and ts != 'USDBRL=X':
            val_backup = precos_google_backup.get(ts, 0)
            if val_backup > 0:
                precos_finais[ts] = val_backup
                print(f"⚠️ {ts}: Usando backup do Google Finance")

    # --- 4. CVM (FUNDOS) ---
    print("--- 🔍 CVM (Fundos) ---")
    df_fundos = df_assets[(df_assets['type'] == 'FUNDO') & (~df_assets['ticker'].isin(tickers_manuais))]
    mapa_cnpjs = {str(r['isin_cnpj']).replace('.','').replace('-','').replace('/','').zfill(14): str(r['ticker']).strip() for _, r in df_fundos.iterrows() if r.get('isin_cnpj')}
    
    fundos_encontrados = 0
    if mapa_cnpjs:
        for i in range(2): # Tenta mês atual e anterior
            mes = (datetime.date.today() - datetime.timedelta(days=i*28)).strftime('%Y%m')
            url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"
            print(f"   Baixando dados CVM: {mes}...")
            try:
                resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=90)
                if resp.status_code == 200:
                    if len(resp.content) < 1000:
                        print(f"   Arquivo CVM {mes} muito pequeno. Ignorando.")
                        continue
                        
                    # Lê com low_memory=False para garantir precisão e tipos corretos
                    with io.BytesIO(resp.content) as zip_buffer:
                         df_cvm = pd.read_csv(zip_buffer, sep=';', compression='zip', encoding='latin1', low_memory=False)
                    
                    # Correção para nome dinâmico da coluna
                    col_cnpj = 'CNPJ_FUNDO'
                    if 'CNPJ_FUNDO_CLASSE' in df_cvm.columns:
                        col_cnpj = 'CNPJ_FUNDO_CLASSE'
                    elif 'CNPJ_FUNDO' not in df_cvm.columns:
                         print(f"   Erro: Coluna de CNPJ não encontrada em {mes}.")
                         continue

                    df_cvm['cnpj_key'] = df_cvm[col_cnpj].str.replace(r'\D', '', regex=True).str.zfill(14)
                    df_cvm = df_cvm.drop_duplicates('cnpj_key', keep='last')
                    
                    cvm_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()
                    
                    for cnpj, ticker in mapa_cnpjs.items():
                        if cnpj in cvm_dict and ticker not in precos_finais: 
                            val_cota = float(cvm_dict[cnpj])
                            precos_finais[ticker] = val_cota
                            fundos_encontrados += 1
                    
                    if fundos_encontrados >= len(mapa_cnpjs) * 0.8:
                        break 
            except Exception as e: 
                print(f"   Erro CVM {mes}: {e}")
                continue
    print(f"   Fundos atualizados via CVM: {fundos_encontrados} encontrados.")

    # --- 5. TESOURO DIRETO ---
    print("--- 🔍 Tesouro Direto ---")
    df_td_assets = df_assets[(df_assets['type'] == 'TESOURO') & (~df_assets['ticker'].isin(tickers_manuais))]
    if not df_td_assets.empty:
        try:
            resp_td = requests.get(get_tesouro_url(), timeout=30)
            df_td = pd.read_csv(io.BytesIO(resp_td.content), sep=';', decimal=',', encoding='latin1')
            df_td['Data Base'] = pd.to_datetime(df_td['Data Base'], dayfirst=True)
            df_hoje = df_td[df_td['Data Base'] == df_td['Data Base'].max()]
            for _, row in df_td_assets.iterrows():
                t_td = str(row['ticker']).strip().upper()
                if t_td in precos_finais: continue
                
                ano = "".join(filter(str.isdigit, t_td))
                if len(ano) == 2: ano = "20" + ano
                tipo = "IPCA" if "IPCA" in t_td else "SELIC" if "SELIC" in t_td else "PREFIXADO"
                mask = (df_hoje['Tipo Titulo'].str.upper().str.contains(tipo)) & (pd.to_datetime(df_hoje['Data Vencimento'], dayfirst=True).dt.year == int(ano))
                if not df_hoje[mask].empty: precos_finais[t_td] = float(df_hoje[mask].iloc[0]['PU Base Manha'])
        except: pass

    # --- GRAVAÇÃO ---
    print("--- 💾 Salvando no Google Sheets ---")
    output = []
    # Garante que usamos a chave original da planilha asset
    for t in df_assets['ticker'].unique():
        ts = str(t).strip()
        if not ts: continue
        
        # Procura preço usando o ticker original ou o novo mapeado
        ts_mapped = RENAME_MAP.get(ts, ts)
        
        v = precos_finais.get(ts_mapped, precos_finais.get(ts, precos_preservados.get(ts, 1.0)))
        output.append([ts, float(v), agora])
    
    # Adiciona dolar
    if 'USDBRL=X' in precos_finais: 
        output.append(['USDBRL=X', float(precos_finais['USDBRL=X']), agora])

    ws_market.clear()
    # USER_ENTERED garante que o Google Sheets interprete o número com precisão total
    ws_market.update(values=[['ticker', 'close_price', 'last_update']] + output, range_name='A1', value_input_option='USER_ENTERED')
    print(f"✅ Atualização de preços concluída: {agora}")

if __name__ == "__main__":
    update_prices()
