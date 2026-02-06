import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import requests
import io

def update_all_market_data():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    # 1. Leitura das Abas
    df_assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    ws_market = sh.worksheet("market_data")
    # Lemos os valores que VOCÊ escreveu manualmente na market_data
    dados_market_atuais = ws_market.get_all_records()
    
    def clean_manual_val(val):
        if val is None or val == "": return 1.0
        s = str(val).replace('.', '').replace(',', '.')
        try: return float(s)
        except: return 1.0

    # Dicionário com os preços que já estão lá (para preservarmos)
    precos_preservados = {str(d['ticker']).strip(): clean_manual_val(d['close_price']) for d in dados_market_atuais}

    precos_finais = {}
    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    # --- PARTE A: YAHOO FINANCE (Ações, FIIs, BDRs, ETFs, Dólar) ---
    tipos_yahoo = ['ACAO_BR', 'FII', 'BDR', 'ETF_BR', 'ETF_US']
    tickers_yahoo = df_assets[df_assets['type'].isin(tipos_yahoo)]['ticker'].unique().tolist()
    tickers_yahoo.append('USDBRL=X')

    try:
        data_yf = yf.download(tickers_yahoo, period="1d", group_by='ticker', progress=False)
        for t in tickers_yahoo:
            try:
                val = data_yf[t]['Close'].iloc[-1] if len(tickers_yahoo) > 1 else data_yf['Close'].iloc[-1]
                if pd.notnull(val): precos_finais[str(t).strip()] = float(val)
            except: pass
    except: pass

    # --- PARTE B: CVM (Fundos) ---
    df_fundos = df_assets[df_assets['type'] == 'FUNDO']
    mapa_cnpjs = {str(r['isin_cnpj']).replace('.','').replace('-','').replace('/','').zfill(14): str(r['ticker']).strip() for _, r in df_fundos.iterrows() if r.get('isin_cnpj')}
    if mapa_cnpjs:
        for i in range(2):
            mes = (datetime.date.today() - datetime.timedelta(days=i*28)).strftime('%Y%m')
            url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"
            try:
                resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
                if resp.status_code == 200:
                    df_cvm = pd.read_csv(io.BytesIO(resp.content), sep=';', compression='zip', encoding='latin1')
                    col_cnpj = [c for c in df_cvm.columns if 'CNPJ' in c.upper()][0]
                    df_cvm['cnpj_key'] = df_cvm[col_cnpj].str.replace(r'\D', '', regex=True).str.zfill(14)
                    df_cvm = df_cvm.drop_duplicates('cnpj_key', keep='last')
                    cvm_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()
                    for cnpj, ticker in mapa_cnpjs.items():
                        if cnpj in cvm_dict: precos_finais[ticker] = float(cvm_dict[cnpj])
                    break
            except: continue

    # --- PARTE FINAL: MONTAGEM DO OUTPUT COM TRAVA PARA MANUAIS ---
    output = []
    # Lista de ativos que o robô NUNCA deve tentar atualizar sozinho
    tickers_bloqueados = ['FGTS_SALDO', 'PREV_ITAU_ULTRA']

    for t in df_assets['ticker'].unique():
        t_str = str(t).strip()
        if not t_str: continue
        
        # 1. Se o ticker estiver na lista de bloqueados, usamos o valor que já estava na market_data
        if t_str in tickers_bloqueados:
            valor_final = precos_preservados.get(t_str, 1.0)
            print(f"🔒 Mantendo valor manual para {t_str}: {valor_final}")
        
        # 2. Se for um ativo automático, usamos o que o robô buscou
        else:
            valor_final = precos_finais.get(t_str, 1.0)
            
        output.append([t_str, valor_final, agora])

    # Adiciona o Dólar
    if 'USDBRL=X' in precos_finais:
        output.append(['USDBRL=X', precos_finais['USDBRL=X'], agora])

    # Limpa e grava
    ws_market.clear()
    ws_market.update(values=[['ticker', 'close_price', 'last_update']] + output, range_name='A1')
    print(f"✅ Processo concluído com sucesso!")

if __name__ == "__main__":
    update_all_market_data()
