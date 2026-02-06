import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import numpy as np
import requests

def get_tesouro_url():
    """Consulta a API do portal Tesouro Transparente para encontrar o link atual do CSV"""
    api_url = "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=taxas-do-tesouro-direto"
    try:
        response = requests.get(api_url, timeout=30)
        data = response.json()
        resources = data['result']['resources']
        for res in resources:
            # Procuramos o arquivo CSV que contém Preço e Taxa no nome
            if "Preco" in res['name'] and "Taxa" in res['name'] and res['format'].lower() == "csv":
                return res['url']
    except Exception as e:
        print(f"⚠️ Erro ao consultar API do Tesouro: {e}")
    # Fallback caso a API falhe
    return "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv"

def update_all_market_data():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    # 1. Autenticação
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=[
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro na autenticação: {e}")
        return

    # 2. Ler a aba 'assets' para mapear o que buscar
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]

    precos_finais = {}
    # Timestamp da atualização (Horário de Brasília se rodar local ou UTC no GitHub)
    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    # --- PARTE A: YAHOO FINANCE (Ações, FIIs, BDRs, ETFs) ---
    tipos_yahoo = ['ACAO_BR', 'FII', 'BDR', 'ETF_BR', 'ETF_US']
    tickers_yahoo = df_assets[df_assets['type'].isin(tipos_yahoo)]['ticker'].tolist()
    
    if tickers_yahoo:
        print(f"🔍 Buscando {len(tickers_yahoo)} ativos no Yahoo Finance...")
        try:
            data_yf = yf.download(tickers_yahoo, period="1d", group_by='ticker', progress=False)
            for t in tickers_yahoo:
                try:
                    # Lógica para tratar retorno de um ou múltiplos tickers
                    if len(tickers_yahoo) > 1:
                        val = data_yf[t]['Close'].iloc[-1]
                    else:
                        val = data_yf['Close'].iloc[-1]
                    
                    if pd.notnull(val):
                        precos_finais[t] = float(val)
                except:
                    precos_finais[t] = 0.0
        except Exception as e:
            print(f"⚠️ Erro Yahoo Finance: {e}")

    # --- PARTE B: CVM (Fundos de Investimento) ---
    df_fundos = df_assets[df_assets['type'] == 'FUNDO']
    mapa_cnpjs = {}
    for _, row in df_fundos.iterrows():
        cnpj_raw = str(row.get('isin_cnpj', '')).strip()
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj_raw)).zfill(14) # Garante 14 dígitos
        if len(cnpj_limpo) == 14:
            mapa_cnpjs[cnpj_limpo] = str(row['ticker'])

    if mapa_cnpjs:
        print(f"🔍 Buscando {len(mapa_cnpjs)} fundos na CVM...")
        hoje = datetime.date.today()
        for i in range(4): # Tenta o mês atual e os 3 anteriores
            mes = (hoje - datetime.timedelta(days=i*28)).strftime('%Y%m')
            url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{mes}.zip"
            try:
                df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1', 
                                     storage_options={'User-Agent': 'Mozilla/5.0'})
                
                # Identifica coluna de CNPJ e limpa
                col_cnpj = [c for c in df_cvm.columns if 'CNPJ_FUNDO' in c][0]
                df_cvm['cnpj_key'] = df_cvm[col_cnpj].str.replace(r'\D', '', regex=True).str.zfill(14)
                
                # Pega a última cota disponível de cada fundo
                df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates('cnpj_key', keep='last')
                cvm_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()
                
                encontrados = 0
                for cnpj, ticker in mapa_cnpjs.items():
                    if cnpj in cvm_dict:
                        precos_finais[ticker] = float(cvm_dict[cnpj])
                        encontrados += 1
                
                if encontrados > 0:
                    print(f"✅ {encontrados} fundos atualizados com dados de {mes}.")
                    break
            except:
                continue

    # --- PARTE C: TESOURO DIRETO (API Dinâmica) ---
    df_td_assets = df_assets[df_assets['type'] == 'TESOURO']
    if not df_td_assets.empty:
        url_td = get_tesouro_url()
        print(f"🔍 Buscando Tesouro Direto...")
        try:
            df_td = pd.read_csv(url_td, sep=';', decimal=',', encoding='latin1')
            df_td['Data Vencimento'] = pd.to_datetime(df_td['Data Vencimento'], dayfirst=True)
            df_td['Data Base'] = pd.to_datetime(df_td['Data Base'], dayfirst=True)
            
            # Filtra pela data mais recente do arquivo
            df_hoje = df_td[df_td['Data Base'] == df_td['Data Base'].max()]

            for _, row in df_td_assets.iterrows():
                ticker = str(row['ticker']).upper()
                # Extrai ano do ticker (ex: TD_IPCA_29 -> 2029)
                digitos = ''.join(filter(str.isdigit, ticker))
                ano_venc = 2000 + int(digitos) if digitos else 2029
                
                tipo = "IPCA+" if "IPCA" in ticker else "Selic" if "SELIC" in ticker else "Prefixado"
                
                mask = df_hoje['Tipo Titulo'].str.contains(tipo, case=False, na=False)
                # Diferencia títulos com Juros Semestrais
                if "JUROS" in ticker:
                    mask = mask & df_hoje['Tipo Titulo'].str.contains("Juros", case=False)
                else:
                    mask = mask & (~df_hoje['Tipo Titulo'].str.contains("Juros", case=False))
                
                mask = mask & (df_hoje['Data Vencimento'].dt.year == ano_venc)
                
                match = df_hoje[mask]
                if not match.empty:
                    precos_finais[row['ticker']] = float(match.iloc[0]['PU Base Manha'])
                    print(f"✅ {row['ticker']} atualizado.")
        except Exception as e:
            print(f"⚠️ Erro Tesouro: {e}")

    # --- 3. Gravação Final na aba 'market_data' ---
    try:
        ws_market = sh.worksheet("market_data")
        ws_market.clear()
        
        # Prepara as linhas incluindo a coluna de timestamp
        final_rows = []
        # Garante que todos os ativos da 'assets' existam na 'market_data'
        for t in df_assets['ticker'].unique():
            t_str = str(t).strip()
            preco = precos_finais.get(t_str, 1.0) # 1.0 é o fallback para ativos manuais
            final_rows.append([t_str, float(preco), agora])
        
        ws_market.update(values=[['ticker', 'close_price', 'last_update']] + final_rows, range_name='A1')
        print(f"🚀 Sucesso! Planilha atualizada em {agora}.")
        
    except Exception as e:
        print(f"❌ Erro ao gravar: {e}")

if __name__ == "__main__":
    update_all_market_data()
