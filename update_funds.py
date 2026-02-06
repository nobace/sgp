import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import numpy as np

def update_portfolio_funds():
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

    # 2. Mapear CNPJs da aba 'assets'
    try:
        ws_assets = sh.worksheet("assets")
        df_assets = pd.DataFrame(ws_assets.get_all_records())
        df_assets.columns = [c.lower().strip() for c in df_assets.columns]
        
        # Cria dicionário {ticker: cnpj} filtrando apenas onde isin_cnpj tem 14 dígitos
        mapa_fundos = {}
        for _, row in df_assets.iterrows():
            ticker = str(row['ticker']).strip()
            # Limpa o CNPJ (deixa apenas números)
            cnpj_raw = str(row.get('isin_cnpj', '')).strip()
            cnpj_limpo = ''.join(filter(str.isdigit, cnpj_raw))
            
            if len(cnpj_limpo) == 14:
                mapa_fundos[ticker] = cnpj_limpo

        print(f"Fundos mapeados para consulta: {list(mapa_fundos.keys())}")
    except Exception as e:
        print(f"❌ Erro ao ler aba assets: {e}")
        return

    # 3. Buscar Dados na CVM (Mês atual ou anterior)
    hoje = datetime.date.today()
    meses = [hoje.strftime('%Y%m'), (hoje - datetime.timedelta(days=28)).strftime('%Y%m')]
    df_cvm = None
    
    for mes in meses:
        url = f"https://dados.cvm.gov.br/dados/FIE/MED/DIARIO/DADOS/inf_diario_fie_{mes}.zip"
        try:
            print(f"🔍 Tentando base CVM: {mes}...")
            df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1')
            print(f"✅ Dados de {mes} carregados!")
            break
        except:
            continue

    if df_cvm is None:
        print("❌ Base da CVM indisponível.")
        return

    # 4. Processar Preços
    # Pega apenas a cota mais recente disponível
    df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates('CNPJ_FUNDO', keep='last')
    # Limpa CNPJs da base CVM para comparação
    df_cvm['cnpj_key'] = df_cvm['CNPJ_FUNDO'].str.replace(r'\D', '', regex=True)
    price_dict = df_cvm.set_index('cnpj_key')['VL_QUOTA'].to_dict()

    # 5. Atualizar aba 'market_data'
    try:
        ws_market = sh.worksheet("market_data")
        market_rows = ws_market.get_all_records()
        # Preserva o que já está lá (ações do outro script)
        final_data = {str(r['ticker']).strip(): r['close_price'] for r in market_rows}
        
        for ticker, cnpj in mapa_fundos.items():
            preco = price_dict.get(cnpj)
            if preco:
                final_data[ticker] = float(preco)
                print(f"💰 {ticker} atualizado: R$ {preco:.6f}")
            else:
                # Se não achar na CVM, mas o ativo é um fundo, garante que não fique 0
                if ticker not in final_data:
                    final_data[ticker] = 1.0

        # Preparar dados para salvar
        updates = [[t, float(p) if pd.notnull(p) else 0.0] for t, p in final_data.items()]
        
        ws_market.clear()
        ws_market.update(values=[['ticker', 'close_price']] + updates, range_name='A1')
        print("🚀 Planilha atualizada com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao gravar: {e}")

if __name__ == "__main__":
    update_portfolio_funds()
