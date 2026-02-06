import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import numpy as np

def update_funds():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    # 1. Autenticação
    info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
    creds = Credentials.from_service_account_info(info, scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ])
    client = gspread.authorize(creds)
    sh = client.open_by_key(ID_PLANILHA)
    
    # 2. Ler Tickers e Tipos
    ws_trans = sh.worksheet("transactions")
    df_trans = pd.DataFrame(ws_trans.get_all_records())
    df_trans.columns = [c.lower().strip() for c in df_trans.columns]
    
    # Cruzamos com a aba assets para saber o TIPO do ativo
    ws_assets = sh.worksheet("assets")
    df_assets = pd.DataFrame(ws_assets.get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]
    
    # Lista de ativos únicos
    tickers_unicos = df_trans['ticker'].unique()
    
    # FILTRO: Só buscaremos na CVM o que tiver 14 dígitos (CNPJ) 
    # e que NÃO seja Ação, FII ou BDR (que o outro script já trata)
    cnpjs_validos = []
    for t in tickers_unicos:
        t_str = str(t).strip().replace('.', '').replace('/', '').replace('-', '')
        if len(t_str) == 14 and t_str.isdigit():
            cnpjs_validos.append(t_str)

    if not cnpjs_validos:
        print("Nenhum fundo com CNPJ detectado para consulta CVM.")
        return

    # 3. Buscar Dados na CVM
    today = datetime.date.today()
    # Tenta o mês atual, se falhar (início do mês), tenta o anterior
    for i in range(2):
        date_ref = today - datetime.timedelta(days=i*30)
        url = f"https://dados.cvm.gov.br/dados/FIE/MED/DIARIO/DADOS/inf_diario_fie_{date_ref.strftime('%Y%m')}.zip"
        try:
            print(f"Tentando CVM: {date_ref.strftime('%m/%Y')}...")
            df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1')
            break
        except:
            continue
    
    # Filtrar cotas mais recentes
    df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates('CNPJ_FUNDO', keep='last')
    price_dict = df_cvm.set_index('CNPJ_FUNDO')['VL_QUOTA'].to_dict()

    # 4. Atualizar apenas os Fundos na market_data sem apagar as Ações
    ws_market = sh.worksheet("market_data")
    market_rows = ws_market.get_all_records()
    
    # Criamos um dicionário do que já existe lá (Ações/FIIs preenchidos pelo update_prices.py)
    final_data = {str(r['ticker']): r['close_price'] for r in market_rows}
    
    for cnpj in cnpjs_validos:
        # Tenta achar o CNPJ na base da CVM
        preco_cota = price_dict.get(cnpj)
        if preco_cota:
            final_data[cnpj] = float(preco_cota)
            print(f"Atualizado: {cnpj} -> R$ {preco_cota}")
        else:
            # Se for Previdência ou ativo manual que não está na CVM, 
            # mantemos o preço 1.0 para o cálculo usar o Saldo como valor total
            if cnpj not in final_data:
                final_data[cnpj] = 1.0

    # 5. Salvar de volta
    updates = [[t, p] for t, p in final_data.items()]
    ws_market.update(values=[['ticker', 'close_price']] + updates, range_name='A1')
