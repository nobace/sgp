import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import numpy as np

def update_portfolio_funds():
    # ID da sua Planilha Google
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    # 1. Autenticação via Secrets do GitHub
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

    # 2. Obter Tickers da aba 'transactions'
    try:
        ws_trans = sh.worksheet("transactions")
        df_trans = pd.DataFrame(ws_trans.get_all_records())
        df_trans.columns = [c.lower().strip() for c in df_trans.columns]
        
        # Filtro Inteligente: Identificar apenas o que tem 14 dígitos (CNPJ)
        # Remove pontos, barras e traços para validar
        def extrair_cnpj(ticker):
            limpo = ''.join(filter(str.isdigit, str(ticker)))
            return limpo if len(limpo) == 14 else None

        df_trans['cnpj_limpo'] = df_trans['ticker'].apply(extrair_cnpj)
        cnpjs_validos = df_trans['cnpj_limpo'].dropna().unique().tolist()
        
    except Exception as e:
        print(f"❌ Erro ao ler aba transactions: {e}")
        return

    if not cnpjs_validos:
        print("ℹ️ Nenhum fundo com CNPJ (14 dígitos) encontrado para atualizar.")
        return

    # 3. Buscar Dados na CVM (Lógica de Mês Atual e Anterior)
    hoje = datetime.date.today()
    meses_para_tentar = [
        hoje.strftime('%Y%m'), 
        (hoje - datetime.timedelta(days=28)).strftime('%Y%m')
    ]
    
    df_cvm = None
    for mes in meses_para_tentar:
        url = f"https://dados.cvm.gov.br/dados/FIE/MED/DIARIO/DADOS/inf_diario_fie_{mes}.zip"
        try:
            print(f"🔍 Tentando base CVM: {mes}...")
            # Lendo diretamente do ZIP para economizar memória
            df_cvm = pd.read_csv(url, sep=';', compression='zip', encoding='latin1')
            print(f"✅ Dados de {mes} carregados!")
            break
        except:
            print(f"⚠️ Mês {mes} ainda não disponível.")
            continue

    if df_cvm is None:
        print("❌ Falha crítica: Base de dados da CVM indisponível no momento.")
        return

    # 4. Processar preços da CVM
    # Filtra apenas os CNPJs que temos na carteira para ganhar performance
    df_cvm = df_cvm[df_cvm['CNPJ_FUNDO'].str.replace(r'\D', '', regex=True).isin(cnpjs_validos)]
    # Pega a cota mais recente de cada fundo
    df_cvm = df_cvm.sort_values('DT_COMPTC').drop_duplicates('CNPJ_FUNDO', keep='last')
    
    # Criar dicionário indexado pelo CNPJ limpo para bater com o ticker
    price_dict = {}
    for _, row in df_cvm.iterrows():
        cnpj_key = ''.join(filter(str.isdigit, str(row['CNPJ_FUNDO'])))
        val = float(row['VL_QUOTA'])
        price_dict[cnpj_key] = val if not np.isnan(val) else 0.0

    # 5. Atualizar a aba 'market_data' preservando ações
    try:
        ws_market = sh.worksheet("market_data")
        # Lê o que já existe (preços de ações do outro script)
        market_rows = ws_market.get_all_records()
        final_prices = {str(r['ticker']).strip(): r['close_price'] for r in market_rows}
        
        # Atualiza ou insere os preços dos fundos
        for cnpj in cnpjs_validos:
            preco = price_dict.get(cnpj, 1.0) # Se não achar na CVM, usa 1.0 como proteção
            final_prices[cnpj] = preco
            print(f"💰 Fundo {cnpj}: R$ {preco:.6f}")

        # Preparar lista final para o Google Sheets (Garante que não há NaNs)
        updates = []
        for t, p in final_prices.items():
            # Força o ticker a ser string para manter zeros à esquerda
            val_p = float(p) if (not np.isnan(p) and not np.isinf(p)) else 0.0
            updates.append([str(t), val_p])

        # Gravação final
        ws_market.clear()
        ws_market.update(values=[['ticker', 'close_price']] + updates, range_name='A1')
        print(f"🚀 Sucesso! {len(cnpjs_validos)} fundos atualizados na planilha.")
        
    except Exception as e:
        print(f"❌ Erro ao gravar na planilha: {e}")

if __name__ == "__main__":
    update_portfolio_funds()
