import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime

def audit_dividends():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    # 1. Carregar Dados
    df_trans = pd.DataFrame(sh.worksheet("transactions").get_all_records())
    df_trans.columns = [c.lower().strip() for c in df_trans.columns]
    df_trans['date'] = pd.to_datetime(df_trans['date'], dayfirst=True)
    
    df_assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]

    historico_calculado = []

    # 2. Iterar por cada ativo da carteira
    tickers = df_trans['ticker'].unique()
    print(f"🔎 Iniciando auditoria histórica para {len(tickers)} ativos...")

    for ticker in tickers:
        t = str(ticker).strip()
        if not t: continue
        
        # Filtra transações deste ativo e ordena por data
        min_date = df_trans[df_trans['ticker'] == t]['date'].min()
        tipo = df_assets[df_assets['ticker'] == t]['type'].values[0] if t in df_assets['ticker'].values else ""
        
        try:
            # Busca histórico no Yahoo (melhor fonte para dados retroativos)
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            
            # Pegamos o histórico de ações (inclui dividendos e splits)
            hist_divs = asset.actions
            if hist_divs.empty: continue
            
            # Filtra apenas dividendos a partir da primeira compra
            relevant_divs = hist_divs[hist_divs.index >= min_date]
            
            for date_ex, row in relevant_divs.iterrows():
                if row['Dividends'] == 0: continue
                
                # CRUCIAL: Quantas ações você tinha ANTES dessa Data Ex?
                # Soma todas as compras/vendas com data inferior à data ex do dividendo
                qtd_na_epoca = df_trans[(df_trans['ticker'] == t) & (df_trans['date'] < date_ex.replace(tzinfo=None))]['quantity'].sum()
                
                if qtd_na_epoca > 0:
                    total_recebido = row['Dividends'] * qtd_na_epoca
                    historico_calculado.append([
                        t,
                        date_ex.strftime('%d/%m/%Y'),
                        float(row['Dividends']),
                        float(qtd_na_epoca),
                        float(total_recebido),
                        datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
                    ])
        except Exception as e:
            print(f"⚠️ Erro ao auditar {t}: {e}")

    # 3. Salvar na aba específica
    try:
        ws_hist = sh.worksheet("dividend_history")
        ws_hist.clear()
        headers = [['Ticker', 'Data Ex', 'Valor Unitario', 'Qtd na Epoca', 'Total Recebido', 'Atualizado em']]
        if historico_calculado:
            ws_hist.update(values=headers + historico_calculado, range_name='A1')
        print(f"✅ Auditoria finalizada: {len(historico_calculado)} registros de proventos encontrados.")
    except Exception as e:
        print(f"❌ Erro ao salvar na planilha: {e}")

if __name__ == "__main__":
    audit_dividends()
