import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime
import time

def audit_dividends_since_2008():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    # 1. Carregar Transações e Assets
    df_trans = pd.DataFrame(sh.worksheet("transactions").get_all_records())
    df_trans.columns = [c.lower().strip() for c in df_trans.columns]
    df_trans['date'] = pd.to_datetime(df_trans['date'], dayfirst=True, errors='coerce')
    
    df_assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]

    historico_calculado = []
    tickers = df_trans['ticker'].unique()
    
    print(f"⏳ Iniciando auditoria profunda (Desde 2008) para {len(tickers)} ativos...")

    for ticker in tickers:
        t = str(ticker).strip()
        if not t or t == "USDBRL=X": continue
        
        # Data de início baseada na sua transação mais antiga para este ticker
        # Se você colocar 2008 na planilha, ele buscará desde lá.
        min_date = df_trans[df_trans['ticker'] == t]['date'].min()
        if pd.isnull(min_date): min_date = datetime.datetime(2008, 1, 1)

        tipo = df_assets[df_assets['ticker'] == t]['type'].values[0] if t in df_assets['ticker'].values else ""
        
        try:
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            
            # Baixa o histórico completo (max) para pegar desde 2008
            hist_divs = asset.actions
            if hist_divs.empty: continue
            
            # Filtra a partir da data da sua primeira compra
            relevant_divs = hist_divs[hist_divs.index >= min_date.replace(tzinfo=hist_divs.index.tz)]
            
            for date_ex, row in relevant_divs.iterrows():
                if row['Dividends'] == 0: continue
                
                # Pega a quantidade que você tinha na data (baseado na aba transactions)
                qtd_na_epoca = df_trans[(df_trans['ticker'] == t) & (df_trans['date'] <= date_ex.replace(tzinfo=None))]['quantity'].sum()
                
                if qtd_na_epoca > 0:
                    total_recebido = row['Dividends'] * qtd_na_epoca
                    historico_calculado.append([
                        t,
                        date_ex.strftime('%d/%m/%Y'),
                        round(float(row['Dividends']), 4),
                        int(qtd_na_epoca),
                        round(float(total_recebido), 2),
                        datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
                    ])
            print(f"✅ {t}: Auditado desde {min_date.year}")
            time.sleep(1) # Evitar bloqueio do Yahoo
        except Exception as e:
            print(f"⚠️ Erro em {t}: {e}")

    # 3. Salvar na aba 'dividend_history'
    try:
        ws_hist = sh.worksheet("dividend_history")
        ws_hist.clear()
        headers = [['Ticker', 'Data Ex', 'Valor Unitario', 'Qtd na Epoca', 'Total Recebido', 'Atualizado em']]
        if historico_calculado:
            # Ordenar por data para facilitar a leitura
            historico_calculado.sort(key=lambda x: datetime.datetime.strptime(x[1], '%d/%m/%Y'))
            ws_hist.update(values=headers + historico_calculado, range_name='A1')
        print(f"🚀 Sucesso! {len(historico_calculado)} pagamentos recuperados desde 2008.")
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")

if __name__ == "__main__":
    audit_dividends_since_2008()
