import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import datetime

def update_dividend_history():
    ID_PLANILHA = "1agsg85drPHHQQHPgUdBKiNQ9_riqV3ZvNxbaZ3upSx8"
    
    try:
        info = json.loads(os.environ['GOOGLE_SHEETS_CREDS'])
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(ID_PLANILHA)
    except Exception as e:
        print(f"❌ Erro Autenticação: {e}")
        return

    # 1. Carregar Transações para saber o que e quando comprou
    df_trans = pd.DataFrame(sh.worksheet("transactions").get_all_records())
    df_trans.columns = [c.lower().strip() for c in df_trans.columns]
    df_trans['date'] = pd.to_datetime(df_trans['date'], dayfirst=True)
    
    # 2. Carregar Assets para saber os tipos
    df_assets = pd.DataFrame(sh.worksheet("assets").get_all_records())
    df_assets.columns = [c.lower().strip() for c in df_assets.columns]

    historico_recebido = []

    print(f"🔎 Iniciando auditoria histórica para {len(df_trans['ticker'].unique())} ativos...")

    for ticker in df_trans['ticker'].unique():
        t = str(ticker).strip()
        if not t: continue
        
        # Descobrir a data da primeira compra deste ativo
        data_inicio = df_trans[df_trans['ticker'] == t]['date'].min()
        tipo = df_assets[df_assets['ticker'] == t]['type'].values[0] if t in df_assets['ticker'].values else ""

        try:
            # Ajuste de sufixo para Yahoo
            t_yf = f"{t}.SA" if tipo in ['ACAO_BR', 'FII', 'BDR', 'ETF_BR'] and not t.endswith('.SA') else t
            asset = yf.Ticker(t_yf)
            
            # Baixa todo o histórico de dividendos
            divs = asset.actions # Inclui Dividends e Splits
            if divs.empty: continue

            # Filtra apenas dividendos após a primeira compra
            divs = divs[divs.index >= data_inicio]
            
            for date_ex, row in divs.iterrows():
                if row['Dividends'] == 0: continue
                
                # Regra de Ouro: Quantas ações você tinha ANTES da data ex?
                qtd_na_data = df_trans[(df_trans['ticker'] == t) & (df_trans['date'] < date_ex)]['quantity'].sum()
                
                if qtd_na_data > 0:
                    valor_total = row['Dividends'] * qtd_na_data
                    historico_recebido.append([
                        t,
                        date_ex.strftime('%d/%m/%Y'),
                        row['Dividends'],
                        qtd_na_data,
                        valor_total,
                        datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
                    ])
                    print(f"✅ {t}: Recebido R$ {valor_total:.2f} em {date_ex.date()}")
        except:
            continue

    # 3. Salvar na aba 'dividend_history' (Crie esta aba se não existir)
    try:
        ws_hist = sh.worksheet("dividend_history")
    except:
        ws_hist = sh.add_worksheet(title="dividend_history", rows="1000", cols="6")

    ws_hist.clear()
    headers = [['Ticker', 'Data Ex', 'Valor Unitario', 'Qtd na Epoca', 'Total Recebido', 'Atualizado em']]
    if historico_recebido:
        ws_hist.update(values=headers + historico_recebido, range_name='A1')
    
    print("✅ Auditoria finalizada e salva na aba 'dividend_history'.")

if __name__ == "__main__":
    update_dividend_history()
