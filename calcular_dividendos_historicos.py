import pandas as pd
import yfinance as yf
from datetime import timedelta
import time

# --- CONFIGURAÇÕES ---
ARQUIVO_TRANSACOES = 'transactions_final_eventos.csv'
ARQUIVO_SAIDA = 'dividend_history_final.csv'

# Mapeamento de Tickers (Caso o Yahoo use sufixo .SA)
def normalizar_ticker_yahoo(ticker):
    ticker = str(ticker).upper().strip()
    if ticker.endswith('11') or ticker.endswith('3') or ticker.endswith('4') or ticker.endswith('5') or ticker.endswith('6'):
        if not ticker.endswith('.SA'):
            return f"{ticker}.SA"
    return ticker

# --- FUNÇÃO: SALDO NA DATA (MÁQUINA DO TEMPO) ---
def calcular_quantidade_na_data(df, ticker, data_corte):
    """
    Calcula quantas ações o usuário tinha EXATAMENTE no final do dia 'data_corte'.
    """
    # Filtrar transações deste ticker até a data de corte (inclusive)
    mask = (df['ticker'] == ticker) & (df['date'] <= data_corte)
    transacoes = df[mask]
    
    if transacoes.empty:
        return 0
    
    qtd = 0.0
    for _, row in transacoes.iterrows():
        q = float(row['quantity'])
        t = row['type'].upper()
        
        # Entradas
        if t in ['COMPRA', 'BONIFICACAO', 'DESDOBRAMENTO']:
            qtd += q
        # Saídas
        elif t in ['VENDA', 'AGRUPAMENTO']:
            qtd -= q
            
    return max(0.0, qtd)

def main():
    print("--- 💰 INICIANDO CÁLCULO DE DIVIDENDOS HISTÓRICOS ---")
    
    # 1. Carregar Transações
    try:
        # Lê o CSV garantindo que os números venham certos (tratando vírgula PT-BR)
        df_trans = pd.read_csv(ARQUIVO_TRANSACOES)
        
        # Limpeza de colunas numéricas (removendo aspas e trocando vírgula por ponto)
        cols_num = ['quantity', 'price', 'total']
        for c in cols_num:
            df_trans[c] = df_trans[c].astype(str).str.replace('"', '').str.replace('.', '').str.replace(',', '.').astype(float)
            
        df_trans['date'] = pd.to_datetime(df_trans['date'])
        tickers_unicos = df_trans['ticker'].unique()
        print(f"📂 Transações carregadas. {len(tickers_unicos)} ativos identificados no histórico.")
        
    except Exception as e:
        print(f"❌ Erro ao ler {ARQUIVO_TRANSACOES}: {e}")
        return

    historico_recebimentos = []

    # 2. Loop por Ativo
    for ticker in tickers_unicos:
        # Ignorar ativos que não pagam dividendos ou desconhecidos (ex: ajustes manuais sem ticker claro)
        if ticker in ['UNKNOWN', 'nan', 'None'] or "FUNDO" in ticker: 
            continue
            
        print(f"\n🔍 Analisando proventos de: {ticker}...")
        
        ticker_y = normalizar_ticker_yahoo(ticker)
        
        try:
            # Baixar dados do Yahoo
            stock = yf.Ticker(ticker_y)
            divs = stock.dividends
            
            if divs.empty:
                print(f"   -> Sem histórico de dividendos no Yahoo.")
                continue
            
            # Converter timezone se necessário para remover info de fuso
            divs.index = divs.index.tz_localize(None)
            
            # 3. Cruzar com a Posição do Usuário
            count_recebimentos = 0
            total_ativo = 0.0
            
            for data_ex, valor_por_acao in divs.items():
                # A "Data Com" geralmente é o dia anterior à Data Ex. 
                # Se eu tinha a ação no dia anterior à Ex, eu recebo.
                # Mas para simplificar e ser seguro: Calculamos o saldo na própria Data Ex.
                # Se a compra foi NA data Ex, não recebe. Se foi antes, recebe.
                # Então calculamos o saldo no dia ANTERIOR à data Ex.
                
                data_com = data_ex - timedelta(days=1)
                
                qtd_possuida = calcular_quantidade_na_data(df_trans, ticker, data_com)
                
                if qtd_possuida > 0:
                    valor_total = qtd_possuida * valor_por_acao
                    
                    historico_recebimentos.append({
                        'Ticker': ticker,
                        'Data Ex': data_ex.strftime('%Y-%m-%d'),
                        'Data Pagamento': (data_ex + timedelta(days=15)).strftime('%Y-%m-%d'), # Yahoo não dá data pagto exata, estimamos +15d pra fluxo
                        'Valor Unitario': valor_por_acao,
                        'Qtd na Epoca': qtd_possuida,
                        'Total Recebido': valor_total,
                        'Ano': data_ex.year,
                        'Mes': data_ex.month
                    })
                    total_ativo += valor_total
                    count_recebimentos += 1
            
            if total_ativo > 0:
                print(f"   ✅ Recebeu {count_recebimentos} pagamentos. Total: R$ {total_ativo:,.2f}")
                
        except Exception as e:
            print(f"   ⚠️ Erro ao processar {ticker}: {e}")
            # Pausa para evitar bloqueio da API
            time.sleep(1)

    # 4. Salvar Resultado
    if historico_recebimentos:
        df_final = pd.DataFrame(historico_recebimentos)
        
        # Formatar números para PT-BR (Excel)
        df_final['Valor Unitario'] = df_final['Valor Unitario'].apply(lambda x: f"{x:.4f}".replace('.', ','))
        df_final['Qtd na Epoca'] = df_final['Qtd na Epoca'].apply(lambda x: f"{x:.4f}".replace('.', ','))
        df_final['Total Recebido Formatado'] = df_final['Total Recebido'].apply(lambda x: f"{x:.2f}".replace('.', ','))
        
        # Ordenar
        df_final = df_final.sort_values('Data Ex', ascending=False)
        
        # Salvar
        df_final.to_csv(ARQUIVO_SAIDA, index=False, sep=',', encoding='utf-8')
        print(f"\n🚀 Sucesso! Arquivo '{ARQUIVO_SAIDA}' gerado com {len(df_final)} registros.")
        print(f"💰 Total Histórico Estimado: R$ {df_final['Total Recebido'].sum():,.2f}")
        
    else:
        print("\n❌ Nenhum dividendo encontrado com base no histórico.")

if __name__ == "__main__":
    main()
