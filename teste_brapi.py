import requests
import os
import json

def diagnostico_itub4():
    token = os.environ.get('BRAPI_TOKEN')
    ticker = "ITUB4"
    
    print(f"--- INICIANDO DIAGNÓSTICO PARA {ticker} ---")
    
    if not token:
        print("❌ ERRO: Token não encontrado.")
        return

    # Testando o endpoint exato da documentação para um único ativo
    url = f"https://brapi.dev/api/quote/{ticker}?token={token}&fundamental=true&dividends=true"
    
    try:
        print(f"📡 Enviando requisição para: {url.replace(token, 'REDACTED')}")
        response = requests.get(url, timeout=30)
        
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            # Imprime o JSON completo para analisarmos a estrutura
            print("📝 Resposta JSON Completa:")
            print(json.dumps(data, indent=2))
            
            # Verificação específica de campos
            results = data.get('results', [])
            if results:
                stock = results[0]
                div_data = stock.get('dividendsData')
                if div_data:
                    print(f"✅ 'dividendsData' encontrado para {ticker}!")
                    cash_divs = div_data.get('cashDividends', [])
                    print(f"💰 Total de dividendos em dinheiro listados: {len(cash_divs)}")
                else:
                    print(f"⚠️ 'dividendsData' NÃO veio no JSON para {ticker}.")
            else:
                print(f"⚠️ Nenhum resultado encontrado no campo 'results'.")
        else:
            print(f"❌ Erro na API: {response.text}")
            
    except Exception as e:
        print(f"❌ Falha na conexão: {e}")

if __name__ == "__main__":
    diagnostico_itub4()
