# import das minhas bibliotecas - verificar se tem todas as bibliotecas necessárias instaladas no meu ambiente virtual (pip install yfinance por exemplo)
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# import das minhas variaveis de ambiente

load_dotenv()

#API Yfanancer buscar os dados dos meus ativos
ticker_api = ['BTC-USD', 'ETH-USD', 'ADA-USD', 'SOL-USD']

#conectar no database do render com as variaveis de ambiente no .env
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

# criar a string de conexão com o banco de dados usando as variáveis de ambiente

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# criar a engine de conexão com o banco de dados usando SQLAlchemy

engine = create_engine(DATABASE_URL)

# criar a função para buscar os dados de um ticker específico usando a API do yfinance

def buscar_dados_ticker(simbolo, periodo='3y', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

# criar a função para buscar os dados de todos os tickers e concatenar em um único DataFrame
def buscar_todos_dados_tickers(ticker_api):
    todos_dados = []
    for simbolo in ticker_api:
        dados = buscar_dados_ticker(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

# testar a função buscar_todos_dados_tickers, para teste local
if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_tickers(ticker_api)
    print(dados_concatenados)

#criar a função para salvar os dados no banco de dados do render usando a engine criada com SQLAlchemy

def salvar_no_postgres(df, schema='public'):
    df.to_sql('tbcryptos', engine, if_exists='replace', index=True, index_label='Date', schema=schema)

if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_tickers(ticker_api)
    salvar_no_postgres(dados_concatenados, schema='public')

