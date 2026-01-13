# Desafio Aboda

Projeto de ETL, API e Bot do Telegram para ingestão, consulta e análise de dados financeiros a partir de arquivos CSV de ativos.

## Funcionalidades

- **ETL (Batch):** importa arquivos CSV da pasta `stocks/` em lotes para o banco SQLite.
- **API (FastAPI):**
  - Upload de CSVs individuais ou em lote.
  - Listagem de tickers importados (`/watchlist`).
  - Consulta de maior volume (`/highest_volume/{ticker}`).
  - Consulta de menor fechamento (`/lowest_closing_price/{ticker}`).
  - Geração de métricas consolidadas em Excel (`/consolidated_metrics`).
- **Bot do Telegram (`@desafio_aboda_bot`):**
  - Comandos interativos: `/watchlist`, `/highest_volume`, `/lowest_closing_price`, `/consolidated_metrics`.
  - Upload de CSV diretamente pelo chat (com legenda contendo o ticker).
  - Integração com **Groq LLM** para interpretar linguagem natural.

## Estrutura do Projeto

!!!!! Para rodar o projeto é necessário colocar a pasta Stocks na raiz do projeto !!!!!
>  **Atenção:** a pasta `stocks/` deve estar na raiz do projeto com os arquivos CSV para importação no código.

- app/ Código principal da aplicação
  - core/ Conexão e inicialização do banco
  - repository/ Queries SQL e persistência
  - services/ Regras de negócio e validação
  - batch/ Importador em lotes de CSVs
  - main.py/ API FastAPI
  - init.py

- bot/ Bot do Telegram
  - handlers/ Handlers de cada comando
  - states.py/ Estados de conversas
  - telegram_bot.py/ Ponto de entrada do Bot
  - init.py

-  Banco SQLite (persistência)
  - aboda.db

- stocks/ Pasta obrigatória com CSVs de ativos

- teste-bot-csv/ Pasta de CSVs de exemplo para enviar no Bot
  - BR.csv
  - FANG.csv

- Dockerfile
- docker-compose.yml
- requirements.txt
- README.md



## Rodando com Docker

####  Passo 1 — Build das imagens

builda a imagem base do projeto: 
docker build . -t aboda  

forma recomendada para múltiplos serviços:
docker compose build --no-cache

#### Passo 2 — Subir todos os serviços
docker compose up -d

Isso irá rodar:
- API em http://127.0.0.1:8000
- Batch (importador de CSVs em lote)
- Bot do Telegram (@desafio_aboda_bot)

#### Rodando serviços separadamente
Se quiser subir apenas alguns serviços:

- Subir apenas a API: 
  - docker compose up api
- Subir apenas o Bot:
  - docker compose up bot
- Rodar o Batch manualmente:
  - docker compose run batch

## Bot do Telegram
O bot está disponível em: @desafio_aboda_bot

Comandos disponíveis:
/start → inicia a conversa
/help → mostra ajuda
/watchlist → lista tickers disponíveis
/highest_volume → consulta maior volume
/lowest_closing_price → consulta menor fechamento
/consolidated_metrics → gera relatório Excel


## Upload de CSV:
Envie um arquivo .csv para o bot com o ticker na legenda.
Exemplo: enviar BR.csv com legenda BR.


## NLP e Planos Futuros
Embora não fosse exigido no desafio, foi implementada uma integração com Groq LLM para permitir consultas em linguagem natural, como:
- "Qual foi o maior volume da BR em 2019?"
- "Mostre métricas consolidadas da FANG entre janeiro e março de 2020."

Evolução do Bot :
1. Implementar TF-IDF para extrair termos relevantes localmente.
2. Usar Redis para armazenar histórico de interações e dar contexto às conversas.
3. Principais desafios: persistência e limites de tempo para armazenamento de contexto.


##  Requisitos
Docker e Docker Compose instalados.

Variáveis de ambiente configuradas em .env:

env
- chave da API do Telegram
TELEGRAM_TOKEN = você deve criar a sua

- chave da API do Groq
GROQ_API_KEY = você deve criar a sua
- endereço da API (pode mudar no docker)
API_URL=http://127.0.0.1:8000
