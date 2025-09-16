# Imagem base com Python
FROM python:3.11-alpine

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia requirements.txt e instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código para dentro do container
COPY . .

# Expõe a porta da API
EXPOSE 8000

# Comando padrão: roda a API FastAPI
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
