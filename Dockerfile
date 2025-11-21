# Imagem base com Python 3.11
FROM python:3.11-slim

# Evitar buffer em logs
ENV PYTHONUNBUFFERED=1

# Diretório de trabalho dentro do container
WORKDIR /app

# Copia o arquivo de dependências
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código da API
COPY . .

# Porta usada pelo Cloud Run
ENV PORT=8080

# Comando para iniciar a API com Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
