
# Cria imagem a partir da versão Alpine com Python 3.9
FROM spark:python3-java17

# Pasta de trabalho
WORKDIR /code

# Utilizando o user root
USER root

# Instala o Python 3 e o pip
RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

# Copia o arquivo de requierements da máquina host para o container
COPY app/requirements.txt app/requirements.txt

# Instala as dependências
RUN pip install -r app/requirements.txt

# Copia os arquivos do host para o container
COPY . .

# Altera o user para o spark
USER spark

# Executa o app
CMD ["spark-submit", "app/app.py"]