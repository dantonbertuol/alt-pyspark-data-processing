# Análise de Dados com Python e Spark
## Estrutura do Projeto
O projeto está dividido em pastas para melhor entendimento e organização. A estrutura do projeto é a seguinte:
- **pasta raiz**: contém o arquivo `readme.md` com a descrição do projeto, o arquivo .gitignore e os arquivos de configuração do ambiente docker.
- **pasta app**: contém o código fonte da aplicação.
  - **pasta elt**: contém o código referente aos processos de extração, transformação e carga dos dados.
  - **pasta files**: contém os arquivos de dados.
  - **pasta logs**: contém os arquivos de log da aplicação.
  - **pasta src**: contém o código fonte da aplicação.
    - **pasta spark**: contém o código fonte da aplicação em Spark.
    - **pasta utils**: contém o código fonte de funções utilitárias (logs).
  - **arquivo app.py**: arquivo principal da aplicação (pipeline).
  
## Etapas do Projeto
O projeto está dividido em 3 etapas:
1. **Extração dos Dados**: extração dos dados de arquivos CSV.
2. **Transformação dos Dados**: transformação dos dados extraídos.
3. **Carga dos Dados**: carga dos dados transformados em um arquivo parquet.
   
### 1. Extração dos Dados
Nesta etapa é realizada a consulta a base de dados do arquivo csv para posterior tratamento e análise.

### 2. Transformação dos Dados
Nesta etapa é realizada a transformação dos dados extraídos. As transformações realizadas foram:
1. **Limpeza dos dados**
   - Conversão de tipos de dados (coluna `timestamp` convertida de `string` para `timestamp`).
   - Conversão do arquivo CSV para um arquivo parquet.
   - Tratamento de datas ausentes 
   - Após a inserção de datas foram removidas as linhas com datas ausentes, pois para os demais itens que estavam ausentes não havia possibilidade de informar um valor "padrão" e poderia impactar na análise.
   - Remoção de outliers utilizando a técnica de quartis (coluna `timestamp`).
   - Corrigidos erros de digitação.
2. **Agregação dos Dados**
   - Criado novo dataframe com os dados agregados, realizando uma contagem de ocorrências por `user_id`, `action`, `source`, `device_type`.
3. **Ranking dos Dados**
   - Criado novo dataframe com os dados ranqueados, utilizando a técnica de "janelas", particionando os dados por `user_id`, `action`, `source`, `device_type` e ordenando por `action_count` em ordem decrescente e filtrando apenas os registros com `rank` igual igual ou inferior a 3 (*top 3 actions*).

### 3. Carga dos Dados
   - Carga dos dados finais em um novo arquivo parquet.

## Execução do Projeto
Para executar o projeto, é necessário ter o Docker instalado e em execução.

Tendo isso, basta clonar o repositório (`git clone https://github.com/dantonbertuol/alt-pyspark-data-processing`), acessar a pasta (`cd alt-pyspark-data-processing`) e executar o comando `docker-compose up` na pasta raiz do projeto. O comando irá criar um container com o ambiente necessário para execução do projeto e executar a aplicação.
Caso ocorra algum erro é possível tentar o comando `docker-compose up --build --force-recreate --no-deps` que força o rebuild da imagem.

O resultado da execução será um arquivo parquet com os dados classificados e agregados, na pasta `app/files` com o nome de `output_results.parquet`.

Os logs da aplicação estarão disponíveis na pasta `app/logs`.
