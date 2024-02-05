from datetime import datetime
import sys
from pathlib import Path
from pyspark.sql import functions as f
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame

PROJECT_PATH = Path(__file__).absolute().parent.parent

sys.path.insert(1, str(PROJECT_PATH))
from src.utils.logs import Logs  # noqa: E402


class TransformData(Logs):
    '''
    Class para transformação de dados.

    Args:
        Logs: Herança da classe de Logs
    '''

    def __init__(self, log_file: str) -> None:
        '''
        Inicia a classe.

        Args:
            log_file (str): Caminho do arquivo de logs
        '''
        self.logs = Logs(log_file)

    def transform_in_parquet_file(self, df: DataFrame, path: str) -> None:
        '''
        Transforma os dados em um arquivo Parquet.

        Args:
            df (DataFrame): DataFrame com os dados
            path (str): Caminho do arquivo Parquet
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting to transform data in parquet file')

        # converte a coluna timestamp para o tipo timestamp
        df = df.withColumn('timestamp', df['timestamp'].cast('timestamp'))
        # escreve no fomat parquet
        df.write.parquet(path, mode='overwrite')

        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Finished transforming data in parquet file')

    def clean_data(self, df: DataFrame) -> DataFrame:
        '''
        Limpa os dados.

        Args:
            df (DataFrame): DataFrame com os dados

        Returns:
            DataFrame: DataFrame com os dados limpos
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting to clean data')

        # Transforma a coluna timestamp para o tipo unix timestamp para trabalhar com os valores
        df = df.withColumn('timestamp', f.unix_timestamp('timestamp'))

        # Tratados valores nulos na coluna timestamp com a mediana
        mediana = df.approxQuantile("timestamp", [0.5], 0.01)[0]
        df = df.fillna(mediana, subset=['timestamp'])

        # Remove linhas com valores nulos
        df = df.dropna()

        # substituir désktop por desktop (erro de digitação)
        df = df.withColumn('device_type', f.when(f.col('device_type') ==
                                                 'désktop', 'desktop').otherwise(f.col('device_type')))

        # Remove os outliers usando o método de Tukey
        quartis = df.approxQuantile("timestamp", [0.25, 0.75], 0.01)
        iqr = quartis[1] - quartis[0]
        limite_inferior = quartis[0] - 1.5 * iqr
        limite_superior = quartis[1] + 1.5 * iqr

        df = df.filter((f.col("timestamp") >= limite_inferior) &
                       (f.col("timestamp") <= limite_superior))

        # Transforma a coluna de unix timestamp para o tipo timestamp
        df = df.withColumn('timestamp', f.from_unixtime('timestamp'))

        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Finished cleaning data')

        return df

    def agg_data(self, df: DataFrame) -> DataFrame:
        '''
        Realiza a agregação dos dados.

        Args:
            df (DataFrame): DataFrame com os dados

        Returns:
            DataFrame: DataFrame com os dados agregados
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting to aggregate data')

        # realiza a agregação dos dados por user_id, action, source e device_type, contando as ações
        df = df.groupBy('user_id', 'action', 'source', 'device_type').agg(f.count('user_id').alias('action_count'))

        # ordena os dados por user_id e action_count
        df = df.orderBy(col('user_id').asc(), col('action_count').desc())

        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Finished aggregating data')
        return df

    def top_actions(self, df: DataFrame) -> DataFrame:
        '''
        Obtem as ações mais realizadas por user_id, source, device_type.

        Args:
            df (DataFrame): DataFrame com os dados

        Returns:
            DataFrame: DataFrame com as ações mais realizadas
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting to get top actions')

        # crio uma janela particionada por user_id, source e device_type, ordenada pela quantidade de ações
        window = Window.partitionBy("user_id", "source", "device_type").orderBy(col("action_count").desc())
        # crio uma coluna rank com a posição da linha na janela
        df = df.withColumn("rank", row_number().over(window))
        # filtro as linhas com rank menor ou igual a 3 (top 3 ações mais realizadas por user_id, source e device_type)
        df = df.filter(col('rank') <= 3)

        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Finished getting top actions')

        return df
