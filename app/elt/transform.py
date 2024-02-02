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
