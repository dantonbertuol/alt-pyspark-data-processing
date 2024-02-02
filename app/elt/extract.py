import warnings
from datetime import datetime
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

PROJECT_PATH = Path(__file__).absolute().parent.parent

sys.path.insert(1, str(PROJECT_PATH))
from src.utils.logs import Logs  # noqa: E402

warnings.filterwarnings('ignore')


class ExtractData(Logs):
    '''
    Class para extração de dados.

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

    def get_csv_data(self, path: str, spark_session: SparkSession) -> DataFrame:
        '''
        Obtem os dados de arquivo CSV.

        Args:
            path (str): Caminho do arquivo CSV
            spark_session (SparkSession): Sessão do Spark

        Returns:
            DataFrame: DataFrame com os dados do arquivo CSV
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting to read CSV data from {path}')

        df = spark_session.read.csv(path, header=True)
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Finished reading CSV data from {path}')
        return df

    def get_parquet_data(self, path: str, spark_session: SparkSession) -> DataFrame:
        '''
        Obtem os dados de arquivo Parquet.

        Args:
            path (str): Caminho do arquivo Parquet
            spark_session (SparkSession): Sessão do Spark

        Returns:
            DataFrame: DataFrame com os dados do arquivo Parquet
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting to read Parquet data from {path}')

        df = spark_session.read.parquet(path)
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Finished reading Parquet data from {path}')
        return df
