from datetime import datetime
import sys
from pathlib import Path
from pyspark.sql.dataframe import DataFrame

PROJECT_PATH = Path(__file__).absolute().parent.parent

sys.path.insert(1, str(PROJECT_PATH))
from src.utils.logs import Logs  # noqa: E402


class LoadData(Logs):
    '''
    Class para carregar dados.

    Args:
        Logs: Heraça da classe de Logs
    '''

    def __init__(self, log_file: str):
        '''
        Inicia a classe.

        Args:
            log_file (str): Caminho do arquivo de logs
        '''
        self.logs = Logs(log_file)

    def write_parquet(self, df: DataFrame, path: str):
        '''
        Escreve os dados em um arquivo Parquet.

        Args:
            df (DataFrame): DataFrame com os dados
            path (str): Caminho do arquivo Parquet
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting to load data in parquet file')
        df.write.parquet(path, mode='overwrite')
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Finished loading data in parquet file')
