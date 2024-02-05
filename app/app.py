from elt.extract import ExtractData
from elt.transform import TransformData
from elt.load import LoadData
from src.spark.spark_app import SparkApp
from pyspark.sql.dataframe import DataFrame
from src.utils.logs import Logs
from pathlib import Path
import os
import sys
from datetime import datetime

PROJECT_PATH = Path(__file__).absolute().parent


class AppProcessing(Logs):
    '''
    Classe para o pipeline de processamento dos dados.

    Args:
        Logs: Herança da classe de Logs
    '''

    def __init__(self, log_file: str) -> None:
        '''
        Inicia a classe.

        Args:
            log_file (str): Caminho do arquivo de logs
        '''
        try:
            self.spark_app = SparkApp()
            self.spark_session = self.spark_app.get_spark_session()
            self.log_file = log_file
            self.logs = Logs(log_file)
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting processing app data')
        except Exception as e:
            self.logs.write(
                f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Error to start processing app data: {str(e)}')
            sys.exit(1)

    def extract_csv_data(self, path: str) -> DataFrame:
        '''
        Extrai os dados de um arquivo CSV.

        Args:
            path (str): Caminho do arquivo CSV

        Returns:
            DataFrame: DataFrame com os dados do arquivo CSV
        '''
        try:
            extract = ExtractData(self.log_file)

            extract_data = extract.get_csv_data(path, self.spark_session)

            return extract_data
        except Exception as e:
            self.logs.write(
                f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Error to extract CSV data: {str(e)}')
            sys.exit(1)

    def transform_data_in_parquet(self, df: DataFrame, path: str) -> None:
        '''
        Transforma os dados em um arquivo Parquet.

        Args:
            df (DataFrame): DataFrame com os dados
        '''
        try:
            transform = TransformData(self.log_file)

            transform.transform_in_parquet_file(df, path)
        except Exception as e:
            self.logs.write(
                f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Error to transform data in parquet file: {str(e)}')
            sys.exit(1)

    def extract_parquet_data(self, path: str) -> DataFrame:
        '''
        Extrai os dados de um arquivo Parquet.

        Args:
            path (str): Caminho do arquivo Parquet

        Returns:
            DataFrame: DataFrame com os dados do arquivo Parquet
        '''
        try:
            extract = ExtractData(self.log_file)

            extract_data = extract.get_parquet_data(path, self.spark_session)

            return extract_data
        except Exception as e:
            self.logs.write(
                f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Error to extract Parquet data: {str(e)}')
            sys.exit(1)

    def clean_data(self, df: DataFrame) -> DataFrame:
        '''
        Limpa os dados.

        Args:
            df (DataFrame): DataFrame com os dados

        Returns:
            DataFrame: DataFrame com os dados limpos
        '''
        try:
            transform = TransformData(self.log_file)

            clean_data = transform.clean_data(df)

            return clean_data
        except Exception as e:
            self.logs.write(
                f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Error to clean data: {str(e)}')
            sys.exit(1)

    def close_spark_session(self) -> None:
        '''
        Fecha a sessão do Spark.
        '''
        self.spark_app.close_spark_session()

    def data_analisys(self, df: DataFrame) -> DataFrame:
        '''
        Realiza a análise dos dados.

        Args:
            df (DataFrame): DataFrame com os dados

        Returns:
            DataFrame: DataFrame com os dados analisados, agregados e classificados
        '''
        try:
            transform = TransformData(self.log_file)

            df = transform.agg_data(df)

            df = transform.top_actions(df)

            return df
        except Exception as e:
            self.logs.write(
                f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Error to analyze data: {str(e)}')
            sys.exit(1)

    def write_results(self, df: DataFrame) -> None:
        '''
        Escreve os resultados.

        Args:
            df (DataFrame): DataFrame com os dados
        '''
        try:
            load = LoadData(self.log_file)

            load.write_parquet(df.select('user_id', 'source', 'device_type', 'action', 'action_count').withColumnRenamed('action', 'top_action'),
                               os.path.join(PROJECT_PATH, 'files', 'output_results.parquet'))
        except Exception as e:
            self.logs.write(
                f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Error to write results: {str(e)}')
            sys.exit(1)

    def show_results(self, path: str) -> None:
        '''
        Mostra os resultados.

        Args:
            df (DataFrame): DataFrame com os dados
        '''
        try:
            extract = ExtractData(self.log_file)
            df = extract.get_parquet_data(path, self.spark_session)
            df.show()
        except Exception as e:
            self.logs.write(
                f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Error to show results: {str(e)}')


if __name__ == '__main__':
    # Caminho de arquivos
    log_path = os.path.join(PROJECT_PATH, 'logs', 'app.log')
    csv_path = os.path.join(PROJECT_PATH, 'files', 'user_logs.csv')
    parquet_path = os.path.join(PROJECT_PATH, 'files', 'user_logs.parquet')

    # Inicia aplicação
    app = AppProcessing(log_path)
    data = app.extract_csv_data(csv_path)
    app.transform_data_in_parquet(data, parquet_path)
    data_parquet = app.extract_parquet_data(parquet_path)
    data_parquet_cleaned = app.clean_data(data_parquet)
    data_rank = app.data_analisys(data_parquet_cleaned)
    app.write_results(data_rank)

    # Mostra os resultados na tela
    app.show_results(os.path.join(PROJECT_PATH, 'files', 'output_results.parquet'))

    # Fecha sessão do Spark
    app.close_spark_session()
