from pyspark.sql import SparkSession


class SparkApp():
    '''
    Classe para iniciar a sessão do Spark.
    '''
    def __init__(self) -> None:
        '''
        Inicia a sessão do Spark.
        '''
        self.spark = SparkSession.builder.appName('alt-bank Spark App').getOrCreate()

    def get_spark_session(self) -> SparkSession:
        '''
        Get spark session.

        Returns:
            _type_: _description_
        '''
        return self.spark

    def close_spark_session(self) -> None:
        '''
        Fecha a sessão do Spark.
        '''
        self.spark.stop()
