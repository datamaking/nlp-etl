# nlp_etl/datasources/hive.py
from pyspark.sql import DataFrame
from app.datasources.datasource import DataSource

class HiveDataSource(DataSource):
    def __init__(self, config):
        self.config = config

    def read_data(self, spark) -> DataFrame:
        df = None
        if self.config.tables:
            if len(self.config.tables) == 1:
                #df = spark.table(self.config.tables[0])
                '''spark.sql("show databases").show()
                query = f"SELECT * FROM {self.config.tables[0]}"
                print(query)
                spark.sql("use docs_db")
                df = spark.sql(query)'''
                file_path = "file:///home/datamaking/work/data/ag_news_data_with_header.csv"
                df = spark.read.csv(file_path, header=True)
                df.show()
            else:
                df = spark.table(self.config.tables[0])
                for table, condition in zip(self.config.tables[1:], self.config.join_conditions):
                    df = df.join(spark.table(table), condition)
            return df.select("id", self.config.text_column).alias(self.config.text_column)
        # Similar logic for queries
        return df

# Factory
class DataSourceFactory:
    @staticmethod
    def create(config):
        if config.source_type == "hive":
            return HiveDataSource(config)
        '''elif config.source_type == "file":
            return FileDataSource(config)
        elif config.source_type == "rdbms":
            return RDBMSDataSource(config)'''
        raise ValueError("Unknown source type")