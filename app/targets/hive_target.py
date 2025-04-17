from pyspark.sql import DataFrame
from app.targets.datatarget import DataTarget

# nlp_etl/targets/hive_target.py
class HiveTarget(DataTarget):
    def __init__(self, config):
        self.config = config

    def write_data(self, df: DataFrame):
        mode = "overwrite" if self.config.mode == "full" else "append"
        # For SCD Type 2 or CDC, add logic to handle keys and timestamps
        df.write.mode(mode).saveAsTable(self.config.table_name)

# Factory
class TargetFactory:
    @staticmethod
    def create(config):
        if config.target_type == "hive":
            return HiveTarget(config)
        '''elif config.target_type == "vector_db":
            return VectorDBTarget(config)'''
        raise ValueError("Unknown target type")