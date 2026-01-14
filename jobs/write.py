from pyspark.sql import DataFrame


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite", partition_by: str = None):
    if partition_by is None:
        df.write.parquet(path=path, mode=mode)
    else:
        df.write.partitionBy(partition_by).parquet(path=path, mode=mode)

def write_csv(df : DataFrame, path : str, mode : str = "overwrite", partition_by: str = None):
    if partition_by is None:
        df.write.csv(path=path, mode=mode, header=True)
    else:
        df.write.partitionBy(partition_by).csv(path=path, mode=mode, header=True)