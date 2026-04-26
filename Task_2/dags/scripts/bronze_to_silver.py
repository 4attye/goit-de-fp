from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when

def clean_and_save_to_silver(table_name):
    spark = SparkSession.builder.appName(f"BronzeToSilver_{table_name}").getOrCreate()
    
    df = spark.read.parquet(f"bronze/{table_name}")
    
    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column, trim(col(column)))
    
    df = df.dropDuplicates()
    df.show(20)
    
    df.write.mode("overwrite").parquet(f"silver/{table_name}")
    spark.stop()

if __name__ == "__main__":
    for table in ["athlete_bio", "athlete_event_results"]:
        clean_and_save_to_silver(table)