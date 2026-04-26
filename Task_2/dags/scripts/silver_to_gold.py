from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

def process_silver_to_gold():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    bio_df = spark.read.parquet("/opt/airflow/silver/athlete_bio")
    results_df = spark.read.parquet("/opt/airflow/silver/athlete_event_results")

    joined_df = bio_df.join(results_df, ["athlete_id", "country_noc"])

    gold_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("weight").alias("avg_weight"),
            avg("height").alias("avg_height")
        ) \
        .withColumn("timestamp", current_timestamp())
    gold_df.show(20)

    gold_df.write.mode("overwrite").parquet("/opt/airflow/gold/avg_stats")
    spark.stop()

if __name__ == "__main__":
    process_silver_to_gold()