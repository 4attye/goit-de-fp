from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, from_json, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from configs import kafka_config
import os


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.7,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell'

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_driver = "com.mysql.cj.jdbc.Driver"

# 1.Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio (база даних і Credentials до неї вам будуть надані).
spark = SparkSession.builder \
    .config("spark.jars", "/home/albus/python_HW/goit-de-fp/Task_1/mysql-connector-j-8.0.32.jar") \
    .appName("AthleteStreamingPipeline") \
    .getOrCreate()

athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=jdbc_driver,
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password).load()

# 2.Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами.
filtered_bio_df = athlete_bio_df.select(
    "athlete_id", "sex", "height", "weight", "country_noc"
).filter(
    (col("height").isNotNull()) & (~isnan(col("height"))) &
    (col("weight").isNotNull()) & (~isnan(col("weight")))
)

# 3.Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results. Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results. Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.
mysql_results_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=jdbc_driver,
    dbtable="athlete_event_results",
    user=jdbc_user,
    password=jdbc_password
).load()

topic_name = f"{kafka_config['my_id']}_athlete_event_results"

mysql_results_df.selectExpr("CAST(athlete_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';") \
    .option("topic", topic_name) \
    .save()

kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';") \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

event_schema = StructType([
    StructField("athlete_id", IntegerType()),
    StructField("sport", StringType()),
    StructField("medal", StringType())
])

parsed_events_df = kafka_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")) \
    .select("data.*")

# 4.Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
enriched_df = parsed_events_df.join(filtered_bio_df, "athlete_id", "inner")

# 5.Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.
final_transformation_df = enriched_df.groupBy(
    "sport", "medal", "sex", "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
).withColumn("timestamp", current_timestamp())

# 6.Зробіть стрим даних (за допомогою функції forEachBatch) у:
def foreach_batch_function(batch_df, batch_id):
    output_df = batch_df.select(
        "sport", 
        "medal", 
        "sex", 
        "country_noc", 
        "avg_height", 
        "avg_weight", 
        "timestamp"
    )
    # а) запис у вихідний кафка-топік,
    output_df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';") \
        .option("topic", f"{kafka_config['my_id']}_athlete_enriched_results") \
        .save()

    # b) запис у базу даних.
    output_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", jdbc_driver) \
        .option("dbtable", f"{kafka_config['my_id']}_athlete_enriched_agg") \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

final_transformation_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start()\
    .awaitTermination()