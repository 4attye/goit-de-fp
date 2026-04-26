from pyspark.sql import SparkSession
import requests
import os

def download_and_save_to_bronze(table_name, url):
    spark = SparkSession.builder.appName(f"LandingToBronze_{table_name}").getOrCreate()

    base_path = "/opt/airflow"
    landing_path = os.path.join(base_path, "landing", table_name)
    os.makedirs(landing_path, exist_ok=True)
    
    local_csv_path = os.path.join(landing_path, f"{table_name}.csv")

    print(f"Downloading {table_name}...")
    response = requests.get(url)
    response.raise_for_status()
    
    with open(local_csv_path, 'wb') as f:
        f.write(response.content)
    
    print(f"Reading {table_name} and saving to Bronze...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(local_csv_path)
    df.show(20)
    
    output_path = os.path.join(base_path, "bronze", table_name)
    df.write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    tables = {
        "athlete_bio": "https://ftp.goit.study/neoversity/athlete_bio.csv",
        "athlete_event_results": "https://ftp.goit.study/neoversity/athlete_event_results.csv"
    }
    for name, link in tables.items():
        download_and_save_to_bronze(name, link)