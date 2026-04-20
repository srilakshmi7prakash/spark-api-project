import requests 
import json
import os
import sys
from pyspark.sql import SparkSession
from src.ingestion.api_client import fetch_api
from config import BRONZE_PATH , SILVER_PATH , GOLD_PATH , API_URL 
from src.processing.bronze_ingestion import write_bronze
from src.processing.silver_transformation import read_bronze , transform_to_silver,write_silver

os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable 

# creating folder
for folder in ['bronze','silver','gold']:
    os.makedirs(folder,exist_ok=True)

if __name__ == "__main__":

    spark = SparkSession.builder.appName("sparkApiProject").getOrCreate()
    api_data = fetch_api(API_URL)  
    write_bronze(BRONZE_PATH,api_data)

    df_raw = read_bronze(spark,BRONZE_PATH)
    silver_df = transform_to_silver(df_raw)
    
    write_silver(SILVER_PATH,silver_df)



