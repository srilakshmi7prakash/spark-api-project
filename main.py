import requests 
import json
import os
import sys
from pyspark.sql import SparkSession

os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable 

# creating folder
for folder in ['bronze','silver','gold']:
    os.makedirs(folder,exist_ok=True)

# API call and request handling
url = "https://jsonplaceholder.typicode.com/users"
response = requests.get(url)
data = response.json()


# //'w' refers to 'write' if it was 'a' it means append
bronze_path = "bronze/raw_users.json"
with open(bronze_path,"w") as f :
    json.dump(data,f)

print("successfully saved raw data to {bronze_path}")

spark = SparkSession.builder.appName("sparkApiProject").getOrCreate()

df = spark.read.option("multiline","true").json(bronze_path)

df.show()

print("data successfully loaded from bronze")