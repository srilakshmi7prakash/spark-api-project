from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# multiline true is to tell spark that the json s not just a single object
# but can be a nested one.By defaultspark expects proper JSON lines
def read_bronze(spark,file_path):
    df = spark.read.option("multiline","true").json(file_path)
    return df


def transform_to_silver(df_raw):
    df_stage_1 = df_raw.dropna(subset= ["id","email"])
    df_stage_2 = df_stage_1.fillna(
        {
    "username" : "unknown",
    "name" : "unkown",
    "phone" : "000-000-0000"
    }
    )
    final_df = df_stage_2.select(
        F.col("id"),
        F.col("name"),
        F.col("address.city").alias("city"),
        F.col("company.name").alias("org")
        ).filter(F.col("id")>=5)
    return final_df

def write_silver(silver_path,silver_df):
    # converting to pandas just becuase Spark requires winutils.exe to write a file since Spark is based on linux and and to write a file in windows
    # it needs a "translator"
    pandas_df = silver_df.toPandas()
    pandas_df.to_csv(silver_path, index=False)

    # silver_df.write.mode("overwrite").option("header","true").csv(silver_path)
    print(f"silver layer created at {silver_path}")