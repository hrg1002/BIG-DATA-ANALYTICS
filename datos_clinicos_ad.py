# Import the library to use Spark
from pyspark.sql import SparkSession
import pandas as pd 
spark = SparkSession.builder \
    .appName("Excel to Spark") \
    .master("local") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
    .getOrCreate() 
def process_medical_data() :
# To use it later with Spark, we have to create a session (ChatGPT)
# Specify the path
    archivo_excel = "./Atenciones.xlsx"

# Read the Excel file into a Spark DataFrame
# inferSchema is to detect the type of data

    spark_df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("dataAddress", "'Página1_1'!A17") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(archivo_excel)
        # Delete null rows
    spark_df = spark_df.dropna()

# Delete duplicated rows
    spark_df = spark_df.dropDuplicates()

# Show the first lines
    spark_df.show()

# To save the DataFrame in Parquet format for its post process
    spark_df.write.mode('overwrite').parquet("clean_atenciones.parquet")

# We close the Spark session when done
spark.stop()
