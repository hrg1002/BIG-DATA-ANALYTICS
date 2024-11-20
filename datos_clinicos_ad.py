from pyspark.sql import SparkSession


# To use it later with Spark, we have to create a session
spark = SparkSession.builder \
    .appName("Excel to Spark") \
    .master("local") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
    .getOrCreate()

archivo_excel = "./Atenciones.xlsx"

spark_df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(archivo_excel)

# Delete null lines
spark_df = spark_df.dropna()

# Delete duplicated lines
spark_df = spark_df.dropDuplicates()

# Show the first lines
spark_df.show()

# To save the DataFrame in Parquet format for its post process

spark_df.write.parquet("clean_atenciones.parquet")

spark.stop()
