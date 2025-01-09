# Description: Transform data from clinical_data.csv to spark dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import current_date, month, dayofmonth, expr
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.mllib.feature import Normalizer
# Crear una sesión de Spark

def process_weather_data(df):
    """
    Add a new column 'season' to the DataFrame based on the current date.
    """
    # Add columns for the current month and day of the month
    df = df.withColumn("current_month", month(current_date())) \
           .withColumn("current_day", dayofmonth(current_date()))
    
    # Use a CASE WHEN expression to determine the season
    df = df.withColumn(
        "season",
        expr("""
            CASE
                WHEN (current_month = 3 AND current_day >= 20) OR (current_month > 3 AND current_month < 6) OR (current_month = 6 AND current_day < 21) THEN 'spring'
                WHEN (current_month = 6 AND current_day >= 21) OR (current_month > 6 AND current_month < 9) OR (current_month = 9 AND current_day < 23) THEN 'summer'
                WHEN (current_month = 9 AND current_day >= 23) OR (current_month > 9 AND current_month < 12) OR (current_month = 12 AND current_day < 21) THEN 'autumn'
                ELSE 'winter'
            END
        """)
    )
    # create a Normalizer object with p=2 (L2 normalization) by default
    #normalizer = Normalizer()

    # normalize the DataFrame
    return df  