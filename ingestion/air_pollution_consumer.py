import json 
import pandas as pd
import pyarrow as pa
import logging
import pyarrow.parquet as pq

# Create a Spark session
def consume_air_pollution_data(message) :
    try :
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        logger.info(f"Received message: {message.value()}")
        air_pollution_data = json.loads(message.value())
        print(air_pollution_data)
        df_weather = pd.DataFrame(air_pollution_data)
        pq.write_parquet(df_weather,"air_pollution_data.parquet")

    except Exception as e:
        print(e)

