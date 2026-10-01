from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import time
import random
from typing import Iterator

# Initialize Spark session with required configurations
spark = SparkSession.builder \
    .appName("Python Data Sources in PySpark 4.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# ------------------------------
# BATCH PROCESSING USING PYTHON DATA SOURCE
# ------------------------------

# Define a Python function as a data source
def batch_data_source():
    """Generates sample batch data"""
    for i in range(10):
        yield (i, f"Item-{i}", random.randint(1, 100))

# Create a DataFrame from the batch data source
batch_df = spark.createDataFrame(batch_data_source(), ["id", "name", "value"])

print("Batch Processing Output:")
batch_df.show()

# Write batch data to Parquet
#batch_df.write.mode("overwrite").parquet("output/batch_data.parquet")


# ------------------------------
# STREAMING PROCESSING USING PYTHON UDTF DATA SOURCE
# ------------------------------

class StreamingDataSource(DataSource):
    @classmethod
    def name(cls):
        return "Fake Streaming Data"
    
    def schema(self):
        return StructType([
            StructField("id", IntegerType()),
            StructField("data", StringType())
        ])
    
    def reader(self, schema:StructType):
        return StreamingDataSourceReader()

class StreamingDataSourceReader(DataSourceReader):
    def read(self, partition):
        i = 0
        while True:
            time.sleep(1)  # Simulating real-time data flow
            yield (i, f"Stream-Item-{i}", random.randint(1, 100))
            i += 1       

# Register UDTF in Spark SQL
spark.dataSource.register(StreamingDataSource)

# Create a streaming DataFrame
streaming_df = spark.readStream.format("Fake Streaming Data").load()

# Write stream data to console
query = streaming_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

#streaming_df.show()

# Allow streaming to run for 10 seconds before stopping
time.sleep(10)
query.stop()
