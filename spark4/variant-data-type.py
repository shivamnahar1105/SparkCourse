from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, VariantType
from pyspark.sql.functions import col, json_tuple, get_json_object

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark 4.0 Variant Type Example") \
    .config("spark.sql.variant.enabled", "true") \
    .getOrCreate()

# Define a schema with the VARIANT data type
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("data", VariantType(), True)  # New VARIANT type in Spark 4.0
])

# Create a DataFrame with heterogeneous data in the VARIANT column
data = [
    (1, {"name": "Alice", "age": 30, "city": "New York"}),
    (2, {"product": "Laptop", "price": 1200.99, "specs": {"RAM": "16GB", "CPU": "Intel i7"}}),
    (3, '{"event": "login", "user": "bob123", "timestamp": "2025-02-14T10:00:00Z"}')  # JSON as a string
]

df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show(truncate=False)

# Extracting JSON fields from the VARIANT column
df_parsed = df.withColumn("name", get_json_object(col("data").cast("string"), "$.name")) \
              .withColumn("price", get_json_object(col("data").cast("string"), "$.price"))

df_parsed.show(truncate=False)

# Filtering rows where 'data' contains a specific key
df_filtered = df.filter(get_json_object(col("data").cast("string"), "$.name").isNotNull())
df_filtered.show(truncate=False)

# Stopping the Spark session
spark.stop()
