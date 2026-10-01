from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize Spark session
# Change ansi.enabled to "true" to illustrate NULL's throwing an exception in this mode
spark = SparkSession.builder \
    .appName("ANSI SQL Compliance Example") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

schema = StructType([
    StructField("name", StringType(), False),
    StructField("salary", IntegerType(), True)  # Allow NULLs
])

# Sample data creation (can also be loaded from a file or other sources)
data = [
    ("John", 1000),
    ("Alice", 1500),
    ("Bob", None),  # This will trigger a NULL handling demonstration
    ("Charlie", 2000)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the original DataFrame
df.show()

# ANSI SQL query example: Try performing operations with NULL handling
# This will raise an error in Spark 4 if we try to do something like this in strict ANSI mode:
# "salary + 100" (which would have previously resulted in NULL, now throws an exception)
df.createOrReplaceTempView("employee")

# Running an ANSI SQL query to calculate salary increases, handling NULLs explicitly
result = spark.sql("""
    SELECT name, 
           CASE 
               WHEN salary IS NULL THEN 'Unknown' 
               ELSE salary + 100 
           END AS salary_increase
    FROM employee
""")

# Show the result
result.show()

# SQL Query with grouping and ordering (standard ANSI SQL syntax)
result_aggregate = spark.sql("""
    SELECT name, COUNT(*) AS count 
    FROM employee
    GROUP BY name
    ORDER BY count DESC
""")

result_aggregate.show()

# Stop the Spark session
spark.stop()
