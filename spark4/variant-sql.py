from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark 4.0 VariantType SQL Example") \
    .config("spark.sql.variant.enabled", "true") \
    .getOrCreate()

# Print Spark version
print("Apache Spark Version:", spark.version)

# Create a SQL table with a VARIANT column
spark.sql("""
    CREATE TABLE sample_data (
          id INT,
          data VARIANT
    )
""")

spark.sql("""
    INSERT INTO sample_data (id, data)
    VALUES
    (1, PARSE_JSON('{"name": "Alice", "age": 30, "city": "New York"}')),
    (2, PARSE_JSON('{"product": "Laptop", "price": 1200.99, "specs": {"RAM": "16GB", "CPU": "Intel i7"}}')),
    (3, PARSE_JSON('{"event": "login", "user": "bob123", "timestamp": "2025-02-14T10:00:00Z"}'))       
""")

# Query the table using Spark SQL
result = spark.sql("""
    SELECT
        id,
        data,
        data.name AS name,
        data.price AS price,
        data.specs.RAM AS ram_spec,
        data.event AS event_type
    FROM sample_data
""")

# Show the result
result.show(truncate=False)

# Stop the Spark session
spark.stop()
