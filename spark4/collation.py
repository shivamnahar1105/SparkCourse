from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session with collation support
spark = SparkSession.builder \
    .appName("Spark 4.0 Collation Example") \
    .config("spark.sql.collation.enabled", "true") \
    .getOrCreate()

# Create a DataFrame with sample names
data = [
    (1, "alice"),
    (2, "Bob"),
    (3, "ALICE"),
    (4, "bob"),
    (5, "Charlie")
]

df = spark.createDataFrame(data, ["id", "name"])

# Define a table with a case-insensitive collation
spark.sql("DROP TABLE IF EXISTS names_ci")
spark.sql("""
    CREATE TABLE names_ci (
        id INT, 
        name STRING 
    ) USING PARQUET
""")

# Insert data into the table
df.write.mode("append").saveAsTable("names_ci")

# Select data with case-insensitive sorting
print("Case-Insensitive Sorting (utf8_general_ci):")
spark.sql("SELECT * FROM names_ci ORDER BY name").show()

# Create another table with case-sensitive collation
spark.sql("DROP TABLE IF EXISTS names_cs")
spark.sql("""
    CREATE TABLE names_cs (
        id INT, 
        name STRING COLLATE 'utf8_bin'
    ) USING PARQUET
""")

df.write.mode("append").saveAsTable("names_cs")

# Select data with case-sensitive sorting
print("Case-Sensitive Sorting (utf8_bin):")
spark.sql("SELECT * FROM names_cs ORDER BY name").show()

# Perform a case-insensitive comparison
print("Case-Insensitive Matching ('ALICE' vs 'alice'):")
spark.sql("SELECT * FROM names_ci WHERE name = 'alice'").show()

# Perform a case-sensitive comparison
print("Case-Sensitive Matching ('ALICE' vs 'alice'):")
spark.sql("SELECT * FROM names_cs WHERE name = 'alice'").show()

# Stop the Spark session
spark.stop()
