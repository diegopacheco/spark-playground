from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("deltaapp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema for the `istari` table
schema = StructType([
    StructField("name", StringType(), True),
    StructField("color", StringType(), True)
])

# Create a DataFrame with sample data
data = [("Gandalf", "Grey"), ("Saruman", "White"), ("Radagast", "Brown")]
df = spark.createDataFrame(data, schema)

# Write DataFrame to Delta Lake format
df.write.mode("overwrite").format("delta").save("/opt/spark/data/istari.delta")

# Read the Delta Lake file into a DataFrame
df_delta = spark.read.format("delta").load("/opt/spark/data/istari.delta")

# Register the DataFrame as a temporary view
df_delta.createOrReplaceTempView("istari")

# Query the table using Spark SQL
result = spark.sql("SELECT * FROM istari")

# Print results
print("Query Result:")
result.show()

# Stop the Spark session
spark.stop()