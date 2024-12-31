from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("parquetapp").getOrCreate()

# Define the schema for the `istari` table
schema = StructType([
    StructField("name", StringType(), True),
    StructField("color", StringType(), True)
])

# Create a DataFrame with sample data
data = [("Gandalf", "Grey"), ("Saruman", "White"), ("Radagast", "Brown")]
df = spark.createDataFrame(data, schema)

# Write DataFrame to Parquet file
df.write.mode("overwrite").format("parquet").save("/opt/spark/data/istari.parquet")

# Read the Parquet file into a DataFrame
df_parquet = spark.read.format("parquet").load("/opt/spark/data/istari.parquet")

# Register the DataFrame as a temporary view
df_parquet.createOrReplaceTempView("istari")

# Query the table using Spark SQL
result = spark.sql("SELECT * FROM istari")

# Print results
print("Query Result:")
result.show()

# Stop the Spark session
spark.stop()