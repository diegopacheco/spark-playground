from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("orcapp").getOrCreate()

# Define the schema for the `istari` table
schema = StructType([
    StructField("name", StringType(), True),
    StructField("color", StringType(), True)
])

# Create a DataFrame with sample data
data = [("Gandalf", "Grey"), ("Saruman", "White"), ("Radagast", "Brown")]
df = spark.createDataFrame(data, schema)

# Write DataFrame to ORC file
df.write.mode("overwrite").format("orc").save("/opt/spark/data/istari.orc")

# Read the ORC file into a DataFrame
df_orc = spark.read.format("orc").load("/opt/spark/data/istari.orc")

# Register the DataFrame as a temporary view
df_orc.createOrReplaceTempView("istari")

# Query the table using Spark SQL
result = spark.sql("SELECT * FROM istari")

# Print results
print("Query Result:")
result.show()

# Stop the Spark session
spark.stop()